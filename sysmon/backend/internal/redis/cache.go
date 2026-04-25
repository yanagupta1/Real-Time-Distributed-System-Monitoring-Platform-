package rediscache

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"

	"github.com/yourusername/sysmon/internal/metrics"
)

const (
	keyLatest    = "sysmon:latest:%s"     // per agentID
	keyAgg       = "sysmon:agg:%s"        // per agentID
	keyAllAgents = "sysmon:agents"        // sorted set of agent IDs by last-seen score
	keyAlerts    = "sysmon:alerts:active" // list of active alert JSON
	keyHistory   = "sysmon:history:%s"   // stream key per agentID
	ttlLatest    = 30 * time.Second
	ttlAgg       = 5 * time.Minute
	maxHistory   = 1000 // entries per agent
)

type Cache struct {
	client *redis.Client
	logger *zap.Logger

	// In-memory accumulator for incremental aggregation (avoids full recalc)
	mu   sync.RWMutex
	accs map[string]*accumulator
}

type accumulator struct {
	agentID  string
	hostname string
	count    int
	sumCPU   float64
	maxCPU   float64
	sumMem   float64
	maxMem   float64
	sumNetSent float64
	sumNetRecv float64
	windowStart time.Time
	lastSnap    *metrics.SystemSnapshot
}

func (a *accumulator) update(snap *metrics.SystemSnapshot) {
	a.count++
	a.sumCPU += snap.CPUTotal
	if snap.CPUTotal > a.maxCPU { a.maxCPU = snap.CPUTotal }
	a.sumMem += snap.MemPercent
	if snap.MemPercent > a.maxMem { a.maxMem = snap.MemPercent }
	a.sumNetSent += float64(snap.NetBytesSent)
	a.sumNetRecv += float64(snap.NetBytesRecv)
	a.lastSnap = snap
}

func (a *accumulator) toAggregated(windowEnd time.Time) metrics.AggregatedMetrics {
	agg := metrics.AggregatedMetrics{
		AgentID:       a.agentID,
		Hostname:      a.hostname,
		WindowStart:   a.windowStart,
		WindowEnd:     windowEnd,
		SampleCount:   a.count,
		UpdatedAt:     time.Now(),
	}
	if a.count > 0 {
		agg.AvgCPU        = math.Round(a.sumCPU/float64(a.count)*100)/100
		agg.MaxCPU        = math.Round(a.maxCPU*100)/100
		agg.AvgMemPercent = math.Round(a.sumMem/float64(a.count)*100)/100
		agg.MaxMemPercent = math.Round(a.maxMem*100)/100
		agg.AvgNetSentBps = a.sumNetSent / float64(a.count)
		agg.AvgNetRecvBps = a.sumNetRecv / float64(a.count)
	}
	if a.lastSnap != nil {
		procs := make([]metrics.ProcessMetrics, len(a.lastSnap.Processes))
		copy(procs, a.lastSnap.Processes)
		sort.Slice(procs, func(i, j int) bool {
			return procs[i].CPUPercent > procs[j].CPUPercent
		})
		if len(procs) > 10 { procs = procs[:10] }
		agg.TopProcesses = procs
	}
	return agg
}

func New(addr, password string, db int, logger *zap.Logger) *Cache {
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		PoolSize:     20,
		MinIdleConns: 5,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})
	return &Cache{
		client: client,
		logger: logger,
		accs:   make(map[string]*accumulator),
	}
}

// Ingest processes a new snapshot: updates latest, accumulates for aggregation,
// appends to history stream, and writes aggregated cache.
func (c *Cache) Ingest(ctx context.Context, snap *metrics.SystemSnapshot) error {
	pipe := c.client.Pipeline()

	// 1. Store latest snapshot
	latestKey := fmt.Sprintf(keyLatest, snap.AgentID)
	latestJSON, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("marshal latest: %w", err)
	}
	pipe.Set(ctx, latestKey, latestJSON, ttlLatest)

	// 2. Track agent in sorted set (score = timestamp for LRU queries)
	pipe.ZAdd(ctx, keyAllAgents, &redis.Z{
		Score:  float64(snap.TimestampMS),
		Member: snap.AgentID + "|" + snap.Hostname,
	})

	// 3. Append to per-agent time series stream (capped)
	pipe.XAdd(ctx, &redis.XAddArgs{
		Stream: fmt.Sprintf(keyHistory, snap.AgentID),
		MaxLen: maxHistory,
		Approx: true,
		Values: map[string]interface{}{
			"cpu":  snap.CPUTotal,
			"mem":  snap.MemPercent,
			"ts":   snap.TimestampMS,
		},
	})

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("pipeline exec: %w", err)
	}

	// 4. Incremental in-memory aggregation
	agg := c.updateAccumulator(snap)

	// 5. Write aggregated result back to Redis
	aggKey := fmt.Sprintf(keyAgg, snap.AgentID)
	aggJSON, err := json.Marshal(agg)
	if err != nil {
		return fmt.Errorf("marshal agg: %w", err)
	}
	if err := c.client.Set(ctx, aggKey, aggJSON, ttlAgg).Err(); err != nil {
		c.logger.Warn("failed to write agg cache", zap.String("agent", snap.AgentID), zap.Error(err))
	}

	return nil
}

func (c *Cache) updateAccumulator(snap *metrics.SystemSnapshot) metrics.AggregatedMetrics {
	c.mu.Lock()
	defer c.mu.Unlock()
	acc, ok := c.accs[snap.AgentID]
	if !ok {
		acc = &accumulator{
			agentID:     snap.AgentID,
			hostname:    snap.Hostname,
			windowStart: snap.Time(),
		}
		c.accs[snap.AgentID] = acc
	}
	// Reset window every 5 minutes
	if time.Since(acc.windowStart) > 5*time.Minute {
		acc.count = 0
		acc.sumCPU = 0; acc.maxCPU = 0
		acc.sumMem = 0; acc.maxMem = 0
		acc.sumNetSent = 0; acc.sumNetRecv = 0
		acc.windowStart = snap.Time()
	}
	acc.update(snap)
	return acc.toAggregated(snap.Time())
}

// GetLatest returns the most recent snapshot for an agent
func (c *Cache) GetLatest(ctx context.Context, agentID string) (*metrics.SystemSnapshot, error) {
	val, err := c.client.Get(ctx, fmt.Sprintf(keyLatest, agentID)).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var snap metrics.SystemSnapshot
	if err := json.Unmarshal(val, &snap); err != nil {
		return nil, err
	}
	return &snap, nil
}

// GetAggregated returns the cached aggregated metrics for an agent
func (c *Cache) GetAggregated(ctx context.Context, agentID string) (*metrics.AggregatedMetrics, error) {
	val, err := c.client.Get(ctx, fmt.Sprintf(keyAgg, agentID)).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var agg metrics.AggregatedMetrics
	if err := json.Unmarshal(val, &agg); err != nil {
		return nil, err
	}
	return &agg, nil
}

// ListAgents returns all known agent IDs (sorted by last seen)
func (c *Cache) ListAgents(ctx context.Context) ([]string, error) {
	members, err := c.client.ZRevRange(ctx, keyAllAgents, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	return members, nil
}

// GetHistory returns recent CPU/mem time series for an agent (last N entries)
func (c *Cache) GetHistory(ctx context.Context, agentID string, count int64) ([]map[string]interface{}, error) {
	msgs, err := c.client.XRevRangeN(ctx, fmt.Sprintf(keyHistory, agentID), "+", "-", count).Result()
	if err != nil {
		return nil, err
	}
	result := make([]map[string]interface{}, 0, len(msgs))
	for _, m := range msgs {
		result = append(result, m.Values)
	}
	return result, nil
}

// Health checks Redis connectivity
func (c *Cache) Health(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

func (c *Cache) Close() error {
	return c.client.Close()
}
