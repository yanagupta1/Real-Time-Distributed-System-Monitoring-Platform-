package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/yourusername/sysmon/internal/metrics"
)

// Handler is called for each decoded snapshot
type Handler func(ctx context.Context, snap *metrics.SystemSnapshot) error

type ConsumerConfig struct {
	Brokers        []string
	Topic          string
	GroupID        string
	MinBytes       int
	MaxBytes       int
	MaxWait        time.Duration
	CommitInterval time.Duration
	NumWorkers     int
}

func DefaultConsumerConfig() ConsumerConfig {
	return ConsumerConfig{
		Brokers:        []string{"localhost:9092"},
		Topic:          "sysmon.metrics",
		GroupID:        "sysmon-backend",
		MinBytes:       1e3,   // 1 KB
		MaxBytes:       10e6,  // 10 MB
		MaxWait:        250 * time.Millisecond,
		CommitInterval: time.Second,
		NumWorkers:     8,
	}
}

type Consumer struct {
	cfg     ConsumerConfig
	reader  *kafka.Reader
	logger  *zap.Logger
	handler Handler
	wg      sync.WaitGroup
	cancel  context.CancelFunc

	// Stats
	mu          sync.Mutex
	msgCount    uint64
	errCount    uint64
	lastMsg     time.Time
}

func NewConsumer(cfg ConsumerConfig, handler Handler, logger *zap.Logger) *Consumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		Topic:          cfg.Topic,
		GroupID:        cfg.GroupID,
		MinBytes:       cfg.MinBytes,
		MaxBytes:       cfg.MaxBytes,
		MaxWait:        cfg.MaxWait,
		CommitInterval: cfg.CommitInterval,
	})
	return &Consumer{cfg: cfg, reader: r, logger: logger, handler: handler}
}

func (c *Consumer) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	// Fan-out to worker pool
	msgCh := make(chan kafka.Message, c.cfg.NumWorkers*4)

	// Reader goroutine
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer close(msgCh)
		for {
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				c.logger.Error("kafka fetch error", zap.Error(err))
				c.mu.Lock(); c.errCount++; c.mu.Unlock()
				continue
			}
			select {
			case msgCh <- msg:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Worker pool
	for i := 0; i < c.cfg.NumWorkers; i++ {
		c.wg.Add(1)
		go func(workerID int) {
			defer c.wg.Done()
			for msg := range msgCh {
				if err := c.process(ctx, msg); err != nil {
					c.logger.Error("process error",
						zap.Int("worker", workerID),
						zap.Error(err))
				}
				if err := c.reader.CommitMessages(ctx, msg); err != nil && ctx.Err() == nil {
					c.logger.Warn("commit error", zap.Error(err))
				}
			}
		}(i)
	}
}

func (c *Consumer) process(ctx context.Context, msg kafka.Message) error {
	var snap metrics.SystemSnapshot
	if err := json.Unmarshal(msg.Value, &snap); err != nil {
		return fmt.Errorf("unmarshal snapshot: %w", err)
	}

	if err := c.handler(ctx, &snap); err != nil {
		return fmt.Errorf("handler: %w", err)
	}

	c.mu.Lock()
	c.msgCount++
	c.lastMsg = time.Now()
	c.mu.Unlock()
	return nil
}

func (c *Consumer) Stop() {
	if c.cancel != nil {
		c.cancel()
	}
	c.reader.Close()
	c.wg.Wait()
}

func (c *Consumer) Stats() (msgCount, errCount uint64, lastMsg time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.msgCount, c.errCount, c.lastMsg
}
