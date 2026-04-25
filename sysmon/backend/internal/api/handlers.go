package api

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/yourusername/sysmon/internal/alert"
	"github.com/yourusername/sysmon/internal/metrics"
	rediscache "github.com/yourusername/sysmon/internal/redis"
)

type Handler struct {
	cache  *rediscache.Cache
	alerts *alert.Engine
	logger *zap.Logger
	startTime time.Time
}

func NewHandler(cache *rediscache.Cache, alerts *alert.Engine, logger *zap.Logger) *Handler {
	return &Handler{cache: cache, alerts: alerts, logger: logger, startTime: time.Now()}
}

func (h *Handler) RegisterRoutes(r *gin.Engine) {
	v1 := r.Group("/api/v1")
	{
		v1.GET("/health",              h.Health)
		v1.GET("/agents",              h.ListAgents)
		v1.GET("/agents/:id/latest",   h.GetLatest)
		v1.GET("/agents/:id/agg",      h.GetAggregated)
		v1.GET("/agents/:id/history",  h.GetHistory)
		v1.GET("/alerts",              h.ListAlerts)
		v1.GET("/alerts/rules",        h.ListRules)
		v1.POST("/alerts/rules",       h.CreateRule)
		v1.DELETE("/alerts/rules/:id", h.DeleteRule)
		v1.GET("/summary",             h.Summary)
	}
}

// GET /api/v1/health
func (h *Handler) Health(c *gin.Context) {
	ctx := c.Request.Context()
	redisOK := h.cache.Health(ctx) == nil
	status := "ok"
	code := http.StatusOK
	if !redisOK {
		status = "degraded"
		code = http.StatusServiceUnavailable
	}
	c.JSON(code, gin.H{
		"status":    status,
		"redis":     redisOK,
		"uptime_s":  time.Since(h.startTime).Seconds(),
		"timestamp": time.Now().UTC(),
	})
}

// GET /api/v1/agents
func (h *Handler) ListAgents(c *gin.Context) {
	agents, err := h.cache.ListAgents(c.Request.Context())
	if err != nil {
		h.logger.Error("list agents", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"agents": agents, "count": len(agents)})
}

// GET /api/v1/agents/:id/latest
func (h *Handler) GetLatest(c *gin.Context) {
	agentID := c.Param("id")
	snap, err := h.cache.GetLatest(c.Request.Context(), agentID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if snap == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "agent not found or data expired"})
		return
	}
	c.JSON(http.StatusOK, snap)
}

// GET /api/v1/agents/:id/agg
func (h *Handler) GetAggregated(c *gin.Context) {
	agentID := c.Param("id")
	agg, err := h.cache.GetAggregated(c.Request.Context(), agentID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if agg == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "no aggregated data yet"})
		return
	}
	c.JSON(http.StatusOK, agg)
}

// GET /api/v1/agents/:id/history?n=100
func (h *Handler) GetHistory(c *gin.Context) {
	agentID := c.Param("id")
	n := int64(60)
	if nStr := c.Query("n"); nStr != "" {
		if parsed, err := strconv.ParseInt(nStr, 10, 64); err == nil && parsed > 0 && parsed <= 1000 {
			n = parsed
		}
	}
	history, err := h.cache.GetHistory(c.Request.Context(), agentID, n)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"agent_id": agentID, "entries": history, "count": len(history)})
}

// GET /api/v1/alerts
func (h *Handler) ListAlerts(c *gin.Context) {
	active := h.alerts.ListActive()
	c.JSON(http.StatusOK, gin.H{"alerts": active, "count": len(active)})
}

// GET /api/v1/alerts/rules
func (h *Handler) ListRules(c *gin.Context) {
	rules := h.alerts.ListRules()
	c.JSON(http.StatusOK, gin.H{"rules": rules})
}

// POST /api/v1/alerts/rules
func (h *Handler) CreateRule(c *gin.Context) {
	var rule metrics.AlertRule
	if err := c.ShouldBindJSON(&rule); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if rule.ID == "" || rule.Metric == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id and metric are required"})
		return
	}
	rule.Enabled = true
	h.alerts.UpsertRule(rule)
	c.JSON(http.StatusCreated, rule)
}

// DELETE /api/v1/alerts/rules/:id
func (h *Handler) DeleteRule(c *gin.Context) {
	id := c.Param("id")
	h.alerts.DeleteRule(id)
	c.JSON(http.StatusOK, gin.H{"deleted": id})
}

// GET /api/v1/summary — cross-agent rollup
func (h *Handler) Summary(c *gin.Context) {
	ctx := c.Request.Context()
	agents, err := h.cache.ListAgents(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	type agentSummary struct {
		AgentEntry  string  `json:"agent"`
		CPU         float64 `json:"cpu_percent"`
		Mem         float64 `json:"mem_percent"`
		ProcessCount int    `json:"process_count"`
	}

	summaries := make([]agentSummary, 0, len(agents))
	for _, entry := range agents {
		// entry format: "agentID|hostname"
		agentID := entry
		for i, ch := range entry {
			if ch == '|' { agentID = entry[:i]; break }
		}
		snap, err := h.cache.GetLatest(ctx, agentID)
		if err != nil || snap == nil {
			continue
		}
		summaries = append(summaries, agentSummary{
			AgentEntry:   entry,
			CPU:          snap.CPUTotal,
			Mem:          snap.MemPercent,
			ProcessCount: len(snap.Processes),
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"agent_count": len(summaries),
		"agents":      summaries,
		"active_alerts": len(h.alerts.ListActive()),
		"timestamp":   time.Now().UTC(),
	})
}
