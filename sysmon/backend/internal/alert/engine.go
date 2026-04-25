package alert

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/yourusername/sysmon/internal/metrics"
)

// Notifier sends alert events (webhook, Slack, PagerDuty, etc.)
type Notifier interface {
	Notify(ctx context.Context, alert metrics.Alert) error
}

// Engine evaluates rules against incoming snapshots
type Engine struct {
	mu         sync.RWMutex
	rules      map[string]metrics.AlertRule
	active     map[string]*metrics.Alert // ruleID+agentID -> active alert
	notifiers  []Notifier
	logger     *zap.Logger
	violations map[string]time.Time // tracks when rule first fired per agent
}

func NewEngine(logger *zap.Logger, notifiers ...Notifier) *Engine {
	return &Engine{
		rules:      make(map[string]metrics.AlertRule),
		active:     make(map[string]*metrics.Alert),
		violations: make(map[string]time.Time),
		notifiers:  notifiers,
		logger:     logger,
	}
}

// UpsertRule adds or updates a rule
func (e *Engine) UpsertRule(r metrics.AlertRule) {
	e.mu.Lock()
	e.rules[r.ID] = r
	e.mu.Unlock()
}

// DeleteRule removes a rule
func (e *Engine) DeleteRule(id string) {
	e.mu.Lock()
	delete(e.rules, id)
	e.mu.Unlock()
}

// ListRules returns all current rules
func (e *Engine) ListRules() []metrics.AlertRule {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]metrics.AlertRule, 0, len(e.rules))
	for _, r := range e.rules {
		out = append(out, r)
	}
	return out
}

// ListActive returns all currently active (unfired) alerts
func (e *Engine) ListActive() []metrics.Alert {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]metrics.Alert, 0, len(e.active))
	for _, a := range e.active {
		out = append(out, *a)
	}
	return out
}

// Evaluate checks a snapshot against all rules
func (e *Engine) Evaluate(ctx context.Context, snap *metrics.SystemSnapshot) {
	e.mu.Lock()
	defer e.mu.Unlock()

	for _, rule := range e.rules {
		if !rule.Enabled {
			continue
		}
		value, ok := e.extractMetric(rule, snap)
		if !ok {
			continue
		}
		key := rule.ID + "|" + snap.AgentID
		if value >= rule.Threshold {
			// Threshold exceeded — track first-seen time
			if _, seen := e.violations[key]; !seen {
				e.violations[key] = time.Now()
			}
			elapsed := time.Since(e.violations[key])
			if elapsed >= time.Duration(rule.Duration)*time.Second {
				// Fire alert if not already active
				if _, active := e.active[key]; !active {
					a := metrics.Alert{
						ID:        uuid.NewString(),
						RuleID:    rule.ID,
						RuleName:  rule.Name,
						AgentID:   snap.AgentID,
						Hostname:  snap.Hostname,
						Metric:    rule.Metric,
						Value:     value,
						Threshold: rule.Threshold,
						Severity:  rule.Severity,
						FiredAt:   time.Now(),
					}
					e.active[key] = &a
					e.logger.Warn("alert fired",
						zap.String("rule", rule.Name),
						zap.String("agent", snap.AgentID),
						zap.Float64("value", value),
						zap.Float64("threshold", rule.Threshold),
					)
					go e.notify(ctx, a)
				} else {
					// Update current value
					e.active[key].Value = value
				}
			}
		} else {
			// Below threshold — resolve if active
			delete(e.violations, key)
			if a, active := e.active[key]; active {
				now := time.Now()
				a.Resolved = true
				a.ResolvedAt = &now
				e.logger.Info("alert resolved",
					zap.String("rule", rule.Name),
					zap.String("agent", snap.AgentID),
				)
				go e.notify(ctx, *a)
				delete(e.active, key)
			}
		}
	}
}

func (e *Engine) extractMetric(rule metrics.AlertRule, snap *metrics.SystemSnapshot) (float64, bool) {
	switch rule.Metric {
	case "cpu":
		return snap.CPUTotal, true
	case "mem":
		return snap.MemPercent, true
	case "net_sent":
		return float64(snap.NetBytesSent), true
	case "disk_read":
		return float64(snap.DiskReadBytes), true
	default:
		return 0, false
	}
}

func (e *Engine) notify(ctx context.Context, a metrics.Alert) {
	for _, n := range e.notifiers {
		if err := n.Notify(ctx, a); err != nil {
			e.logger.Error("notifier failed", zap.Error(err))
		}
	}
}

// DefaultRules returns sensible production defaults
func DefaultRules() []metrics.AlertRule {
	return []metrics.AlertRule{
		{
			ID: "cpu-critical", Name: "CPU Critical",
			Metric: "cpu", Threshold: 90.0, Duration: 30,
			Severity: "critical", Enabled: true,
		},
		{
			ID: "cpu-warn", Name: "CPU Warning",
			Metric: "cpu", Threshold: 75.0, Duration: 60,
			Severity: "warn", Enabled: true,
		},
		{
			ID: "mem-critical", Name: "Memory Critical",
			Metric: "mem", Threshold: 90.0, Duration: 30,
			Severity: "critical", Enabled: true,
		},
		{
			ID: "mem-warn", Name: "Memory Warning",
			Metric: "mem", Threshold: 80.0, Duration: 60,
			Severity: "warn", Enabled: true,
		},
	}
}
