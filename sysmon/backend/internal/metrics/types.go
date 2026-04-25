package metrics

import "time"

// ProcessMetrics holds per-process snapshot data
type ProcessMetrics struct {
	PID        int     `json:"pid"`
	Name       string  `json:"name"`
	CPUPercent float64 `json:"cpu_percent"`
	MemRSSKB   uint64  `json:"mem_rss_kb"`
	MemVMSKB   uint64  `json:"mem_vms_kb"`
	NumThreads uint32  `json:"num_threads"`
	ReadBytes  uint64  `json:"read_bytes"`
	WriteBytes uint64  `json:"write_bytes"`
	Status     string  `json:"status"`
}

// SystemSnapshot is the top-level payload from a C++ agent
type SystemSnapshot struct {
	TimestampMS   int64            `json:"timestamp_ms"`
	Hostname      string           `json:"hostname"`
	AgentID       string           `json:"agent_id"`
	CPUTotal      float64          `json:"cpu_total_percent"`
	CPUPerCore    []float64        `json:"cpu_per_core"`
	MemTotalKB    uint64           `json:"mem_total_kb"`
	MemUsedKB     uint64           `json:"mem_used_kb"`
	MemAvailKB    uint64           `json:"mem_available_kb"`
	MemPercent    float64          `json:"mem_percent"`
	NetBytesSent  uint64           `json:"net_bytes_sent"`
	NetBytesRecv  uint64           `json:"net_bytes_recv"`
	DiskReadBytes uint64           `json:"disk_read_bytes"`
	DiskWriteBytes uint64          `json:"disk_write_bytes"`
	Processes     []ProcessMetrics `json:"processes"`
}

func (s *SystemSnapshot) Time() time.Time {
	return time.UnixMilli(s.TimestampMS)
}

// AggregatedMetrics is incrementally computed and cached in Redis
type AggregatedMetrics struct {
	AgentID        string    `json:"agent_id"`
	Hostname       string    `json:"hostname"`
	WindowStart    time.Time `json:"window_start"`
	WindowEnd      time.Time `json:"window_end"`
	SampleCount    int       `json:"sample_count"`
	AvgCPU         float64   `json:"avg_cpu"`
	MaxCPU         float64   `json:"max_cpu"`
	AvgMemPercent  float64   `json:"avg_mem_percent"`
	MaxMemPercent  float64   `json:"max_mem_percent"`
	AvgNetSentBps  float64   `json:"avg_net_sent_bps"`
	AvgNetRecvBps  float64   `json:"avg_net_recv_bps"`
	TopProcesses   []ProcessMetrics `json:"top_processes"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// AlertRule defines a threshold-based rule
type AlertRule struct {
	ID        string  `json:"id"`
	Name      string  `json:"name"`
	Metric    string  `json:"metric"`  // "cpu", "mem", "proc_cpu"
	Threshold float64 `json:"threshold"`
	Duration  int     `json:"duration_s"` // seconds above threshold
	Severity  string  `json:"severity"`   // "warn" | "critical"
	Enabled   bool    `json:"enabled"`
}

// Alert is a triggered alert event
type Alert struct {
	ID        string    `json:"id"`
	RuleID    string    `json:"rule_id"`
	RuleName  string    `json:"rule_name"`
	AgentID   string    `json:"agent_id"`
	Hostname  string    `json:"hostname"`
	Metric    string    `json:"metric"`
	Value     float64   `json:"value"`
	Threshold float64   `json:"threshold"`
	Severity  string    `json:"severity"`
	FiredAt   time.Time `json:"fired_at"`
	Resolved  bool      `json:"resolved"`
	ResolvedAt *time.Time `json:"resolved_at,omitempty"`
}
