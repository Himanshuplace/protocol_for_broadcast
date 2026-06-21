package collector

import (
	"context"
	"time"
)

// RunResult captures all measured metrics from one benchmark scenario run.
type RunResult struct {
	RunID          string    `json:"run_id"`
	Protocol       string    `json:"protocol"`
	Scenario       string    `json:"scenario"`
	MsgSize        int       `json:"msg_size"`
	GeneratorType  string    `json:"generator_type"`
	NetProfile     string    `json:"net_profile"`
	BroadcastStrat string    `json:"broadcast_strat"`
	ReceiverCount  int       `json:"receiver_count"`
	SenderCount    int       `json:"sender_count"`
	DurationS      int       `json:"duration_s"`
	WarmupS        int       `json:"warmup_s"`
	StartedAt      time.Time `json:"started_at"`
	EndedAt        time.Time `json:"ended_at"`
	GoVersion      string    `json:"go_version"`
	OSArch         string    `json:"os_arch"`
	CPUModel       string    `json:"cpu_model"`

	// Latency (nanoseconds)
	LatMinNs    int64 `json:"lat_min_ns"`
	LatAvgNs    int64 `json:"lat_avg_ns"`
	LatP50Ns    int64 `json:"lat_p50_ns"`
	LatP95Ns    int64 `json:"lat_p95_ns"`
	LatP99Ns    int64 `json:"lat_p99_ns"`
	LatP999Ns   int64 `json:"lat_p999_ns"`
	LatMaxNs    int64 `json:"lat_max_ns"`
	LatStddevNs int64 `json:"lat_stddev_ns"`

	// Throughput
	MsgsPerSec    float64 `json:"msgs_per_sec"`
	BytesPerSec   float64 `json:"bytes_per_sec"`
	TotalMsgsSent int64   `json:"total_msgs_sent"`
	TotalMsgsRecv int64   `json:"total_msgs_recv"`

	// Reliability
	MsgsLost       int64   `json:"msgs_lost"`
	MsgsReordered  int64   `json:"msgs_reordered"`
	MsgsDuplicated int64   `json:"msgs_duplicated"`
	LossRatePct    float64 `json:"loss_rate_pct"`

	// Resources
	CPUPctAvg     float64 `json:"cpu_pct_avg"`
	CPUPctP99     float64 `json:"cpu_pct_p99"`
	MemBytesAvg   int64   `json:"mem_bytes_avg"`
	MemBytesMax   int64   `json:"mem_bytes_max"`
	GoroutinesAvg int32   `json:"goroutines_avg"`
	GoroutinesMax int32   `json:"goroutines_max"`
	FDCountAvg    int32   `json:"fd_count_avg"`
	FDCountMax    int32   `json:"fd_count_max"`

	// Connection lifecycle
	HandshakeAvgNs int64 `json:"handshake_avg_ns"`
	HandshakeP99Ns int64 `json:"handshake_p99_ns"`
	ReconnectAvgNs int64 `json:"reconnect_avg_ns"`
	ReconnectP99Ns int64 `json:"reconnect_p99_ns"`

	// Per-client breakdown (one entry per connected subscriber). Populated for
	// multi-receiver broadcast runs; empty otherwise.
	PerClient []ClientStat `json:"per_client,omitempty"`

	// Arbitrary extra fields
	Config map[string]any `json:"config,omitempty"`
}

// ClientStat is the per-subscriber view of a broadcast run: what this one client
// received, how much it missed relative to the fastest client, and its latency.
type ClientStat struct {
	ClientID    string  `json:"client_id"`
	MsgRecv     int64   `json:"msg_recv"`
	Delivered   int64   `json:"delivered"`
	Lost        int64   `json:"lost"`
	Duplicated  int64   `json:"duplicated"`
	Reordered   int64   `json:"reordered"`
	LossRatePct float64 `json:"loss_rate_pct"`
	FirstSeq    uint64  `json:"first_seq"`
	LastSeq     uint64  `json:"last_seq"`
	LatP50Ns    int64   `json:"lat_p50_ns"`
	LatP99Ns    int64   `json:"lat_p99_ns"`
	LatMaxNs    int64   `json:"lat_max_ns"`
}

// ResultCollector stores and retrieves benchmark results.
type ResultCollector interface {
	Store(ctx context.Context, result *RunResult) error
	List(ctx context.Context, protocol, scenario string, limit int) ([]*RunResult, error)
	Close() error
}
