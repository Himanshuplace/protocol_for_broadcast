package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// MetricLabels are the common label keys used across all Prometheus metrics.
// These form the primary axes for Grafana dashboard filtering.
const (
	LabelProtocol     = "protocol"     // e.g., "udp", "tcp", "websocket-gorilla"
	LabelScenario     = "scenario"     // e.g., "A", "B", "C", "D", "E"
	LabelMsgSize      = "msg_size"     // e.g., "64", "1024", "65536"
	LabelNetCondition = "net_condition" // e.g., "clean", "loss1", "latency20ms"
	LabelDirection    = "direction"    // "sent" or "recv"
	LabelStrategy     = "strategy"     // broadcast strategy: "naive", "workerpool", etc.
	LabelComponent    = "component"    // "server" or "client"
	LabelAssetClass   = "asset_class"  // "equity", "future", "option", etc.
)

// BenchmarkMetrics holds all Prometheus metric vectors for the benchmark platform.
// Create one instance per benchmark run and pass to each transport implementation.
// Uses a private prometheus.Registry (not the global default) to allow multiple
// concurrent benchmarks in the same process without metric name collisions.
type BenchmarkMetrics struct {
	reg prometheus.Registerer

	// Latency: nanosecond-resolution histogram with exponential buckets
	// Buckets: 1µs, 2µs, 4µs, 8µs, ... up to ~1s (29 buckets)
	LatencyNs *prometheus.HistogramVec

	// Throughput counters (monotonically increasing)
	ThroughputMessages *prometheus.CounterVec
	ThroughputBytes    *prometheus.CounterVec

	// Connection gauge: current active connections
	ConnectionsActive *prometheus.GaugeVec

	// Reliability counters
	PacketLost       *prometheus.CounterVec
	PacketDuplicated *prometheus.CounterVec
	PacketReordered  *prometheus.CounterVec

	// Resource gauges (updated from ResourceSampler)
	CPUPercent  *prometheus.GaugeVec
	MemoryBytes *prometheus.GaugeVec
	Goroutines  *prometheus.GaugeVec
	FDCount     *prometheus.GaugeVec

	// Connection lifecycle histograms
	HandshakeDurationNs *prometheus.HistogramVec
	ReconnectDurationNs *prometheus.HistogramVec

	// Broadcast strategy performance
	BroadcastDurationNs *prometheus.HistogramVec

	// Market data specific
	TicksPublished  *prometheus.CounterVec
	TicksDelivered  *prometheus.CounterVec
	TickLatencyNs   *prometheus.HistogramVec
}

// NewBenchmarkMetrics creates and registers all metrics with the given registerer.
// Pass prometheus.NewRegistry() for test isolation or prometheus.DefaultRegisterer
// for the global registry (shared between instances — use only one at a time).
func NewBenchmarkMetrics(reg prometheus.Registerer) (*BenchmarkMetrics, error) {
	m := &BenchmarkMetrics{reg: reg}

	// Latency histogram: exponential buckets from 1µs to ~1.07s
	// 29 buckets × 2× growth: 1000, 2000, 4000, 8000, ... 536870912 nanoseconds
	latencyBuckets := prometheus.ExponentialBuckets(1_000, 2, 29)

	var err error
	register := func(c prometheus.Collector) {
		if err == nil {
			err = reg.Register(c)
		}
	}

	m.LatencyNs = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "benchmark_latency_nanoseconds",
		Help:    "One-way message latency from send timestamp to receive time, in nanoseconds.",
		Buckets: latencyBuckets,
	}, []string{LabelProtocol, LabelScenario, LabelMsgSize, LabelNetCondition})
	register(m.LatencyNs)

	m.ThroughputMessages = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "benchmark_throughput_messages_total",
		Help: "Total benchmark messages sent or received.",
	}, []string{LabelProtocol, LabelDirection})
	register(m.ThroughputMessages)

	m.ThroughputBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "benchmark_throughput_bytes_total",
		Help: "Total benchmark bytes sent or received.",
	}, []string{LabelProtocol, LabelDirection})
	register(m.ThroughputBytes)

	m.ConnectionsActive = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "benchmark_connections_active",
		Help: "Number of currently connected clients.",
	}, []string{LabelProtocol})
	register(m.ConnectionsActive)

	m.PacketLost = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "benchmark_packets_lost_total",
		Help: "Total messages declared lost (gap in sequence number).",
	}, []string{LabelProtocol, LabelScenario})
	register(m.PacketLost)

	m.PacketDuplicated = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "benchmark_packets_duplicated_total",
		Help: "Total messages received more than once.",
	}, []string{LabelProtocol})
	register(m.PacketDuplicated)

	m.PacketReordered = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "benchmark_packets_reordered_total",
		Help: "Total messages received out of sequence order.",
	}, []string{LabelProtocol})
	register(m.PacketReordered)

	m.CPUPercent = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "benchmark_cpu_percent",
		Help: "Process CPU utilization percentage.",
	}, []string{LabelProtocol, LabelComponent})
	register(m.CPUPercent)

	m.MemoryBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "benchmark_memory_bytes",
		Help: "Process RSS memory in bytes.",
	}, []string{LabelProtocol, LabelComponent})
	register(m.MemoryBytes)

	m.Goroutines = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "benchmark_goroutines",
		Help: "Number of live goroutines.",
	}, []string{LabelProtocol, LabelComponent})
	register(m.Goroutines)

	m.FDCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "benchmark_fd_count",
		Help: "Number of open file descriptors.",
	}, []string{LabelProtocol, LabelComponent})
	register(m.FDCount)

	// Connection lifecycle: nanosecond buckets from 100µs to ~30s
	lifecycleBuckets := prometheus.ExponentialBuckets(100_000, 3, 18)

	m.HandshakeDurationNs = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "benchmark_handshake_nanoseconds",
		Help:    "Connection establishment (handshake) duration in nanoseconds.",
		Buckets: lifecycleBuckets,
	}, []string{LabelProtocol})
	register(m.HandshakeDurationNs)

	m.ReconnectDurationNs = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "benchmark_reconnect_nanoseconds",
		Help:    "Reconnection duration after disconnect, in nanoseconds.",
		Buckets: lifecycleBuckets,
	}, []string{LabelProtocol})
	register(m.ReconnectDurationNs)

	m.BroadcastDurationNs = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "benchmark_broadcast_nanoseconds",
		Help:    "Time to complete one Broadcast() call to all receivers, in nanoseconds.",
		Buckets: prometheus.ExponentialBuckets(1_000, 2, 25),
	}, []string{LabelProtocol, LabelStrategy, "receivers"})
	register(m.BroadcastDurationNs)

	// Market data metrics
	m.TicksPublished = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "benchmark_market_ticks_published_total",
		Help: "Total market data ticks published by the feed generator.",
	}, []string{LabelAssetClass, "symbol"})
	register(m.TicksPublished)

	m.TicksDelivered = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "benchmark_market_ticks_delivered_total",
		Help: "Total market data ticks delivered to at least one subscriber.",
	}, []string{LabelProtocol, LabelAssetClass})
	register(m.TicksDelivered)

	m.TickLatencyNs = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "benchmark_market_tick_latency_nanoseconds",
		Help:    "Tick-to-trade latency: time from tick generation to subscriber delivery.",
		Buckets: latencyBuckets,
	}, []string{LabelProtocol, LabelAssetClass})
	register(m.TickLatencyNs)

	if err != nil {
		return nil, err
	}
	return m, nil
}

// MustNewBenchmarkMetrics panics if registration fails. Use in main() only.
func MustNewBenchmarkMetrics(reg prometheus.Registerer) *BenchmarkMetrics {
	m, err := NewBenchmarkMetrics(reg)
	if err != nil {
		panic("BenchmarkMetrics registration failed: " + err.Error())
	}
	return m
}
