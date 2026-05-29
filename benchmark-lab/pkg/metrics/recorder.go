package metrics

import (
	"sync/atomic"
	"time"
)

// Recorder is the central measurement sink for a single benchmark run.
// Inject one Recorder per transport+scenario combination. All methods are safe
// for concurrent use from multiple goroutines.
//
// Hot path call order (per received message):
//  1. RecordRecv(seq, sendNs, size) → updates atomic counters + records HDR latency
//  2. At end-of-run: Snapshot() → computes final stats from HDR + sequence tracker
//
// RecordRecv is designed to be as fast as possible:
//   - Atomic counter increments: ~2ns each (single CPU instruction)
//   - HDR histogram record: ~50ns (mutex acquire + bitset update)
//   - Sequence track observe: ~30ns (mutex + bit array operation)
//
// Total overhead per message: ~100ns worst case — acceptable even at 10M msg/sec.
type Recorder struct {
	label    string // "protocol/scenario/msg_size/net_condition" for logging
	protocol string
	scenario string

	// HDR histogram for end-of-run latency report (accurate percentiles)
	latency *HDRHistogram

	// Sequence-based reliability tracking
	seqTracker *SequenceTracker

	// Background resource sampler
	resources *ResourceSampler

	// Prometheus metrics (optional — nil if not configured)
	prom *BenchmarkMetrics

	// Atomic counters — updated in the hot path with no locking
	msgSent   atomic.Uint64
	msgRecv   atomic.Uint64
	bytesSent atomic.Uint64
	bytesRecv atomic.Uint64

	// Handshake/reconnect tracking (separate histogram, lower sample rate)
	handshake *HDRHistogram
	reconnect *HDRHistogram

	startTime time.Time
}

// RecorderConfig configures a Recorder instance.
type RecorderConfig struct {
	Label             string
	Protocol          string
	Scenario          string
	SequenceWindowSize uint64 // default 4096
	SampleInterval    time.Duration
	Prometheus        *BenchmarkMetrics // optional; nil = no Prometheus export
}

// NewRecorder creates a Recorder with the given configuration.
func NewRecorder(cfg RecorderConfig) *Recorder {
	if cfg.SequenceWindowSize == 0 {
		cfg.SequenceWindowSize = 4096
	}
	if cfg.SampleInterval == 0 {
		cfg.SampleInterval = 100 * time.Millisecond
	}
	return &Recorder{
		label:      cfg.Label,
		protocol:   cfg.Protocol,
		scenario:   cfg.Scenario,
		latency:    NewHDRHistogram(),
		seqTracker: NewSequenceTracker(cfg.SequenceWindowSize),
		resources:  NewResourceSampler(cfg.SampleInterval),
		handshake:  NewHDRHistogram(),
		reconnect:  NewHDRHistogram(),
		prom:       cfg.Prometheus,
		startTime:  time.Now(),
	}
}

// Start begins background resource sampling. Call before the benchmark run.
func (r *Recorder) Start() {
	r.startTime = time.Now()
	r.resources.Start()
}

// Stop ends background resource sampling. Call after the benchmark run.
func (r *Recorder) Stop() {
	r.resources.Stop()
}

// RecordSend records one outbound message of size bytes with the given sequence number.
// Called on the sender side for each message dispatched to Broadcast/Send.
//
//go:nosplit
func (r *Recorder) RecordSend(seq uint64, size int) {
	r.msgSent.Add(1)
	r.bytesSent.Add(uint64(size))
}

// RecordRecv records one inbound message. This is the hot path called for every
// received message on the receiver side.
//
// seq and sendNs come directly from the wire frame header (pkg/wire).
// recvNs should be time.Now().UnixNano() captured immediately after the socket read.
//
//go:nosplit
func (r *Recorder) RecordRecv(seq uint64, sendNs int64, size int, recvNs int64) {
	r.msgRecv.Add(1)
	r.bytesRecv.Add(uint64(size))

	// Record latency: recvNs - sendNs (one-way, requires clock sync for distributed)
	latNs := recvNs - sendNs
	r.latency.Record(latNs)
	r.seqTracker.Observe(seq)

	// Prometheus update (optional, ~10ns overhead with pointer check)
	if r.prom != nil {
		r.prom.LatencyNs.
			WithLabelValues(r.protocol, r.scenario, "0", "clean").
			Observe(float64(latNs))
		r.prom.ThroughputMessages.
			WithLabelValues(r.protocol, "recv").
			Inc()
		r.prom.ThroughputBytes.
			WithLabelValues(r.protocol, "recv").
			Add(float64(size))
	}
}

// RecordHandshake records the time taken to establish one connection.
func (r *Recorder) RecordHandshake(d time.Duration) {
	r.handshake.Record(d.Nanoseconds())
	if r.prom != nil {
		r.prom.HandshakeDurationNs.
			WithLabelValues(r.protocol).
			Observe(float64(d.Nanoseconds()))
	}
}

// RecordReconnect records the time taken to re-establish a dropped connection.
func (r *Recorder) RecordReconnect(d time.Duration) {
	r.reconnect.Record(d.Nanoseconds())
	if r.prom != nil {
		r.prom.ReconnectDurationNs.
			WithLabelValues(r.protocol).
			Observe(float64(d.Nanoseconds()))
	}
}

// Flush finalizes loss accounting. Call once at end-of-run with the last
// sequence number sent. Must be called before Snapshot() for accurate loss counts.
func (r *Recorder) Flush(lastSentSeq uint64) {
	r.seqTracker.Flush(lastSentSeq)
}

// Reset clears all metrics for reuse between warmup and measurement phases.
func (r *Recorder) Reset() {
	r.latency.Reset()
	r.handshake.Reset()
	r.reconnect.Reset()
	r.seqTracker.Reset()
	r.resources.Reset()
	r.msgSent.Store(0)
	r.msgRecv.Store(0)
	r.bytesSent.Store(0)
	r.bytesRecv.Store(0)
	r.startTime = time.Now()
}

// RecorderSnapshot is the final measurement result for one benchmark run.
type RecorderSnapshot struct {
	Label    string
	Protocol string
	Scenario string
	Duration time.Duration

	// Latency
	Latency HistogramSnapshot
	// Throughput
	MsgSent     uint64
	MsgRecv     uint64
	BytesSent   uint64
	BytesRecv   uint64
	MsgPerSec   float64
	BytesPerSec float64
	// Reliability
	Seq SeqStatsSnapshot
	// Resources
	Resources ResourceSnapshot
	// Connection lifecycle
	Handshake HistogramSnapshot
	Reconnect HistogramSnapshot
}

// Snapshot computes and returns the final measurement result.
// Call after Flush(lastSentSeq).
func (r *Recorder) Snapshot() RecorderSnapshot {
	elapsed := time.Since(r.startTime)
	elapsedSec := elapsed.Seconds()

	msgRecv := r.msgRecv.Load()
	bytesRecv := r.bytesRecv.Load()

	snap := RecorderSnapshot{
		Label:     r.label,
		Protocol:  r.protocol,
		Scenario:  r.scenario,
		Duration:  elapsed,
		Latency:   r.latency.Snapshot(),
		MsgSent:   r.msgSent.Load(),
		MsgRecv:   msgRecv,
		BytesSent: r.bytesSent.Load(),
		BytesRecv: bytesRecv,
		Seq:       r.seqTracker.Snapshot(),
		Resources: r.resources.Snapshot(),
		Handshake: r.handshake.Snapshot(),
		Reconnect: r.reconnect.Snapshot(),
	}

	if elapsedSec > 0 {
		snap.MsgPerSec = float64(msgRecv) / elapsedSec
		snap.BytesPerSec = float64(bytesRecv) / elapsedSec
	}

	return snap
}

// MsgSentCount returns the live message sent count without locking.
//
//go:nosplit
func (r *Recorder) MsgSentCount() uint64 { return r.msgSent.Load() }

// MsgRecvCount returns the live message received count without locking.
//
//go:nosplit
func (r *Recorder) MsgRecvCount() uint64 { return r.msgRecv.Load() }
