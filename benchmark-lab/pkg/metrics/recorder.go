package metrics

import (
	"sync"
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

	// Sequence-based reliability tracking (single-stream: standalone clients)
	seqTracker *SequenceTracker

	// Per-connection stats. When one Recorder aggregates many receivers (a
	// broadcast benchmark with N subscribers), each connection is tracked
	// independently so fan-out copies of the same seq are not counted as
	// duplicates, throughput is per subscriber, and per-client latency/loss can
	// be reported. Empty for single-stream recorders.
	connMu    sync.Mutex
	connStats map[string]*connStat

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
	Label              string
	Protocol           string
	Scenario           string
	SequenceWindowSize uint64 // default 4096
	SampleInterval     time.Duration
	Prometheus         *BenchmarkMetrics // optional; nil = no Prometheus export
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
		connStats:  make(map[string]*connStat),
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
	r.recordSample(sendNs, size, recvNs)
	r.seqTracker.Observe(seq)
}

// RecordRecvFrom is like RecordRecv but attributes the sequence number to a
// specific connection. Use this when one Recorder aggregates many receivers
// (a broadcast benchmark with N subscribers): each connection gets its own
// SequenceTracker, so fan-out copies of the same seq across subscribers are not
// miscounted as duplicates, and throughput is reported per subscriber.
func (r *Recorder) RecordRecvFrom(connID string, seq uint64, sendNs int64, size int, recvNs int64) {
	r.recordSample(sendNs, size, recvNs)
	r.connMu.Lock()
	cs := r.connStats[connID]
	if cs == nil {
		cs = newConnStat()
		r.connStats[connID] = cs
	}
	r.connMu.Unlock()
	cs.observe(seq, size, recvNs-sendNs)
}

// connStat holds per-connection measurement state for one subscriber.
type connStat struct {
	tracker *SequenceTracker
	latency *HDRHistogram

	mu        sync.Mutex
	msgRecv   uint64
	bytesRecv uint64
	firstSeq  uint64 // first sequence number this connection observed (0 = none)
	lastSeq   uint64 // highest sequence number this connection observed
}

func newConnStat() *connStat {
	return &connStat{tracker: NewSequenceTracker(4096), latency: NewHDRHistogram()}
}

func (c *connStat) observe(seq uint64, size int, latNs int64) {
	c.tracker.Observe(seq)
	c.latency.Record(latNs)
	c.mu.Lock()
	c.msgRecv++
	c.bytesRecv += uint64(size)
	if c.firstSeq == 0 || seq < c.firstSeq {
		c.firstSeq = seq
	}
	if seq > c.lastSeq {
		c.lastSeq = seq
	}
	c.mu.Unlock()
}

// ConnSnapshot is a per-connection measurement result for one subscriber.
type ConnSnapshot struct {
	ConnID     string
	MsgRecv    uint64
	Delivered  uint64
	Duplicated uint64
	Reordered  uint64
	FirstSeq   uint64
	LastSeq    uint64
	Latency    HistogramSnapshot
}

// ConnSnapshots returns one snapshot per connection that has delivered at least
// one message. Used to build per-client comparison reports. Loss is computed by
// the caller, which knows the highest sequence number broadcast across all
// connections.
func (r *Recorder) ConnSnapshots() []ConnSnapshot {
	r.connMu.Lock()
	defer r.connMu.Unlock()
	out := make([]ConnSnapshot, 0, len(r.connStats))
	for id, cs := range r.connStats {
		seq := cs.tracker.Snapshot()
		cs.mu.Lock()
		out = append(out, ConnSnapshot{
			ConnID:     id,
			MsgRecv:    cs.msgRecv,
			Delivered:  seq.Delivered,
			Duplicated: seq.Duplicated,
			Reordered:  seq.Reordered,
			FirstSeq:   cs.firstSeq,
			LastSeq:    cs.lastSeq,
			Latency:    cs.latency.Snapshot(),
		})
		cs.mu.Unlock()
	}
	return out
}

// recordSample records the latency, byte, and message counters shared by both
// RecordRecv and RecordRecvFrom. It does NOT do sequence tracking.
func (r *Recorder) recordSample(sendNs int64, size int, recvNs int64) {
	r.msgRecv.Add(1)
	r.bytesRecv.Add(uint64(size))

	// Record latency: recvNs - sendNs (one-way, requires clock sync for distributed)
	latNs := recvNs - sendNs
	r.latency.Record(latNs)

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
	r.connMu.Lock()
	r.connStats = make(map[string]*connStat)
	r.connMu.Unlock()
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
	seqSnap := r.seqTracker.Snapshot()

	// When this recorder aggregates multiple connections (broadcast benchmark),
	// reliability is summed across per-connection trackers — so fan-out copies of
	// the same seq are not counted as duplicates — and the raw N× receive counts
	// are normalized to per-subscriber throughput.
	r.connMu.Lock()
	nConn := len(r.connStats)
	if nConn > 0 {
		var agg SeqStatsSnapshot
		for _, cs := range r.connStats {
			s := cs.tracker.Snapshot()
			agg.Delivered += s.Delivered
			agg.Lost += s.Lost
			agg.Duplicated += s.Duplicated
			agg.Reordered += s.Reordered
		}
		seqSnap = agg
	}
	r.connMu.Unlock()

	if nConn > 0 {
		msgRecv /= uint64(nConn)
		bytesRecv /= uint64(nConn)
	}

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
		Seq:       seqSnap,
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
