// Package metrics provides the measurement infrastructure for the benchmark platform.
//
// Architecture:
//   - HDRHistogram: accurate latency percentiles (used in the final report)
//   - SequenceTracker: per-sender sliding window for loss/reorder/dup detection
//   - ResourceSampler: background goroutine reading CPU/mem/goroutine/FD every 100ms
//   - PrometheusExporter: live metrics for Grafana dashboards
//   - Recorder: central sink that composes all of the above
//
// Why two histogram implementations (HDR + Prometheus)?
// HDR histograms provide exact percentiles with configurable precision (3 sig figs = 0.1% error).
// Prometheus histograms use predefined buckets and are less accurate at tail percentiles.
// We run both: HDR for the final benchmark report, Prometheus for real-time Grafana visibility.
package metrics

import (
	"sync"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
)

// HDRHistogram wraps hdrhistogram.Histogram with a mutex for concurrent use.
// Configured for nanosecond latency measurements across the full benchmark range.
type HDRHistogram struct {
	mu   sync.Mutex
	hist *hdrhistogram.Histogram
}

// HistogramSnapshot holds a point-in-time copy of all latency percentiles.
// All Duration fields are nanosecond-precision.
type HistogramSnapshot struct {
	Min    time.Duration
	Max    time.Duration
	Mean   time.Duration
	P50    time.Duration
	P95    time.Duration
	P99    time.Duration
	P999   time.Duration
	StdDev time.Duration
	Count  int64
}

// NewHDRHistogram creates a latency histogram covering 1ns to 30 seconds with
// 3 significant figures (0.1% accuracy at all percentiles).
// This range covers everything from loopback UDP (<1µs) to cross-continent HTTP (>100ms).
//
// Memory usage: ~1.8MB per histogram (fixed regardless of sample count).
// Compare to streaming P99: HDR uses more memory but provides exact percentiles.
func NewHDRHistogram() *HDRHistogram {
	return &HDRHistogram{
		hist: hdrhistogram.New(
			1,              // minimum value: 1 nanosecond
			30_000_000_000, // maximum value: 30 seconds in nanoseconds
			3,              // significant figures: 0.1% accuracy
		),
	}
}

// Record records a latency sample in nanoseconds.
// Negative values (clock skew on distributed setups) are clamped to 1.
// Lock-contended: caller should batch or use separate histograms per goroutine
// for extremely high-throughput scenarios (>10M samples/sec).
func (h *HDRHistogram) Record(latencyNs int64) {
	if latencyNs < 1 {
		latencyNs = 1
	}
	h.mu.Lock()
	// RecordValue returns an error only if the value is outside [min, max].
	// We clamp above, so this error is safe to ignore.
	_ = h.hist.RecordValue(latencyNs)
	h.mu.Unlock()
}

// RecordDuration is a convenience wrapper for time.Duration latency values.
func (h *HDRHistogram) RecordDuration(d time.Duration) {
	h.Record(d.Nanoseconds())
}

// RecordSince records the latency from a send timestamp to now.
// Equivalent to RecordDuration(time.Since(sendTime)).
//
//go:nosplit
func (h *HDRHistogram) RecordSince(sendNs int64) {
	h.Record(time.Now().UnixNano() - sendNs)
}

// Snapshot returns a point-in-time copy of all percentile values.
// Does NOT reset the histogram — use Reset() explicitly between phases.
func (h *HDRHistogram) Snapshot() HistogramSnapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.hist.TotalCount() == 0 {
		return HistogramSnapshot{}
	}
	return HistogramSnapshot{
		Min:    time.Duration(h.hist.Min()),
		Max:    time.Duration(h.hist.Max()),
		Mean:   time.Duration(int64(h.hist.Mean())),
		P50:    time.Duration(h.hist.ValueAtQuantile(50.0)),
		P95:    time.Duration(h.hist.ValueAtQuantile(95.0)),
		P99:    time.Duration(h.hist.ValueAtQuantile(99.0)),
		P999:   time.Duration(h.hist.ValueAtQuantile(99.9)),
		StdDev: time.Duration(int64(h.hist.StdDev())),
		Count:  h.hist.TotalCount(),
	}
}

// Reset clears all recorded values. Call between warmup and measurement phases.
func (h *HDRHistogram) Reset() {
	h.mu.Lock()
	h.hist.Reset()
	h.mu.Unlock()
}

// Count returns the total number of recorded samples.
func (h *HDRHistogram) Count() int64 {
	h.mu.Lock()
	n := h.hist.TotalCount()
	h.mu.Unlock()
	return n
}
