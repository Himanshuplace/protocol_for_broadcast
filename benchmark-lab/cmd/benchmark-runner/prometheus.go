package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/himanshuplace/protocol_for_broadcast/internal/scenarios"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/metrics"
)

// promExporter publishes the runner's live metrics in Prometheus text format
// at /metrics. It reads the recorder Snapshot once per second on a background
// goroutine — OFF the message hot path — so enabling export adds no per-message
// overhead and does not distort the latency it measures. That keeps the
// comparison fair: every protocol pays the same (zero) measurement tax.
//
// The authoritative latency numbers still come from the HDR histogram in the
// final JSON report; these gauges drive the live Grafana view.
type promExporter struct {
	reg  *prometheus.Registry
	srv  *http.Server
	rec  *metrics.Recorder
	cfg  scenarios.ScenarioConfig
	done chan struct{}

	msgsPerSec  *prometheus.GaugeVec
	bytesPerSec *prometheus.GaugeVec
	latencyUs   *prometheus.GaugeVec // label: quantile (min/avg/p50/p95/p99/p999/max)
	msgsSent    *prometheus.GaugeVec
	msgsRecv    *prometheus.GaugeVec
	lost        *prometheus.GaugeVec
	duplicated  *prometheus.GaugeVec
	reordered   *prometheus.GaugeVec
	cpuPct      *prometheus.GaugeVec
	memBytes    *prometheus.GaugeVec
	goroutines  *prometheus.GaugeVec
	fdCount     *prometheus.GaugeVec
}

// startPrometheus builds the exporter, serves /metrics on the given port, and
// starts the 1 Hz sampling loop. Call before runner.Run().
func startPrometheus(port int, cfg scenarios.ScenarioConfig, rec *metrics.Recorder) *promExporter {
	reg := prometheus.NewRegistry()
	base := []string{metrics.LabelProtocol, metrics.LabelScenario}

	gauge := func(name, help string, labels []string) *prometheus.GaugeVec {
		g := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help}, labels)
		reg.MustRegister(g)
		return g
	}

	e := &promExporter{
		reg:  reg,
		rec:  rec,
		cfg:  cfg,
		done: make(chan struct{}),

		msgsPerSec:  gauge("benchmark_live_msgs_per_sec", "Live messages received per second.", base),
		bytesPerSec: gauge("benchmark_live_bytes_per_sec", "Live bytes received per second.", base),
		latencyUs:   gauge("benchmark_live_latency_microseconds", "Live one-way latency by quantile, in microseconds.", append(append([]string{}, base...), "quantile")),
		msgsSent:    gauge("benchmark_live_msgs_sent_total", "Cumulative messages sent.", base),
		msgsRecv:    gauge("benchmark_live_msgs_recv_total", "Cumulative messages received.", base),
		lost:        gauge("benchmark_live_msgs_lost_total", "Cumulative messages declared lost.", base),
		duplicated:  gauge("benchmark_live_msgs_duplicated_total", "Cumulative duplicate messages.", base),
		reordered:   gauge("benchmark_live_msgs_reordered_total", "Cumulative reordered messages.", base),
		cpuPct:      gauge("benchmark_live_cpu_percent", "Process CPU utilization percent (100 = one core).", base),
		memBytes:    gauge("benchmark_live_mem_bytes", "Process memory obtained from OS, in bytes.", base),
		goroutines:  gauge("benchmark_live_goroutines", "Live goroutine count.", base),
		fdCount:     gauge("benchmark_live_fd_count", "Open file descriptors / handles (-1 if unavailable).", base),
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	e.srv = &http.Server{Addr: fmt.Sprintf(":%d", port), Handler: mux}

	go func() { _ = e.srv.ListenAndServe() }()
	go e.loop()

	return e
}

func (e *promExporter) loop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	p, s := e.cfg.Protocol, e.cfg.Scenario
	for {
		select {
		case <-e.done:
			return
		case <-ticker.C:
			snap := e.rec.Snapshot()
			e.msgsPerSec.WithLabelValues(p, s).Set(snap.MsgPerSec)
			e.bytesPerSec.WithLabelValues(p, s).Set(snap.BytesPerSec)
			e.msgsSent.WithLabelValues(p, s).Set(float64(snap.MsgSent))
			e.msgsRecv.WithLabelValues(p, s).Set(float64(snap.MsgRecv))
			e.lost.WithLabelValues(p, s).Set(float64(snap.Seq.Lost))
			e.duplicated.WithLabelValues(p, s).Set(float64(snap.Seq.Duplicated))
			e.reordered.WithLabelValues(p, s).Set(float64(snap.Seq.Reordered))

			e.latencyUs.WithLabelValues(p, s, "min").Set(float64(snap.Latency.Min.Microseconds()))
			e.latencyUs.WithLabelValues(p, s, "avg").Set(float64(snap.Latency.Mean.Microseconds()))
			e.latencyUs.WithLabelValues(p, s, "p50").Set(float64(snap.Latency.P50.Microseconds()))
			e.latencyUs.WithLabelValues(p, s, "p95").Set(float64(snap.Latency.P95.Microseconds()))
			e.latencyUs.WithLabelValues(p, s, "p99").Set(float64(snap.Latency.P99.Microseconds()))
			e.latencyUs.WithLabelValues(p, s, "p999").Set(float64(snap.Latency.P999.Microseconds()))
			e.latencyUs.WithLabelValues(p, s, "max").Set(float64(snap.Latency.Max.Microseconds()))

			e.cpuPct.WithLabelValues(p, s).Set(snap.Resources.CPUAvg)
			e.memBytes.WithLabelValues(p, s).Set(float64(snap.Resources.MemAvg))
			e.goroutines.WithLabelValues(p, s).Set(float64(snap.Resources.GoroutineAvg))
			e.fdCount.WithLabelValues(p, s).Set(float64(snap.Resources.FDAvg))
		}
	}
}

func (e *promExporter) stop() {
	close(e.done)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = e.srv.Shutdown(ctx)
}
