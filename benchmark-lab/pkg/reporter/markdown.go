package reporter

import (
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/himanshuplace/protocol_for_broadcast/pkg/collector"
)

// MarkdownReporter renders a comparison table sorted by P99 latency.
type MarkdownReporter struct {
	out io.Writer
}

// NewMarkdownReporter creates a reporter writing Markdown to w.
func NewMarkdownReporter(w io.Writer) *MarkdownReporter {
	if w == nil {
		w = os.Stdout
	}
	return &MarkdownReporter{out: w}
}

func (r *MarkdownReporter) Name() string { return "markdown" }

func (r *MarkdownReporter) Report(results []*collector.RunResult) error {
	if len(results) == 0 {
		fmt.Fprintln(r.out, "No results.")
		return nil
	}

	// Sort by P99 latency ascending
	sorted := make([]*collector.RunResult, len(results))
	copy(sorted, results)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].LatP99Ns < sorted[j].LatP99Ns
	})

	fmt.Fprintf(r.out, "# Protocol Benchmark Results\n\n")
	fmt.Fprintf(r.out, "Generated: %s\n\n", time.Now().UTC().Format(time.RFC3339))

	// Summary table
	fmt.Fprintf(r.out, "| Protocol | Scenario | Receivers | P50 Latency | P99 Latency | P99.9 Latency | msgs/sec | Loss%% | CPU%% | Memory |\n")
	fmt.Fprintf(r.out, "|----------|----------|-----------|-------------|-------------|----------------|----------|-------|-------|--------|\n")

	for _, res := range sorted {
		fmt.Fprintf(r.out, "| %-20s | %-8s | %9d | %11s | %11s | %14s | %8.0f | %5.2f | %5.1f | %6s |\n",
			res.Protocol,
			res.Scenario,
			res.ReceiverCount,
			fmtNs(res.LatP50Ns),
			fmtNs(res.LatP99Ns),
			fmtNs(res.LatP999Ns),
			res.MsgsPerSec,
			res.LossRatePct,
			res.CPUPctAvg,
			fmtBytes(res.MemBytesAvg),
		)
	}

	// Detail section
	fmt.Fprintf(r.out, "\n## Full Results\n\n")
	for _, res := range sorted {
		fmt.Fprintf(r.out, "### %s — Scenario %s\n\n", res.Protocol, res.Scenario)
		fmt.Fprintf(r.out, "- **Run ID**: `%s`\n", res.RunID)
		fmt.Fprintf(r.out, "- **Duration**: %ds (warmup %ds)\n", res.DurationS, res.WarmupS)
		fmt.Fprintf(r.out, "- **Receivers**: %d | **Msg Size**: %d bytes | **Generator**: %s\n",
			res.ReceiverCount, res.MsgSize, res.GeneratorType)
		fmt.Fprintf(r.out, "- **Network**: %s | **Broadcast Strategy**: %s\n", res.NetProfile, res.BroadcastStrat)
		fmt.Fprintf(r.out, "\n#### Latency\n\n")
		fmt.Fprintf(r.out, "| Min | P50 | P95 | P99 | P99.9 | Max |\n")
		fmt.Fprintf(r.out, "|-----|-----|-----|-----|-------|-----|\n")
		fmt.Fprintf(r.out, "| %s | %s | %s | %s | %s | %s |\n\n",
			fmtNs(res.LatMinNs), fmtNs(res.LatP50Ns), fmtNs(res.LatP95Ns),
			fmtNs(res.LatP99Ns), fmtNs(res.LatP999Ns), fmtNs(res.LatMaxNs))
		fmt.Fprintf(r.out, "#### Throughput & Reliability\n\n")
		fmt.Fprintf(r.out, "- **msgs/sec**: %.0f | **MB/sec**: %.2f\n", res.MsgsPerSec, res.BytesPerSec/1e6)
		fmt.Fprintf(r.out, "- **Sent**: %d | **Received**: %d | **Lost**: %d (%.3f%%)\n",
			res.TotalMsgsSent, res.TotalMsgsRecv, res.MsgsLost, res.LossRatePct)
		fmt.Fprintf(r.out, "\n#### Resources\n\n")
		fmt.Fprintf(r.out, "- **CPU avg**: %.1f%% | **Memory avg**: %s | **Goroutines avg**: %d\n\n",
			res.CPUPctAvg, fmtBytes(res.MemBytesAvg), res.GoroutinesAvg)
	}
	return nil
}

func fmtNs(ns int64) string {
	switch {
	case ns < 1000:
		return fmt.Sprintf("%dns", ns)
	case ns < 1_000_000:
		return fmt.Sprintf("%.1fµs", float64(ns)/1000)
	case ns < 1_000_000_000:
		return fmt.Sprintf("%.2fms", float64(ns)/1_000_000)
	default:
		return fmt.Sprintf("%.2fs", float64(ns)/1_000_000_000)
	}
}

func fmtBytes(b int64) string {
	switch {
	case b < 1024:
		return fmt.Sprintf("%dB", b)
	case b < 1024*1024:
		return fmt.Sprintf("%.1fKB", float64(b)/1024)
	case b < 1024*1024*1024:
		return fmt.Sprintf("%.1fMB", float64(b)/(1024*1024))
	default:
		return fmt.Sprintf("%.2fGB", float64(b)/(1024*1024*1024))
	}
}
