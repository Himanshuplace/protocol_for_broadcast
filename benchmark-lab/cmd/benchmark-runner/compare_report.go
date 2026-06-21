package main

import (
	"fmt"
	"html"
	"io"
	"strings"

	"github.com/himanshuplace/protocol_for_broadcast/pkg/collector"
)

// writeComparisonHTML renders a self-contained, dependency-free comparison report
// of several protocol runs: a side-by-side summary of every metric plus a
// per-client breakdown for each protocol. No CDN/JS — opens offline.
func writeComparisonHTML(w io.Writer, results []*collector.RunResult) error {
	var b strings.Builder

	b.WriteString(comparePrefix)

	// Header / context line from the first result.
	r0 := results[0]
	fmt.Fprintf(&b, `<header><h1>Protocol Comparison</h1><span class="ctx">scenario %s · %d B · %d receivers · %ds · %s</span></header>`,
		html.EscapeString(r0.Scenario), r0.MsgSize, r0.ReceiverCount, r0.DurationS, html.EscapeString(r0.OSArch))

	b.WriteString(`<main>`)

	// ── Summary table: metrics as rows, protocols as columns ──────────────────
	b.WriteString(`<h2>Summary</h2><div class="scroll"><table><thead><tr><th>Metric</th>`)
	for _, r := range results {
		fmt.Fprintf(&b, `<th>%s</th>`, html.EscapeString(r.Protocol))
	}
	b.WriteString(`</tr></thead><tbody>`)

	type row struct {
		label   string
		lowBest bool // for best-cell highlight; nil-direction rows pass want=false and skip
		mark    bool // whether to highlight a best cell
		vals    []float64
		render  func(v float64) string
	}
	num := func(extract func(*collector.RunResult) float64) []float64 {
		out := make([]float64, len(results))
		for i, r := range results {
			out[i] = extract(r)
		}
		return out
	}
	rows := []row{
		{"Throughput (msg/s)", false, true, num(func(r *collector.RunResult) float64 { return r.MsgsPerSec }), func(v float64) string { return fmtNum(v) }},
		{"Bandwidth", false, true, num(func(r *collector.RunResult) float64 { return r.BytesPerSec }), fmtBps},
		{"Latency p50", true, true, num(func(r *collector.RunResult) float64 { return float64(r.LatP50Ns) }), fmtNs},
		{"Latency p99", true, true, num(func(r *collector.RunResult) float64 { return float64(r.LatP99Ns) }), fmtNs},
		{"Latency p99.9", true, true, num(func(r *collector.RunResult) float64 { return float64(r.LatP999Ns) }), fmtNs},
		{"Latency max", true, false, num(func(r *collector.RunResult) float64 { return float64(r.LatMaxNs) }), fmtNs},
		{"Loss %", true, true, num(func(r *collector.RunResult) float64 { return r.LossRatePct }), func(v float64) string { return fmt.Sprintf("%.3f%%", v) }},
		{"Duplicated", true, false, num(func(r *collector.RunResult) float64 { return float64(r.MsgsDuplicated) }), func(v float64) string { return fmtNum(v) }},
		{"Reordered", true, false, num(func(r *collector.RunResult) float64 { return float64(r.MsgsReordered) }), func(v float64) string { return fmtNum(v) }},
		{"CPU %", true, true, num(func(r *collector.RunResult) float64 { return r.CPUPctAvg }), func(v float64) string { return fmt.Sprintf("%.0f", v) }},
		{"Memory", true, false, num(func(r *collector.RunResult) float64 { return float64(r.MemBytesAvg) }), fmtBytes},
		{"Goroutines", true, false, num(func(r *collector.RunResult) float64 { return float64(r.GoroutinesAvg) }), func(v float64) string { return fmt.Sprintf("%.0f", v) }},
		{"Open FDs/handles", true, false, num(func(r *collector.RunResult) float64 { return float64(r.FDCountAvg) }), func(v float64) string { return fmt.Sprintf("%.0f", v) }},
		{"Handshake p99", true, false, num(func(r *collector.RunResult) float64 { return float64(r.HandshakeP99Ns) }), fmtNs},
	}
	for _, rw := range rows {
		best := -1
		if rw.mark {
			best = bestIndex(rw.vals, rw.lowBest)
		}
		fmt.Fprintf(&b, `<tr><td class="metric">%s</td>`, rw.label)
		for i, v := range rw.vals {
			cls := ""
			if i == best {
				cls = ` class="best"`
			}
			fmt.Fprintf(&b, `<td%s>%s</td>`, cls, rw.render(v))
		}
		b.WriteString(`</tr>`)
	}
	b.WriteString(`</tbody></table></div>`)

	// ── Per-client breakdown per protocol ─────────────────────────────────────
	for _, r := range results {
		if len(r.PerClient) == 0 {
			continue
		}
		fmt.Fprintf(&b, `<h2>Per-client — %s <span class="sub">(%d clients)</span></h2><div class="scroll"><table><thead><tr>`,
			html.EscapeString(r.Protocol), len(r.PerClient))
		for _, h := range []string{"Client", "Received", "Lost", "Loss %", "Dup", "Reorder", "First seq", "Last seq", "Lat p50", "Lat p99", "Lat max"} {
			fmt.Fprintf(&b, `<th>%s</th>`, h)
		}
		b.WriteString(`</tr></thead><tbody>`)
		for _, c := range r.PerClient {
			lossCls := ""
			if c.Lost > 0 {
				lossCls = ` class="warn"`
			}
			fmt.Fprintf(&b, `<tr><td class="metric">%s</td><td>%s</td><td%s>%s</td><td%s>%.3f%%</td><td>%s</td><td>%s</td><td>%d</td><td>%d</td><td>%s</td><td>%s</td><td>%s</td></tr>`,
				html.EscapeString(shortID(c.ClientID)),
				fmtNum(float64(c.MsgRecv)),
				lossCls, fmtNum(float64(c.Lost)),
				lossCls, c.LossRatePct,
				fmtNum(float64(c.Duplicated)),
				fmtNum(float64(c.Reordered)),
				c.FirstSeq, c.LastSeq,
				fmtNs(float64(c.LatP50Ns)),
				fmtNs(float64(c.LatP99Ns)),
				fmtNs(float64(c.LatMaxNs)),
			)
		}
		b.WriteString(`</tbody></table></div>`)
	}

	b.WriteString(`<p class="note">Loss is measured relative to the fastest client: the highest sequence number any client received is treated as the last broadcast, and each client's loss counts what it missed from its first received packet onward. Per-packet timing is in the <code>trace-&lt;protocol&gt;.csv</code> files when run with <code>--trace</code>.</p>`)
	b.WriteString(`</main></body></html>`)

	_, err := io.WriteString(w, b.String())
	return err
}

// bestIndex returns the index of the best value (lowest if lowBest, else highest).
func bestIndex(vals []float64, lowBest bool) int {
	best := 0
	for i := 1; i < len(vals); i++ {
		if (lowBest && vals[i] < vals[best]) || (!lowBest && vals[i] > vals[best]) {
			best = i
		}
	}
	return best
}

func shortID(id string) string {
	if len(id) > 18 {
		return id[:18] + "…"
	}
	return id
}

func fmtNum(v float64) string {
	switch {
	case v >= 1e9:
		return fmt.Sprintf("%.2fB", v/1e9)
	case v >= 1e6:
		return fmt.Sprintf("%.2fM", v/1e6)
	case v >= 1e3:
		return fmt.Sprintf("%.1fK", v/1e3)
	default:
		return fmt.Sprintf("%.0f", v)
	}
}

func fmtBps(v float64) string {
	switch {
	case v >= 1e9:
		return fmt.Sprintf("%.2f GB/s", v/1e9)
	case v >= 1e6:
		return fmt.Sprintf("%.2f MB/s", v/1e6)
	case v >= 1e3:
		return fmt.Sprintf("%.1f KB/s", v/1e3)
	default:
		return fmt.Sprintf("%.0f B/s", v)
	}
}

func fmtBytes(v float64) string {
	switch {
	case v >= 1e9:
		return fmt.Sprintf("%.2f GB", v/1e9)
	case v >= 1e6:
		return fmt.Sprintf("%.1f MB", v/1e6)
	case v >= 1e3:
		return fmt.Sprintf("%.0f KB", v/1e3)
	default:
		return fmt.Sprintf("%.0f B", v)
	}
}

// fmtNs renders nanoseconds as µs (under 1ms) or ms.
func fmtNs(ns float64) string {
	if ns <= 0 {
		return "0"
	}
	if ns < 1e6 {
		return fmt.Sprintf("%.0f µs", ns/1e3)
	}
	return fmt.Sprintf("%.2f ms", ns/1e6)
}

const comparePrefix = `<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Protocol Comparison</title><style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:#0d1117;color:#e6edf3;font-family:'Segoe UI',system-ui,sans-serif;font-size:14px}
header{background:#161b22;border-bottom:1px solid #30363d;padding:14px 24px;display:flex;align-items:baseline;gap:14px}
header h1{font-size:17px}
.ctx{color:#8b949e;font-size:13px;font-family:monospace}
main{padding:20px 24px;max-width:1500px}
h2{font-size:15px;margin:26px 0 10px;color:#58a6ff}
h2 .sub,.metric .sub{color:#8b949e;font-weight:400;font-size:12px}
.scroll{overflow-x:auto;border:1px solid #30363d;border-radius:8px}
table{width:100%;border-collapse:collapse;background:#161b22}
th,td{text-align:right;padding:8px 14px;border-bottom:1px solid #21262d;font-family:monospace;white-space:nowrap}
th{color:#8b949e;font-weight:500;font-size:11px;text-transform:uppercase;text-align:right;background:#161b22;position:sticky;top:0}
th:first-child,td.metric{text-align:left}
td.metric{color:#e6edf3;font-family:'Segoe UI',system-ui,sans-serif}
tr:last-child td{border-bottom:none}
tr:hover td{background:rgba(255,255,255,.03)}
td.best{color:#3fb950;font-weight:700}
td.warn{color:#f85149;font-weight:700}
.note{margin-top:22px;color:#8b949e;font-size:12px;line-height:1.5}
code{background:#21262d;padding:1px 5px;border-radius:4px;font-size:12px}
</style></head><body>`
