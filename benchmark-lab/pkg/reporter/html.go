package reporter

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/himanshuplace/protocol_for_broadcast/pkg/collector"
)

// HTMLReporter generates a self-contained HTML report with Chart.js visualizations.
type HTMLReporter struct {
	out io.Writer
}

// NewHTMLReporter creates a reporter writing HTML to w.
func NewHTMLReporter(w io.Writer) *HTMLReporter {
	if w == nil {
		w = os.Stdout
	}
	return &HTMLReporter{out: w}
}

func (r *HTMLReporter) Name() string { return "html" }

func (r *HTMLReporter) Report(results []*collector.RunResult) error {
	sorted := make([]*collector.RunResult, len(results))
	copy(sorted, results)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].LatP99Ns < sorted[j].LatP99Ns
	})

	labels := make([]string, len(sorted))
	p50Data := make([]float64, len(sorted))
	p99Data := make([]float64, len(sorted))
	throughput := make([]float64, len(sorted))
	lossData := make([]float64, len(sorted))
	cpuData := make([]float64, len(sorted))

	for i, r := range sorted {
		labels[i] = r.Protocol
		p50Data[i] = float64(r.LatP50Ns) / 1000.0 // µs
		p99Data[i] = float64(r.LatP99Ns) / 1000.0
		throughput[i] = r.MsgsPerSec
		lossData[i] = r.LossRatePct
		cpuData[i] = r.CPUPctAvg
	}

	labelsJSON, _ := json.Marshal(labels)
	p50JSON, _ := json.Marshal(p50Data)
	p99JSON, _ := json.Marshal(p99Data)
	tpJSON, _ := json.Marshal(throughput)
	lossJSON, _ := json.Marshal(lossData)
	cpuJSON, _ := json.Marshal(cpuData)

	fmt.Fprintf(r.out, `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Protocol Benchmark Results</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }
  h1, h2 { color: #333; }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
  .card { background: #fff; border-radius: 8px; padding: 20px; box-shadow: 0 2px 4px rgba(0,0,0,.1); }
  table { width: 100%%; border-collapse: collapse; font-size: 13px; }
  th { background: #2563eb; color: #fff; padding: 8px 12px; text-align: left; }
  td { padding: 6px 12px; border-bottom: 1px solid #eee; }
  tr:nth-child(even) td { background: #f9f9f9; }
  .best td { background: #d1fae5 !important; }
  .worst td { background: #fee2e2 !important; }
  .ts { color: #888; font-size: 12px; margin-bottom: 20px; }
</style>
</head>
<body>
<h1>Protocol Benchmark Comparison</h1>
<p class="ts">Generated: %s | %d runs</p>

<div class="grid">
  <div class="card"><h2>Latency (µs) — Lower is Better</h2>
    <canvas id="latChart"></canvas></div>
  <div class="card"><h2>Throughput (msgs/sec) — Higher is Better</h2>
    <canvas id="tpChart"></canvas></div>
  <div class="card"><h2>Packet Loss (%%) — Lower is Better</h2>
    <canvas id="lossChart"></canvas></div>
  <div class="card"><h2>CPU Usage (%%) — Lower is Better</h2>
    <canvas id="cpuChart"></canvas></div>
</div>

<div class="card" style="margin-top:20px">
<h2>Summary Table</h2>
<table>
<thead><tr>
  <th>Protocol</th><th>Scenario</th><th>P50 (µs)</th><th>P99 (µs)</th><th>P99.9 (µs)</th>
  <th>msgs/sec</th><th>Loss%%</th><th>CPU%%</th><th>Memory</th><th>Goroutines</th>
</tr></thead>
<tbody>
`, time.Now().UTC().Format(time.RFC3339), len(sorted))

	for i, res := range sorted {
		rowClass := ""
		if i == 0 {
			rowClass = ` class="best"`
		} else if i == len(sorted)-1 {
			rowClass = ` class="worst"`
		}
		fmt.Fprintf(r.out, `<tr%s>
  <td>%s</td><td>%s</td>
  <td>%.1f</td><td>%.1f</td><td>%.1f</td>
  <td>%.0f</td><td>%.3f</td><td>%.1f</td>
  <td>%s</td><td>%d</td>
</tr>`,
			rowClass,
			res.Protocol, res.Scenario,
			float64(res.LatP50Ns)/1000, float64(res.LatP99Ns)/1000, float64(res.LatP999Ns)/1000,
			res.MsgsPerSec, res.LossRatePct, res.CPUPctAvg,
			fmtBytes(res.MemBytesAvg), res.GoroutinesAvg,
		)
	}

	fmt.Fprintf(r.out, `</tbody></table></div>

<script>
const labels = %s;
const colors = labels.map((_,i) => `+"`"+`hsl(${i * 360 / labels.length}, 70%%, 50%%)`+"`"+`);

new Chart(document.getElementById('latChart'), {
  type: 'bar',
  data: {
    labels,
    datasets: [
      { label: 'P50', data: %s, backgroundColor: colors.map(c => c+'99') },
      { label: 'P99', data: %s, backgroundColor: colors },
    ]
  },
  options: { plugins: { legend: { position: 'top' } }, scales: { y: { beginAtZero: true } } }
});

new Chart(document.getElementById('tpChart'), {
  type: 'bar',
  data: { labels, datasets: [{ label: 'msgs/sec', data: %s, backgroundColor: colors }] },
  options: { plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true } } }
});

new Chart(document.getElementById('lossChart'), {
  type: 'bar',
  data: { labels, datasets: [{ label: 'loss%%', data: %s, backgroundColor: colors }] },
  options: { plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true } } }
});

new Chart(document.getElementById('cpuChart'), {
  type: 'bar',
  data: { labels, datasets: [{ label: 'CPU%%', data: %s, backgroundColor: colors }] },
  options: { plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true } } }
});
</script>
</body></html>
`, labelsJSON, p50JSON, p99JSON, tpJSON, lossJSON, cpuJSON)

	return nil
}
