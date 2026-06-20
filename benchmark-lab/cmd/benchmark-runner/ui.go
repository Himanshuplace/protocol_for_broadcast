package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/himanshuplace/protocol_for_broadcast/internal/scenarios"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/metrics"
)

// liveUpdate is one SSE frame sent to the browser every 500ms.
type liveUpdate struct {
	ElapsedS    float64 `json:"elapsed_s"`
	TotalS      float64 `json:"total_s"`
	Phase       string  `json:"phase"`
	Protocol    string  `json:"protocol"`
	Scenario    string  `json:"scenario"`
	MsgsSent    uint64  `json:"msgs_sent"`
	MsgsRecv    uint64  `json:"msgs_recv"`
	MsgsPerSec  float64 `json:"msgs_per_sec"`
	BytesPerSec float64 `json:"bytes_per_sec"`
	LatP50Us    int64   `json:"lat_p50_us"`
	LatP99Us    int64   `json:"lat_p99_us"`
	LatP999Us   int64   `json:"lat_p999_us"`
	LatMinUs    int64   `json:"lat_min_us"`
	LatAvgUs    int64   `json:"lat_avg_us"`
	LatMaxUs    int64   `json:"lat_max_us"`
	Duplicated  uint64  `json:"duplicated"`
	Reordered   uint64  `json:"reordered"`
}

type uiServer struct {
	cfg       scenarios.ScenarioConfig
	rec       *metrics.Recorder
	startTime time.Time
	mu        sync.Mutex
	clients   map[chan string]struct{}
}

// startUI starts the HTTP dashboard server and begins sampling the recorder.
// Call before runner.Run() so the browser can connect early.
func startUI(port int, cfg scenarios.ScenarioConfig, rec *metrics.Recorder) *http.Server {
	u := &uiServer{
		cfg:       cfg,
		rec:       rec,
		startTime: time.Now(),
		clients:   make(map[chan string]struct{}),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/events", u.sseHandler)
	mux.HandleFunc("/", u.dashboardHandler)

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	go u.sample()
	go func() { _ = srv.ListenAndServe() }()

	return srv
}

func (u *uiServer) sample() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	warmupS := u.cfg.WarmupDuration.Seconds()
	totalS := (u.cfg.WarmupDuration + u.cfg.Duration).Seconds()

	for range ticker.C {
		elapsed := time.Since(u.startTime).Seconds()
		snap := u.rec.Snapshot()

		var phase string
		switch {
		case elapsed < warmupS+0.1:
			phase = "warmup"
		case elapsed < totalS+0.2:
			phase = "measuring"
		default:
			phase = "done"
		}

		upd := liveUpdate{
			ElapsedS:    elapsed,
			TotalS:      totalS,
			Phase:       phase,
			Protocol:    u.cfg.Protocol,
			Scenario:    u.cfg.Scenario,
			MsgsSent:    snap.MsgSent,
			MsgsRecv:    snap.MsgRecv,
			MsgsPerSec:  snap.MsgPerSec,
			BytesPerSec: snap.BytesPerSec,
			LatP50Us:    snap.Latency.P50.Microseconds(),
			LatP99Us:    snap.Latency.P99.Microseconds(),
			LatP999Us:   snap.Latency.P999.Microseconds(),
			LatMinUs:    snap.Latency.Min.Microseconds(),
			LatAvgUs:    snap.Latency.Mean.Microseconds(),
			LatMaxUs:    snap.Latency.Max.Microseconds(),
			Duplicated:  snap.Seq.Duplicated,
			Reordered:   snap.Seq.Reordered,
		}

		data, _ := json.Marshal(upd)
		u.broadcast("event: update\ndata: " + string(data) + "\n\n")
	}
}

func (u *uiServer) broadcast(msg string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	for ch := range u.clients {
		select {
		case ch <- msg:
		default:
		}
	}
}

func (u *uiServer) sseHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	ch := make(chan string, 32)
	u.mu.Lock()
	u.clients[ch] = struct{}{}
	u.mu.Unlock()

	defer func() {
		u.mu.Lock()
		delete(u.clients, ch)
		u.mu.Unlock()
	}()

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	for {
		select {
		case msg := <-ch:
			fmt.Fprint(w, msg)
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

func (u *uiServer) dashboardHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, dashboardHTML)
}

// shutdownUI gracefully stops the dashboard HTTP server.
func shutdownUI(srv *http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

const dashboardHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Protocol Benchmark — Live Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  :root {
    --bg: #0d1117;
    --surface: #161b22;
    --border: #30363d;
    --text: #e6edf3;
    --muted: #8b949e;
    --green: #3fb950;
    --blue: #58a6ff;
    --orange: #d29922;
    --red: #f85149;
    --purple: #bc8cff;
    --cyan: #39d353;
  }
  body { background: var(--bg); color: var(--text); font-family: 'Segoe UI', system-ui, sans-serif; font-size: 14px; }

  header {
    background: var(--surface);
    border-bottom: 1px solid var(--border);
    padding: 14px 24px;
    display: flex;
    align-items: center;
    gap: 16px;
  }
  header h1 { font-size: 16px; font-weight: 600; color: var(--text); }
  .badge {
    display: inline-block;
    padding: 2px 10px;
    border-radius: 12px;
    font-size: 12px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }
  .badge-warmup   { background: rgba(210,153,34,0.2); color: var(--orange); border: 1px solid rgba(210,153,34,0.4); }
  .badge-measuring{ background: rgba(63,185,80,0.2);  color: var(--green);  border: 1px solid rgba(63,185,80,0.4); }
  .badge-done     { background: rgba(88,166,255,0.2); color: var(--blue);   border: 1px solid rgba(88,166,255,0.4); }
  .badge-waiting  { background: rgba(139,148,158,0.2);color: var(--muted);  border: 1px solid rgba(139,148,158,0.4); }

  .protocol-tag {
    background: rgba(88,166,255,0.15);
    color: var(--blue);
    border: 1px solid rgba(88,166,255,0.3);
    border-radius: 6px;
    padding: 2px 10px;
    font-size: 13px;
    font-weight: 600;
    font-family: monospace;
  }

  .progress-bar-wrap {
    flex: 1;
    background: var(--border);
    border-radius: 4px;
    height: 8px;
    overflow: hidden;
  }
  .progress-bar-fill {
    height: 100%;
    background: linear-gradient(90deg, var(--blue), var(--green));
    border-radius: 4px;
    transition: width 0.4s ease;
  }
  .timer { font-size: 13px; color: var(--muted); white-space: nowrap; font-family: monospace; }

  main { padding: 20px 24px; max-width: 1400px; }

  /* Stat cards */
  .cards {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 12px;
    margin-bottom: 20px;
  }
  .card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 16px;
  }
  .card-label { font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 6px; }
  .card-value { font-size: 26px; font-weight: 700; color: var(--text); font-family: monospace; line-height: 1; }
  .card-unit  { font-size: 12px; color: var(--muted); margin-top: 4px; }
  .card-green .card-value { color: var(--green); }
  .card-blue  .card-value { color: var(--blue); }
  .card-orange .card-value { color: var(--orange); }
  .card-purple .card-value { color: var(--purple); }

  /* Charts */
  .charts {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 16px;
    margin-bottom: 20px;
  }
  @media (max-width: 800px) { .charts { grid-template-columns: 1fr; } }
  .chart-box {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 16px;
  }
  .chart-title { font-size: 13px; font-weight: 600; color: var(--muted); margin-bottom: 12px; }
  .chart-canvas { width: 100% !important; height: 200px !important; }

  /* Raw table */
  .raw-table-box {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 16px;
  }
  .raw-table-title { font-size: 13px; font-weight: 600; color: var(--muted); margin-bottom: 12px; }
  table { width: 100%; border-collapse: collapse; }
  th, td { text-align: left; padding: 6px 12px; border-bottom: 1px solid var(--border); font-size: 13px; }
  th { color: var(--muted); font-weight: 500; font-size: 11px; text-transform: uppercase; }
  td { font-family: monospace; }
  tr:last-child td { border-bottom: none; }
  tr:hover td { background: rgba(255,255,255,0.03); }

  .connecting { text-align: center; padding: 40px; color: var(--muted); font-size: 14px; }
  .dot { display: inline-block; animation: blink 1s infinite; } .dot:nth-child(2) { animation-delay: 0.2s; } .dot:nth-child(3) { animation-delay: 0.4s; }
  @keyframes blink { 0%,80%,100% { opacity:0; } 40% { opacity:1; } }
</style>
</head>
<body>

<header>
  <h1>Protocol Benchmark</h1>
  <span class="protocol-tag" id="hdr-protocol">—</span>
  <span class="badge badge-waiting" id="hdr-phase">Waiting</span>
  <div class="progress-bar-wrap">
    <div class="progress-bar-fill" id="progress-fill" style="width:0%"></div>
  </div>
  <span class="timer" id="hdr-timer">0s / 0s</span>
</header>

<main>
  <div id="connecting" class="connecting">
    Connecting to benchmark runner<span class="dot">.</span><span class="dot">.</span><span class="dot">.</span>
  </div>

  <div id="dashboard" style="display:none">
    <div class="cards">
      <div class="card card-green">
        <div class="card-label">Throughput</div>
        <div class="card-value" id="c-msgs">0</div>
        <div class="card-unit">msg / sec</div>
      </div>
      <div class="card card-blue">
        <div class="card-label">Bandwidth</div>
        <div class="card-value" id="c-bw">0</div>
        <div class="card-unit" id="c-bw-unit">MB / s</div>
      </div>
      <div class="card card-orange">
        <div class="card-label">P50 Latency</div>
        <div class="card-value" id="c-p50">0</div>
        <div class="card-unit">µs</div>
      </div>
      <div class="card card-purple">
        <div class="card-label">P99 Latency</div>
        <div class="card-value" id="c-p99">0</div>
        <div class="card-unit">µs</div>
      </div>
      <div class="card">
        <div class="card-label">Total Sent</div>
        <div class="card-value" id="c-sent">0</div>
        <div class="card-unit">messages</div>
      </div>
      <div class="card">
        <div class="card-label">Total Received</div>
        <div class="card-value" id="c-recv">0</div>
        <div class="card-unit">messages</div>
      </div>
    </div>

    <div class="charts">
      <div class="chart-box">
        <div class="chart-title">THROUGHPUT — msg/sec over time</div>
        <canvas id="chart-throughput" class="chart-canvas"></canvas>
      </div>
      <div class="chart-box">
        <div class="chart-title">P99 LATENCY — µs over time</div>
        <canvas id="chart-latency" class="chart-canvas"></canvas>
      </div>
    </div>

    <div class="raw-table-box">
      <div class="raw-table-title">RAW METRICS</div>
      <table>
        <thead>
          <tr>
            <th>Metric</th><th>Value</th>
            <th>Metric</th><th>Value</th>
          </tr>
        </thead>
        <tbody id="raw-table-body">
        </tbody>
      </table>
    </div>
  </div>
</main>

<script>
const MAX_POINTS = 60;

let throughputData = [];
let latencyData    = [];
let labels         = [];

const chartDefaults = {
  type: 'line',
  options: {
    animation: false,
    responsive: true,
    maintainAspectRatio: false,
    plugins: { legend: { display: false } },
    scales: {
      x: { display: false },
      y: {
        grid: { color: 'rgba(255,255,255,0.05)' },
        ticks: { color: '#8b949e', font: { size: 11 } },
        border: { color: '#30363d' }
      }
    },
    elements: { point: { radius: 0 }, line: { tension: 0.3, borderWidth: 2 } }
  }
};

const tChart = new Chart(document.getElementById('chart-throughput'), {
  ...chartDefaults,
  data: {
    labels: [],
    datasets: [{
      data: [],
      borderColor: '#3fb950',
      backgroundColor: 'rgba(63,185,80,0.1)',
      fill: true
    }]
  }
});

const lChart = new Chart(document.getElementById('chart-latency'), {
  ...chartDefaults,
  data: {
    labels: [],
    datasets: [{
      data: [],
      borderColor: '#bc8cff',
      backgroundColor: 'rgba(188,140,255,0.1)',
      fill: true
    }]
  }
});

function push(chart, label, value) {
  chart.data.labels.push(label);
  chart.data.datasets[0].data.push(value);
  if (chart.data.labels.length > MAX_POINTS) {
    chart.data.labels.shift();
    chart.data.datasets[0].data.shift();
  }
  chart.update('none');
}

function fmtNum(n) {
  if (n >= 1e9) return (n/1e9).toFixed(2) + 'B';
  if (n >= 1e6) return (n/1e6).toFixed(2) + 'M';
  if (n >= 1e3) return (n/1e3).toFixed(1) + 'K';
  return String(Math.round(n));
}

function fmtBytes(b) {
  if (b >= 1e9) return { v: (b/1e9).toFixed(2), u: 'GB/s' };
  if (b >= 1e6) return { v: (b/1e6).toFixed(2), u: 'MB/s' };
  if (b >= 1e3) return { v: (b/1e3).toFixed(1), u: 'KB/s' };
  return { v: String(Math.round(b)), u: 'B/s' };
}

function fmtTime(s) {
  const m = Math.floor(s/60);
  const ss = Math.floor(s%60);
  return m > 0 ? m + 'm ' + ss + 's' : ss + 's';
}

function onUpdate(d) {
  document.getElementById('connecting').style.display = 'none';
  document.getElementById('dashboard').style.display  = 'block';

  // Header
  document.getElementById('hdr-protocol').textContent = d.protocol + ' / Scenario ' + d.scenario;
  const phaseEl = document.getElementById('hdr-phase');
  phaseEl.textContent = d.phase.charAt(0).toUpperCase() + d.phase.slice(1);
  phaseEl.className = 'badge badge-' + d.phase;

  const pct = Math.min(100, (d.elapsed_s / d.total_s) * 100);
  document.getElementById('progress-fill').style.width = pct + '%';
  document.getElementById('hdr-timer').textContent = fmtTime(d.elapsed_s) + ' / ' + fmtTime(d.total_s);

  // Cards
  document.getElementById('c-msgs').textContent = fmtNum(d.msgs_per_sec);
  const bw = fmtBytes(d.bytes_per_sec);
  document.getElementById('c-bw').textContent = bw.v;
  document.getElementById('c-bw-unit').textContent = bw.u;
  document.getElementById('c-p50').textContent = fmtNum(d.lat_p50_us);
  document.getElementById('c-p99').textContent = fmtNum(d.lat_p99_us);
  document.getElementById('c-sent').textContent = fmtNum(d.msgs_sent);
  document.getElementById('c-recv').textContent = fmtNum(d.msgs_recv);

  // Charts
  const t = fmtTime(d.elapsed_s);
  push(tChart, t, Math.round(d.msgs_per_sec));
  push(lChart, t, d.lat_p99_us);

  // Raw table
  const rows = [
    ['Latency Min',    d.lat_min_us + ' µs',   'Latency Avg',   d.lat_avg_us + ' µs'],
    ['Latency P50',    d.lat_p50_us + ' µs',   'Latency P99',   d.lat_p99_us + ' µs'],
    ['Latency P99.9',  d.lat_p999_us + ' µs',  'Latency Max',   d.lat_max_us + ' µs'],
    ['Msgs Sent',      fmtNum(d.msgs_sent),     'Msgs Recv',     fmtNum(d.msgs_recv)],
    ['Throughput',     fmtNum(d.msgs_per_sec) + ' msg/s', 'Bandwidth', bw.v + ' ' + bw.u],
    ['Duplicated',     fmtNum(d.duplicated),    'Reordered',     fmtNum(d.reordered)],
    ['Elapsed',        fmtTime(d.elapsed_s),    'Phase',         d.phase],
  ];
  document.getElementById('raw-table-body').innerHTML = rows.map(r =>
    '<tr><td>' + r[0] + '</td><td>' + r[1] + '</td><td>' + r[2] + '</td><td>' + r[3] + '</td></tr>'
  ).join('');
}

function connect() {
  const es = new EventSource('/events');
  es.addEventListener('update', e => {
    try { onUpdate(JSON.parse(e.data)); } catch(_) {}
  });
  es.onerror = () => {
    es.close();
    setTimeout(connect, 2000);
  };
}

connect();
</script>
</body>
</html>`
