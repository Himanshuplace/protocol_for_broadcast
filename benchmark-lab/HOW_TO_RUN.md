# How to Run — Backend + UI

This shows the **proper way to run the backend and a UI together**. Pick one of
the two UI paths (you can use both).

> Mental model: the **backend** is `benchmark-runner` — it generates the broadcast,
> drives all transports, and measures. It also **serves its own built-in UI**, so
> "backend + UI" is usually a *single command*. **Grafana** is an optional, heavier
> UI that runs separately (in Docker) and reads metrics the backend exposes.

Open a terminal in the project folder for everything below:

```powershell
cd "C:\Users\Lenovo\Documents\Himanshu-Files\Project-For-Learning\protocol_for_broadcast\benchmark-lab"
```

---

## What runs where (ports)

| Piece | What it is | URL / Port | Needs Docker |
|---|---|---|---|
| `benchmark-runner` | Backend (measure) | — | No |
| Built-in live dashboard | UI (served by the backend) | http://localhost:7070 | No |
| Prometheus `/metrics` | Backend metrics feed | http://localhost:9190/metrics | No |
| Prometheus | Metrics store | http://localhost:9090 | Yes |
| Grafana | UI (dashboards) | http://localhost:3000 | Yes |
| `websocket-server` | Backend for k6 | ws://localhost:9555/ws | No |

---

## Path A — Backend + built-in UI (no Docker, simplest)

**One command runs the backend and the UI together.** The `--ui` flag serves the
live dashboard.

```powershell
go run .\cmd\benchmark-runner\ run --protocol udp --duration 60s --warmup 5s --receivers 10 --ui
```

Then open the **UI** in your browser:

```
http://localhost:7070
```

You'll see live cards (throughput, bandwidth, P50/P99 latency, sent/received) and
live charts that update every 500 ms while the benchmark runs.

- Change the UI port if 7070 is taken: add `--ui-port 7171`.
- Try other transports: `--protocol tcp | websocket-gorilla | http2 | sse | http3 | webtransport | webrtc`.

That's the whole "backend + UI" loop in one terminal.

---

## Path B — Backend + Grafana UI (Docker, the serious dashboard)

Grafana gives the deeper, comparison-grade UI. It runs separately and reads the
backend's `/metrics` endpoint via Prometheus. Use **two terminals**.

### Step 1 — Start Docker Desktop
Wait until the Docker whale icon is steady (the daemon must be running).

### Step 2 — Terminal 1: start the UI backend (Prometheus + Grafana)

```powershell
docker compose -f docker\docker-compose.yml up -d prometheus grafana
```

### Step 3 — Terminal 2: run the backend WITH `--metrics`

The `--metrics` flag exposes the data Prometheus scrapes. Use a duration of **at
least 60s** so Prometheus has time to scrape it.

```powershell
go run .\cmd\benchmark-runner\ run --protocol http2 --duration 90s --warmup 5s --receivers 20 --metrics
```

> Tip: add `--ui` too — then you get the built-in dashboard *and* Grafana at once:
> `... --metrics --ui`

### Step 4 — Open the Grafana UI

```
http://localhost:3000
```

Logs in anonymously as admin. Open the **"Protocol Broadcast Benchmark"** dashboard.
It shows latency percentiles, throughput, bandwidth, loss/dup/reorder, CPU, memory,
goroutines, and handles — with a **Protocol** dropdown at the top.

**To compare protocols:** run them one after another (each with `--metrics`), then
select multiple in the Protocol dropdown to overlay them:

```powershell
go run .\cmd\benchmark-runner\ run --protocol udp --duration 90s --metrics
go run .\cmd\benchmark-runner\ run --protocol tcp --duration 90s --metrics
go run .\cmd\benchmark-runner\ run --protocol http3 --duration 90s --metrics
```

### Step 5 — Stop the UI backend when done

```powershell
docker compose -f docker\docker-compose.yml down
```

---

## Path C — Backend + k6 (independent validation)

k6 is an external load tester that subscribes to a real broadcast server and
cross-checks throughput/ordering. Use **two terminals**.

### Terminal 1 — start the publishing backend

```powershell
go run .\cmd\websocket-server\ --addr 127.0.0.1:9555 --rate 10000 --msg-size 512
```

### Terminal 2 — run k6 against it

```powershell
k6 run -e TARGET=ws://localhost:9555/ws -e VUS=20 -e DURATION=30 k6\websocket.js
```

Check the summary: `ws_msgs_received` (throughput), `ws_seq_gaps` and
`ws_bad_magic` (must be **0**). k6 latency is millisecond-granular — for precise
latency trust the Go backend's numbers.

**Optional — feed k6 into Grafana** (Prometheus + Grafana from Path B running):

```powershell
$env:K6_PROMETHEUS_RW_SERVER_URL = "http://localhost:9090/api/v1/write"
k6 run -o experimental-prometheus-rw -e TARGET=ws://localhost:9555/ws k6\websocket.js
```

---

## Path D — Compare two or more protocols (side-by-side report)

Run a chosen set of protocols **sequentially** (for a fair comparison) and get one
HTML report comparing all their metrics, plus a per-client breakdown and an
optional per-packet trace.

```powershell
go run .\cmd\benchmark-runner\ compare `
  --protocols websocket-gorilla,sse `
  --receivers 5 --duration 30s --rate-limit 5000 `
  --trace --output html --out comparison.html
```

Open **`comparison.html`** in a browser. You get:

- **Summary** — every metric side by side (throughput, bandwidth, latency
  p50/p99/p99.9/max, loss %, duplicated, reordered, CPU %, memory, goroutines,
  open FDs/handles, handshake). The best value in each row is highlighted.
- **Per-client breakdown** — one row per connected subscriber: received, lost,
  loss %, dup, reorder, first/last seq, latency p50/p99/max — so you can see which
  client got how much and which one lagged.
- **Per-packet trace** (`--trace`) — `trace-<protocol>.csv` files, one row per
  packet: `protocol, client_id, seq, send_unixnano, recv_unixnano, latency_us`.
  Use this to see exactly when each packet arrived and reconstruct ordering offline.

Notes:
- Omit `--protocols` to compare all 11 transports.
- Always pair `--trace` with `--rate-limit` — a flood produces millions of rows
  (the trace is capped at 5M rows as a safety guard).
- Other outputs: `--output markdown` or `--output json` print to stdout instead of HTML.
- Loss is measured **relative to the fastest client** (what each client missed from
  its first received packet onward), so a late-joining subscriber isn't penalized.

---

## Recommended setup (everything together)

Three terminals + a browser:

```
Terminal 1:  docker compose -f docker\docker-compose.yml up -d prometheus grafana
Terminal 2:  go run .\cmd\benchmark-runner\ run --protocol udp --duration 120s --receivers 10 --metrics --ui
Browser   :  http://localhost:7070   (built-in live UI)
Browser   :  http://localhost:3000   (Grafana, deeper analysis)
```

---

## Save results to a file (no UI)

```powershell
go run .\cmd\benchmark-runner\ run --protocol udp --duration 30s --output json     > result.json
go run .\cmd\benchmark-runner\ run --protocol udp --duration 30s --output markdown  > result.md
go run .\cmd\benchmark-runner\ run --protocol udp --duration 30s --output html      > result.html
```

---

## Troubleshooting

| Problem | Fix |
|---|---|
| UI page at :7070 is blank | Did you pass `--ui`? Is the run still going? |
| Grafana shows "No data" | Run the backend **with `--metrics`**, duration ≥ 60s, Docker running |
| `docker ps` / compose errors | Start Docker Desktop |
| Port 7070 / 9190 in use | `--ui-port 7171` / `--metrics-port 9191` |
| Server port 9000 in use | `--port 9100` |
| `unknown protocol "xyz"` | Use an exact name from the protocol list |
| k6 "connection refused" | Start `websocket-server` first, wait ~2s |
| HTTP/3 / WebRTC / WebTransport fails | QUIC/UDP/TLS can fail on some Windows setups — use udp/tcp/websocket |

---

## See also
- [`GUIDE.md`](./GUIDE.md) — fuller walkthrough and flag reference.
- [`BENCHMARKING.md`](./BENCHMARKING.md) — methodology and *why each protocol behaves differently*.
- **Compare protocols:** see Path D above for the side-by-side comparison report.
```
