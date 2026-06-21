# Complete Guide — Understand & Run the Benchmark

A start-to-finish walkthrough. If you read nothing else, read this. For the deep
*why* behind each protocol, see [`BENCHMARKING.md`](./BENCHMARKING.md).

---

## 1. What this project actually does

It sends the **same** stream of messages over **11 different network protocols**
(UDP, TCP, 3× WebSocket, HTTP/1, HTTP/2, HTTP/3, SSE, WebTransport, WebRTC) and
measures how each one performs: **latency, throughput, packet loss, CPU, memory**.

Because every protocol carries the identical workload through identical
measurement code, the comparison is **fair** — any difference is the protocol's
fault, not the test's.

```
                 ┌─────────────────────────────────────────┐
   one generator │  payload → [24-byte wire header] → frame │
                 └───────────────────┬─────────────────────┘
                                     │ Broadcast()
        ┌────────────┬───────────────┼───────────────┬────────────┐
       UDP          TCP           WebSocket         HTTP/2  ...  WebRTC
        │            │               │                │            │
        └────────────┴───────────────┴───────────────┴────────────┘
                                     │ every received message
                                     ▼
                    ┌────────────────────────────────┐
                    │  one Recorder                  │
                    │  • HDR latency (nanosecond)    │
                    │  • loss / reorder / duplicate  │
                    │  • CPU / mem / goroutines / FDs │
                    └────────────────────────────────┘
```

---

## 2. Before you start

| Need | Status on this machine | If missing |
|---|---|---|
| **Go 1.25+** | ✅ Installed (go 1.26) | https://go.dev/dl |
| **k6** | ✅ Installed (v2.0.0) | `winget install GrafanaLabs.k6` |
| **Docker Desktop** | ⚠️ Installed but **not running** | Start Docker Desktop (only needed for Grafana) |

Open a terminal **in the `benchmark-lab` folder** for every command below:

```powershell
cd "C:\Users\Lenovo\Documents\Himanshu-Files\Project-For-Learning\protocol_for_broadcast\benchmark-lab"
```

---

## 3. Three ways to see results (pick what you need)

| Way | Command flag | Where you see it | Needs Docker? |
|---|---|---|---|
| **A. JSON output** | (default) | Terminal | No |
| **B. Live web dashboard** | `--ui` | http://localhost:7070 | No |
| **C. Grafana (serious)** | `--metrics` | http://localhost:3000 | Yes |

You can combine them: `--ui --metrics` runs both at once.

---

## 4. The fastest possible run (30 seconds)

```powershell
go run .\cmd\benchmark-runner\ run --protocol udp --duration 10s
```

You'll get a JSON block at the end with latency percentiles, throughput, loss,
CPU, and memory. That's the raw, authoritative result.

---

## 5. Way B — Live web dashboard (recommended for a quick look)

```powershell
go run .\cmd\benchmark-runner\ run --protocol udp --duration 30s --warmup 5s --ui
```

Then open **http://localhost:7070**. You'll see:

- A progress bar (warmup → measuring → done)
- Live cards: throughput, bandwidth, P50/P99 latency, sent/received
- Two live charts: messages/sec and P99 latency over time

No Docker required. Change the port with `--ui-port 7171` if 7070 is busy.

Try other protocols the same way:

```powershell
go run .\cmd\benchmark-runner\ run --protocol tcp               --duration 30s --ui
go run .\cmd\benchmark-runner\ run --protocol websocket-gorilla --duration 30s --ui
go run .\cmd\benchmark-runner\ run --protocol http2             --duration 30s --ui
```

---

## 6. Way C — Grafana (the serious, big-company dashboard)

This is the one for proper analysis and comparing protocols side-by-side.

**Step 1 — Start Docker Desktop** (wait until the whale icon is steady).

**Step 2 — Bring up Prometheus + Grafana:**

```powershell
docker compose -f docker\docker-compose.yml up -d prometheus grafana
```

**Step 3 — Run a benchmark WITH `--metrics`** (so Prometheus can scrape it):

```powershell
go run .\cmd\benchmark-runner\ run --protocol http2 --duration 60s --receivers 20 --metrics
```

**Step 4 — Open Grafana:** http://localhost:3000
(logs in anonymously as admin; open the **"Protocol Broadcast Benchmark"** dashboard).

You'll see latency percentiles, throughput, bandwidth, loss/dup/reorder, CPU,
memory, goroutines, and open handles — all live, with a **Protocol dropdown** at
the top.

**To compare protocols:** run them one after another, each with `--metrics`, then
select multiple in the Protocol dropdown to overlay them on the same charts:

```powershell
go run .\cmd\benchmark-runner\ run --protocol udp   --duration 60s --metrics
go run .\cmd\benchmark-runner\ run --protocol tcp   --duration 60s --metrics
go run .\cmd\benchmark-runner\ run --protocol http3 --duration 60s --metrics
```

**When done:**

```powershell
docker compose -f docker\docker-compose.yml down
```

---

## 7. Run k6 (independent validation of WebSocket)

k6 is the industry-standard load tester. Here it independently confirms a
standard third-party client can subscribe to the broadcast and sustain the
throughput, and that the wire frames decode correctly.

**Terminal 1 — start the publishing WebSocket server:**

```powershell
go run .\cmd\websocket-server\ --addr 127.0.0.1:9555 --rate 10000 --msg-size 512
```

**Terminal 2 — run k6 against it:**

```powershell
k6 run -e TARGET=ws://localhost:9555/ws -e VUS=20 -e DURATION=30 k6\websocket.js
```

Look for these in the k6 summary:
- `ws_msgs_received` — total messages a standard client received
- `ws_seq_gaps` — must be **0** (no loss/reorder over reliable WebSocket)
- `ws_bad_magic` — must be **0** (frames decoded correctly)

> k6's latency is millisecond-granular, so it reports ~0ms on loopback. That's
> expected — the Go harness (nanosecond HDR histogram) is the real latency
> instrument. k6 validates throughput, delivery, and ordering.

**Optional — stream k6 into the same Grafana** (Prometheus + Grafana running):

```powershell
$env:K6_PROMETHEUS_RW_SERVER_URL = "http://localhost:9090/api/v1/write"
k6 run -o experimental-prometheus-rw -e TARGET=ws://localhost:9555/ws k6\websocket.js
```

---

## 8. The flags you'll actually use

```powershell
go run .\cmd\benchmark-runner\ run `
  --protocol  udp     `   # which transport (see list below)
  --duration  60s     `   # how long to measure
  --warmup    5s      `   # discarded ramp-up time
  --receivers 10      `   # number of subscribers
  --msg-size  1024    `   # payload bytes per message
  --rate-limit 0      `   # max msgs/sec (0 = flood as fast as possible)
  --generator random  `   # random | sequential | json | binary | market
  --output    json    `   # json | markdown | html
  --ui                `   # live web dashboard (localhost:7070)
  --metrics               # Prometheus endpoint for Grafana (localhost:9190)
```

**Protocol names:** `udp`, `tcp`, `websocket-gorilla`, `websocket-gobwas`,
`websocket-coder`, `http1`, `http2`, `http3`, `sse`, `webtransport`, `webrtc`.

---

## 9. How to read the numbers

- **P99 / P999 latency is the headline** for real-time, not the average. A good
  average with a bad P999 = periodic stalls (usually head-of-line blocking).
- **Loss/reorder should be 0** for everything except UDP. Non-zero loss on a
  reliable protocol means the receiver couldn't keep up — a real capacity limit.
- **CPU per message:** UDP/TCP are cheap (kernel path); HTTP/3, WebTransport, and
  WebRTC are expensive (user-space crypto + congestion control).
- **Memory & handles grow with `--receivers`** for connection-based protocols —
  that's why connectionless UDP fans out more cheaply.

---

## 10. Troubleshooting

| Problem | Fix |
|---|---|
| `unknown protocol "xyz"` | Use an exact name from §8 |
| Port 7070 / 9190 in use | `--ui-port 7171` / `--metrics-port 9191` |
| Server port 9000 in use | `--port 9100` |
| Grafana empty / "No data" | Did you run with `--metrics`? Is Docker running? |
| Grafana can't reach runner | Runner must be running **with `--metrics`** during the scrape |
| HTTP/3, WebRTC, WebTransport fails | These use QUIC/UDP/TLS which can fail on some Windows setups — use udp/tcp/websocket |
| k6 "connection refused" | Start `websocket-server` first and wait ~2s before running k6 |
| `docker ps` errors | Docker Desktop isn't running — start it |

---

## 11. Where to go next

- **[`BENCHMARKING.md`](./BENCHMARKING.md)** — the full methodology and the
  per-protocol deep dive: *why* each protocol behaves the way it does and what
  problem each one has.
- **[`README.md`](./README.md)** — concise flag reference and build instructions.
- **[`ARCHITECTURE.html`](./ARCHITECTURE.html)** — visual walkthrough of the code
  architecture (open in a browser).
```
