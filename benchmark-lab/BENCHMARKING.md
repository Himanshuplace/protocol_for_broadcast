# Protocol Broadcast Benchmark — Methodology & Analysis

A rigorous, apples-to-apples comparison of 11 network transports carrying the
**same** real-time broadcast workload. This document explains *what* is measured,
*how* fairness is guaranteed, *why* each protocol behaves the way it does, and
*how* to run and read the results.

> **The core principle:** hold everything constant except the transport. Same
> payload generator, same wire frame, same broadcast fan-out, same measurement
> code. The only independent variable is the protocol. That is what makes the
> comparison fair.

---

## 1. Architecture of the measurement

### 1.1 One engine, one workload

Every transport implements a single Go interface (`pkg/transport.Transport`):

```
Start() · Stop() · Broadcast([]byte) · Send(id, []byte) · Connections() · Stats()
```

The benchmark runner (`internal/scenarios`) is protocol-agnostic. It:

1. Builds **one** payload generator (random / sequential / json / binary / market).
2. Calls `Broadcast()` in a tight loop (optionally rate-limited).
3. Routes every received message through **one** recorder.

Because the generator, the loop, and the recorder are identical for all 11
transports, any difference in the numbers is caused by the protocol — not by the
test harness.

### 1.2 The wire frame (how latency is measured at all)

Every message, on every protocol, is prefixed with the same 24-byte header
(`pkg/wire`, little-endian):

```
[0:4]   uint32  Magic   = 0xBEEFCAFE   frame marker
[4:12]  uint64  SeqNum                 monotonic per-sender counter
[12:20] int64   SendNs                 sender's time.Now().UnixNano()
[20:24] uint32  PayloadLen             payload byte count
[24:]   bytes   Payload
```

- **SeqNum** → loss, duplication, and reordering detection (sliding-window bitset).
- **SendNs** → one-way latency = `recvNs − sendNs`. On a single host the sender and
  receiver share the same monotonic clock, so this is exact to the nanosecond.

### 1.3 Why the Go harness is the instrument (not k6)

k6 is a first-class HTTP/WebSocket *client-side* load tester. But:

- It cannot speak **6 of the 11** transports (UDP, raw TCP, HTTP/3, WebTransport,
  WebRTC) without fragile, hand-built extensions that would just re-wrap this
  Go code in a slower shell.
- Its JS clock (`Date.now()`) is **millisecond** granular. Loopback latencies here
  are **microseconds**, so k6 would report ~0 ms — useless for tail latency.
- Its per-sample pipeline can't keep up with 100k+ msg/s without distorting the
  very latency it measures.

So the Go harness — with a nanosecond HDR histogram recording off a dedicated
path — is the authoritative instrument. **k6's role is independent validation**
(see §5): it confirms a standard third-party client interoperates with the
servers and sustains the throughput, which earns trust in the Go numbers.

---

## 2. What is measured (metric catalog)

| Metric | Source | Meaning |
|---|---|---|
| **Latency** min/avg/p50/p95/p99/p999/max | HDR histogram (1ns–30s, 3 sig figs) | One-way send→receive time. Tail percentiles (p99/p999) matter most for real-time. |
| **Throughput** msg/s | atomic counter / elapsed | Messages delivered per second. |
| **Bandwidth** bytes/s | atomic counter / elapsed | Wire bytes per second (payload + 24-byte header). |
| **Loss** | sequence tracker | SeqNums never received by end of run. |
| **Duplicates** | sequence tracker | SeqNums received more than once. |
| **Reorders** | sequence tracker | SeqNums that arrived after a later one. |
| **CPU %** | `GetProcessTimes` (Win) / `/proc/self/stat` (Linux) | 100 = one full core. Same accounting as Task Manager / `top`. |
| **Memory** | `runtime.ReadMemStats.Sys` | Bytes obtained from the OS. |
| **Goroutines** | `runtime.NumGoroutine` | Concurrency cost of the transport. |
| **Open handles / FDs** | `GetProcessHandleCount` (Win) / `/proc/self/fd` (Linux) | Sockets/files held open — grows with connection count. |
| **Handshake** avg/p99 | HDR histogram | Connection-establishment cost (matters for TLS/QUIC/WebRTC). |

Resource sampling runs on a background goroutine every 100 ms — **off the message
hot path** — so enabling metrics adds no per-message overhead and does not skew
latency. Every protocol pays the same (zero) measurement tax.

---

## 3. Per-protocol analysis — how each differs and *why it has the problems it has*

All 11 carry the identical broadcast. The differences below are intrinsic to the
protocol, so the benchmark surfaces them cleanly.

### UDP — `--protocol udp`
- **How it carries the broadcast:** one connectionless datagram per message to each subscriber.
- **Strength:** lowest possible latency; no connection state; no head-of-line blocking — a lost packet never delays the next.
- **The problems & why:**
  - **No reliability** — the network may drop datagrams; nothing retransmits. Expect non-zero *loss* under load. This is by design (UDP has no ACKs).
  - **No ordering** — packets can arrive out of sequence → *reorders*.
  - **No flow/congestion control** — a fast sender overruns a slow receiver's socket buffer → bursty loss.
  - **MTU limited** — payloads above ~1472 bytes fragment; large messages amplify loss.
  - On **Windows** there is no `SO_REUSEPORT`, so only one listener socket (Linux can fan-in across cores).
- **Watch:** lowest p99 latency, but the only protocol with real loss/reorder.

### TCP — `--protocol tcp`
- **How:** reliable ordered byte stream; one socket per subscriber, framed by the 24-byte header.
- **Strength:** zero loss, zero reorder, congestion-controlled, universal.
- **The problems & why:**
  - **Head-of-line blocking** — a single lost segment stalls *all* bytes behind it until retransmitted. Tail latency spikes under loss.
  - **Nagle's algorithm** batches small writes; without `TCP_NODELAY` it adds up to ~40 ms. (Disabled here, but it's the classic TCP latency trap.)
  - **Per-connection state** — kernel buffers + a socket per subscriber → memory and FD count grow O(N).
  - **Slow start** — throughput ramps rather than starting at full rate.
- **Watch:** zero loss, but higher p99 than UDP and FD/memory rising with subscribers.

### HTTP/1.1 — `--protocol http1`
- **How:** long-lived chunked-transfer response; the server streams frames down an open connection.
- **Strength:** passes through virtually any proxy/firewall; trivially debuggable.
- **The problems & why:**
  - **One response at a time per connection** — no multiplexing; concurrency needs many TCP connections.
  - **Chunked-encoding overhead** — each chunk carries a size line.
  - Inherits **all TCP problems** (HoL, per-connection state).
- **Watch:** more overhead and connections than raw TCP for the same data.

### HTTP/2 — `--protocol http2`
- **How:** many binary multiplexed *streams* over a single TCP connection.
- **Strength:** one connection serves many logical streams; HPACK header compression; binary framing.
- **The problems & why:**
  - **TCP head-of-line blocking across streams** — this is HTTP/2's defining flaw: one lost TCP segment stalls *every* stream on the connection, because they all share one ordered byte stream. (This is precisely what HTTP/3 fixes.)
  - **Flow-control windows** can throttle a fast broadcaster.
  - More CPU than HTTP/1 for framing/compression.
- **Watch:** good multiplexing, but tail latency suffers under any packet loss because of shared-connection HoL.

### HTTP/3 (QUIC) — `--protocol http3`
- **How:** multiplexed streams over QUIC over UDP, with built-in TLS 1.3.
- **Strength:** **no cross-stream HoL** (each stream is independently ordered); fast 1-RTT/0-RTT handshakes; connection migration.
- **The problems & why:**
  - **Higher CPU** — congestion control + crypto run in user space, not the kernel. Expect the highest CPU per message.
  - **UDP is often throttled/blocked** by middleboxes and OS UDP buffers (notably on Windows).
  - Younger stacks → more variance.
- **Watch:** better tail latency than HTTP/2 under loss, but the CPU panel will be highest.

### SSE (Server-Sent Events) — `--protocol sse`
- **How:** `text/event-stream` over HTTP; the server pushes `data:` lines.
- **Strength:** dead simple, auto-reconnect built in, one-way push fits market data.
- **The problems & why:**
  - **Text only** — binary payloads must be base64-encoded → **~33% bandwidth bloat** and encode/decode CPU. The benchmark shows this directly in bytes/s.
  - **Unidirectional** (server→client only).
  - **Line parsing** overhead per event.
  - Inherits TCP HoL.
- **Watch:** noticeably higher bytes/s than binary protocols for the same payload — that's the base64 tax.

### WebSocket × 3 libraries — `--protocol websocket-gorilla | websocket-gobwas | websocket-coder`
- **How:** full-duplex binary frames over an upgraded HTTP/TCP connection.
- **Strength:** bidirectional, low per-message overhead after handshake, binary-native, browser-friendly.
- **The problems & why:** inherits TCP HoL and per-connection state. The interesting comparison here is **library implementation at the same protocol**:
  - **gorilla** — mature, a goroutine pair per connection; easiest, moderate memory.
  - **gobwas** — zero-copy, low-allocation, manual buffer control; typically the **lowest memory and CPU**.
  - **coder** — modern, context-aware, `net/http`-native API; clean, middle-ground performance.
- **Watch:** identical protocol behavior, but the CPU/memory/goroutine panels expose the library cost differences — a great demonstration of why implementation matters as much as protocol.

### WebTransport — `--protocol webtransport`
- **How:** reliable streams *and* unreliable datagrams over HTTP/3/QUIC.
- **Strength:** pick per-message reliability; multiplexed; no cross-stream HoL.
- **The problems & why:**
  - **Very new** — limited client/library maturity.
  - **QUIC CPU cost** (same as HTTP/3).
  - **Datagram size limits** (bounded by QUIC packet size).
  - Longer handshake than raw UDP/TCP.
- **Watch:** flexibility of UDP-like datagrams with QUIC's stream quality, at a CPU/handshake premium.

### WebRTC DataChannel — `--protocol webrtc`
- **How:** SCTP over DTLS over ICE/UDP, with a signaling step to exchange SDP/ICE.
- **Strength:** configurable reliability *and* ordering per channel; designed for P2P/NAT traversal.
- **The problems & why:**
  - **Heavy setup** — ICE candidate gathering + DTLS handshake + SCTP association → by far the **highest handshake latency**. The benchmark waits up to 15 s for connections for this reason.
  - **STUN/TURN dependency** for NAT traversal (uses Google STUN by default here).
  - **SCTP overhead** on top of DTLS on top of UDP.
  - Complex; overkill for a simple server→client fan-out.
- **Watch:** the handshake metric dwarfs every other protocol; steady-state latency is reasonable but setup cost is enormous.

### Summary of the trade-off space

| Protocol | Reliable | Ordered | HoL blocking | Handshake cost | CPU | Best at |
|---|---|---|---|---|---|---|
| UDP | ✗ | ✗ | none | ~0 | low | lowest latency, tolerant of loss |
| TCP | ✓ | ✓ | yes (stream) | low | low | simple reliable streams |
| HTTP/1 | ✓ | ✓ | yes | low | low | proxy/firewall traversal |
| HTTP/2 | ✓ | ✓ | yes (shared) | medium | medium | many streams, one connection |
| HTTP/3 | ✓ | ✓ | none (per-stream) | low (1-RTT) | high | streams without HoL |
| SSE | ✓ | ✓ | yes | low | low (but base64 bloat) | simple browser push |
| WebSocket | ✓ | ✓ | yes (stream) | low | low–med | bidirectional browser apps |
| WebTransport | ✓/✗ | ✓/✗ | none | medium | high | mixed reliability over QUIC |
| WebRTC | ✓/✗ | ✓/✗ | none | **very high** | high | P2P / NAT traversal |

---

## 4. Running the benchmark

### 4.1 Single protocol with full metrics + live dashboard

```powershell
# From benchmark-lab/
go run .\cmd\benchmark-runner\ run --protocol udp --duration 30s --warmup 5s `
  --receivers 10 --msg-size 1024 --metrics --ui
```

- `--metrics` exposes Prometheus at `http://localhost:9190/metrics` (for Grafana).
- `--ui` opens the lightweight live view at `http://localhost:7070` (no Docker).
- Final JSON (authoritative HDR latency) prints to stdout. Use `--output markdown` for a table.

### 4.2 Grafana (the serious dashboard)

```powershell
# 1. Start Docker Desktop (the daemon must be running).
# 2. Bring up Prometheus + Grafana:
docker compose -f docker\docker-compose.yml up -d prometheus grafana

# 3. Run any benchmark WITH --metrics so Prometheus can scrape it:
go run .\cmd\benchmark-runner\ run --protocol http2 --duration 60s --receivers 20 --metrics

# 4. Open Grafana:
#    http://localhost:3000   (anonymous admin; dashboard: "Protocol Broadcast Benchmark")
```

Prometheus scrapes the runner via `host.docker.internal:9190`. The dashboard
auto-provisions with panels for latency percentiles, throughput, bandwidth,
loss/dup/reorder, CPU, memory, goroutines, and handles — with a **protocol**
selector so you can overlay transports for direct comparison. Run several
protocols in sequence (all with `--metrics`) and compare them in one view.

### 4.3 Compare all protocols in one shot

```powershell
go run .\cmd\benchmark-runner\ compare --duration 20s --receivers 10 --output markdown
```

---

## 5. k6 — independent validation (HTTP/WebSocket family)

k6 is the industry-standard external load tester. Here it **cross-checks** the Go
harness on the transports it natively speaks, proving the servers interoperate
with a standard client and sustain the measured throughput.

> **Scope (honest):** k6 cleanly validates **WebSocket** for a streaming broadcast.
> SSE needs the `xk6-sse` extension; HTTP/1 and HTTP/2 *infinite-stream* broadcast
> is not a natural fit for k6's request/response model. k6 latency is **millisecond**
> granular — use it for throughput/delivery/ordering, not µs tail latency.

### Run the WebSocket validation

```powershell
# 1. Start the publishing server:
go run .\cmd\websocket-server\ --addr 127.0.0.1:9555 --rate 10000 --msg-size 512

# 2. In another terminal, run k6:
k6 run -e TARGET=ws://localhost:9555/ws -e VUS=20 -e DURATION=30 k6\websocket.js
```

k6 decodes the same 24-byte wire header, so it reports:
- `ws_msgs_received`, `ws_bytes_received` — throughput a standard client sustains
- `ws_seq_gaps` — loss/reorder (threshold: **must be 0** over reliable WS)
- `ws_bad_magic` — frame-decode correctness (threshold: **must be 0**)
- `ws_latency_ms` — coarse latency sanity bound

### Stream k6 into the same Grafana

```powershell
$env:K6_PROMETHEUS_RW_SERVER_URL = "http://localhost:9090/api/v1/write"
k6 run -o experimental-prometheus-rw -e TARGET=ws://localhost:9555/ws k6\websocket.js
```

Prometheus is started with `--web.enable-remote-write-receiver`, so k6's metrics
land beside the Go harness's in Grafana. **Where they overlap (WebSocket), agreement
between k6 and the Go harness validates the harness** — which is what lets you trust
the Go numbers for UDP/WebRTC/WebTransport, where k6 cannot go.

---

## 6. How to read the results

- **Tail latency (p99, p999) is the headline number** for real-time broadcast, not
  the average. A great average with a bad p999 means periodic stalls — usually
  head-of-line blocking.
- **Loss/reorder should be zero** for every protocol except UDP (and WebTransport/
  WebRTC datagram mode). Non-zero loss on a reliable protocol means the *receiver
  could not keep up* — a real capacity finding.
- **CPU per message** separates kernel-path protocols (UDP/TCP: low) from
  user-space crypto protocols (HTTP/3/WebTransport/WebRTC: high).
- **Memory and handles scaling with `--receivers`** exposes per-connection cost —
  the reason connectionless UDP scales fan-out more cheaply than per-socket TCP.
- **Warmup is discarded.** Only the measurement window counts, so slow-start and
  JIT/connection ramp don't pollute the steady-state numbers.
```
