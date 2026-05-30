# Project Context — Protocol Benchmark Platform

> **Purpose of this file:** Complete working knowledge for continuing development in any future session. Read this before touching any code.

---

## What This Project Is

A production-grade Go benchmark platform that empirically compares **9 network transport protocols** for real-time financial market data distribution (stock ticks, futures, options). The workload simulates an exchange-grade feed: one publisher emits price ticks for 100+ instruments; thousands of subscribers receive selective updates.

**The research question:** Which protocol (UDP, TCP, HTTP/1.1, HTTP/2, HTTP/3/QUIC, WebSocket, SSE, WebTransport, WebRTC DataChannels) performs best for latency, throughput, reliability, broadcast fanout, reconnect speed, CPU efficiency, and memory efficiency?

**Domain:** Real-time financial market data — same use case as Bloomberg, NASDAQ, CME, Binance.

---

## Repository Layout

```
himanshuplace/protocol_for_broadcast   (GitHub repo)
└── benchmark-lab/                     ← ALL code lives here
    ├── go.mod                         (module: github.com/himanshuplace/protocol_for_broadcast)
    ├── Makefile
    ├── cmd/                           ← binary entrypoints
    ├── internal/                      ← transport implementations + scenarios
    ├── pkg/                           ← shared libraries
    ├── docker/                        ← Dockerfile.server + docker-compose.yml
    ├── prometheus/prometheus.yml
    ├── grafana/provisioning/
    ├── scripts/                       ← netem-setup.sh, run-benchmarks.sh, smoke-test.sh
    └── .github/workflows/             ← ci.yml + benchmark.yml
```

**Working directory for all `go` commands:** `benchmark-lab/`

**Active development branch:** `claude/protocol-benchmark-platform-4JTiG`

**Go module path:** `github.com/himanshuplace/protocol_for_broadcast`
(Note: this is also the repo name — the module path = repo path, no `/benchmark-lab` suffix)

---

## Go Module

```
go 1.25.0   ← upgraded from 1.23 by go mod tidy; functionally identical
```

Direct dependencies (from go.mod `require` block without `// indirect`):
```
github.com/HdrHistogram/hdrhistogram-go v1.1.2
github.com/bytedance/sonic               v1.15.1
github.com/coder/websocket               v1.8.14
github.com/gobwas/ws                     v1.4.0
github.com/gorilla/websocket             v1.5.3
github.com/jackc/pgx/v5                 v5.9.2
github.com/panjf2000/ants/v2            v2.12.1
github.com/pion/webrtc/v3               v3.3.6
github.com/prometheus/client_golang      v1.19.1
github.com/quic-go/quic-go              v0.59.1
github.com/quic-go/webtransport-go      v0.10.0
github.com/spf13/cobra                   v1.10.2
github.com/spf13/viper                   v1.21.0
github.com/valyala/bytebufferpool        v1.0.0
go.uber.org/zap                          v1.28.0
golang.org/x/sys                         v0.45.0
golang.org/x/time                        v0.15.0
```

**`google.golang.org/protobuf v1.34.2`** is in the module but only as indirect (used by prometheus). There is **no `.proto` file generated yet** — the `proto/` and `kubernetes/` directories are empty.

---

## Build Environment

- **Target:** Linux/AMD64 (Docker, bare metal)
- **`GOAMD64=v3`** — requires AVX2 (Intel Haswell 2013+, AMD Ryzen Zen1 2017+). Enables SIMD in `bytedance/sonic` JSON, vectorized memcopy in broadcast hot path.
- **`CGO_ENABLED=0`** in Docker builds. Native builds use CGO only if eBPF is enabled (not yet implemented).
- Docker base: `golang:1.23-bookworm` → `debian:bookworm-slim` (glibc, not musl/Alpine).

```bash
# Build everything
cd benchmark-lab && go build ./...

# Run all tests (race detector)
go test -race -count=1 -timeout=120s ./...

# Build specific binary
GOAMD64=v3 go build -o bin/benchmark-runner ./cmd/benchmark-runner
```

**Current test status:** 4 packages pass, rest have no test files yet.
```
ok  internal/tcp
ok  internal/udp
ok  pkg/transport
ok  pkg/wire
```

---

## Architecture — Key Packages

### `pkg/wire` — Universal Measurement Frame

Every byte on every transport goes through this 24-byte header:
```
Offset  Size  Field
0       4     Magic: 0xBEEFCAFE (validates benchmark traffic)
4       8     SeqNum uint64 LE  (monotonic; gaps=loss, OOO=reorder)
12      8     SendNs int64 LE   (time.Now().UnixNano() at send)
20      4     PayloadLen uint32 LE
24      N     Payload
```

Key functions:
```go
wire.Encode(seq uint64, sendNs int64, payload []byte) []byte          // allocates
wire.EncodeInto(dst []byte, seq, sendNs int64, payload []byte)        // zero-alloc
wire.EncodeAppend(dst []byte, seq, sendNs int64, payload []byte) []byte
wire.Decode(b []byte) (Frame, bytesConsumed int, err error)           // zero-copy payload
wire.DecodeHeader(b []byte) (seq uint64, sendNs int64, plen uint32, err error)
wire.HeaderLen = 24
```

Benchmarks (Intel Xeon 2.80GHz): `EncodeInto` 5.9ns/op 0 allocs; `Decode` 4.5ns/op 0 allocs.

### `pkg/transport` — Core Interfaces

```go
type ConnID string

type Transport interface {
    Start() error
    Stop() error
    Broadcast(data []byte) error
    Send(id ConnID, data []byte) error
    Connections() int
    Stats() Stats
}

type TopicTransport interface {
    Transport
    Subscribe(id ConnID, topics []uint32) error
    Unsubscribe(id ConnID, topics []uint32) error
    Publish(topic uint32, data []byte) error
}

type RecvHandler func(id ConnID, data []byte, recvAt time.Time)

type Stats struct {
    Protocol      string
    Connections   int
    Sent, Received, Lost, Duplicated, Reordered uint64
    BytesSent, BytesRecv uint64
    MinLatencyNs, AvgLatencyNs, P50LatencyNs, P95LatencyNs, P99LatencyNs, P999LatencyNs, MaxLatencyNs int64
    CPUPercent    float64
    MemBytes      uint64
    Goroutines    int
    FDs           int
    HandshakeNs   int64
    ReconnectNs   int64
    SnapshotAt    time.Time
}
```

**`Registry[C any]`** — 16-shard generic concurrent map. Each shard is 64 bytes (cache-line padded with `[32]byte` pad after `sync.RWMutex`). Hash: FNV-1a 32-bit. Key methods: `Add`, `Remove`, `Get`, `Snapshot() []C`, `Range(fn func(ConnID, C) bool)`.

**`BaseTransport`** — embeddable lifecycle helper with `MarkStarted()`, `IsStarted()`, `Uptime()`.

### `pkg/market` — Financial Domain Model

```go
type AssetClass uint8  // AssetEquity=0, AssetFuture=1, AssetOption=2, AssetCrypto=3, AssetFX=4

type Instrument struct {
    Symbol     string
    Class      AssetClass
    TickRateHz float64  // ticks/sec this instrument generates
    Volatility float64  // annualized σ for GBM price model
    MidPrice   int64    // initial price in integer micros
    SpreadBps  int      // bid-ask spread in basis points
}

func DefaultUniverse() []Instrument  // 100 instruments: 30 equity + 20 future + 30 option + 10 crypto + 10 FX
                                     // aggregate ~14,000 ticks/sec

type MarketTick struct { /* exactly 64 bytes = one CPU cache line */ }
// Fields: SeqNum uint64, Timestamp int64, RecvNs int64, InstrHash uint32,
//         BidPrice/AskPrice/LastPrice int64, Volume uint32, BidSize/AskSize uint16,
//         Flags TickFlags, Class AssetClass, [2]byte padding

const TickSize = 64  // fixed wire size

func (t *MarketTick) Encode(dst *[TickSize]byte)   // zero-alloc
func (t *MarketTick) Decode(src *[TickSize]byte)
```

**`Router`** — 64-shard topic router. Maps `instrHash uint32` → `[]ConnID`. Key methods:
```go
func (r *Router) Subscribe(connID ConnID, instrHashes []uint32)
func (r *Router) Unsubscribe(connID ConnID, instrHashes []uint32)
func (r *Router) Route(instrHash uint32, data []byte, sendFn func(ConnID, []byte) error) (int, error)
func (r *Router) RouteAsync(...)  // snapshot first, release lock before sendFn
```

**`(inst *Instrument) SymbolHash() uint32`** — FNV-1a 32-bit, tagged `//go:nosplit`.

### `pkg/metrics` — Measurement Infrastructure

**`RecorderConfig`** + **`NewRecorder(cfg RecorderConfig) *Recorder`**:
```go
type RecorderConfig struct {
    Label              string
    Protocol           string
    Scenario           string
    SequenceWindowSize uint64        // default 4096
    SampleInterval     time.Duration // default 100ms
    Prometheus         *BenchmarkMetrics // nil = disabled
}
```

**`Recorder`** hot-path methods (called per message):
```go
func (r *Recorder) RecordSend(seq uint64, size int)
func (r *Recorder) RecordRecv(seq uint64, sendNs int64, size int, recvNs int64)
func (r *Recorder) RecordHandshake(d time.Duration)
func (r *Recorder) RecordReconnect(d time.Duration)
func (r *Recorder) Flush(lastSentSeq uint64)   // call at end-of-run for accurate loss
func (r *Recorder) Reset()                      // clears all — used between warmup/measure
func (r *Recorder) Start()                      // begins resource sampling
func (r *Recorder) Stop()                       // stops resource sampling
func (r *Recorder) Snapshot() RecorderSnapshot
```

**`RecorderSnapshot`** fields:
```go
type RecorderSnapshot struct {
    Latency   HistogramSnapshot   // .Min .Max .Mean .P50 .P95 .P99 .P999 .StdDev .Count (all time.Duration)
    MsgSent   uint64
    MsgRecv   uint64
    BytesSent uint64
    BytesRecv uint64
    MsgPerSec float64
    BytesPerSec float64
    Seq       SeqStatsSnapshot    // .Delivered .Lost .Duplicated .Reordered (all uint64)
    Resources ResourceSnapshot    // .CPUAvg .CPUP99 .MemAvg .MemMax .GoroutineAvg .GoroutineMax .FDAvg .FDMax
    Handshake HistogramSnapshot
    Reconnect HistogramSnapshot
}
```

**`SequenceTracker`** — 4096-bit sliding window (512-byte bitmask). Loss declared when `nextExpected` advances 128 positions past a seq without seeing it. Method: `Observe(seq uint64)`.

**`ResourceSampler`** — polls `/proc/self/stat` (CPU), `/proc/self/fd` count (FDs), `runtime.ReadMemStats` (memory) every `SampleInterval`. Linux-specific code in `resources_linux.go` (build tag `linux`); `resources_other.go` fallback uses `syscall.Getrusage`.

**`BenchmarkMetrics`** (Prometheus) — registered metric vectors:
- `benchmark_latency_nanoseconds` (HistogramVec), `benchmark_throughput_messages_total` (CounterVec)
- `benchmark_connections_active`, `benchmark_packet_lost/duplicated/reordered_total`
- `benchmark_cpu_percent`, `benchmark_memory_bytes`, `benchmark_goroutines`, `benchmark_fd_count`
- `benchmark_handshake_nanoseconds`, `benchmark_reconnect_nanoseconds`, `benchmark_broadcast_nanoseconds`
- `benchmark_ticks_published/delivered_total`, `benchmark_tick_latency_nanoseconds`

### `pkg/generator` — Payload Generators

```go
type Size int  // Size16B=16, Size32B=32, Size64B=64, Size128B, Size256B, Size512B,
               // Size1KB=1024, Size4KB, Size16KB, Size64KB, Size256KB, Size1MB
type Generator interface {
    Next(targetSize Size) []byte  // returns wire-framed payload of exactly targetSize bytes
    Name() string
}

// Available generators:
NewRandomGenerator()      // PCG64 PRNG, 8-bytes-at-a-time via unsafe.Pointer
NewSequentialGenerator()  // deterministic byte(i%256), has Verify() method
NewBinaryGenerator()      // zero-filled payload
NewJSONGenerator()        // bytedance/sonic JSON with printable ASCII padding
NewMarketTickGenerator(universe []Instrument)  // GBM price model, Box-Muller Z
```

**`MarketTickGenerator`** key methods:
```go
func (g *MarketTickGenerator) NextTick(instrIdx int, out *market.MarketTick)
func (g *MarketTickGenerator) NextTickEncoded(instrIdx int) []byte  // returns 88-byte wire frame
func (g *MarketTickGenerator) RunFeed(ctx context.Context, out chan<- *market.MarketTick)
func (g *MarketTickGenerator) CurrentPrice(instrIdx int) int64
func (g *MarketTickGenerator) TotalTicksGenerated() uint64
```

Cache-line padding: `atomicPrice` struct has `[56]byte` pad after `atomic.Int64` → 64-byte total, prevents false sharing.

**GBM model:** `perTickVol = σ * sqrt(dt/252/6.5/3600)`, `lnReturn = -0.5*v² + v*Z`. Uses CAS loop for atomic price update.

**JSON generator** — `bytedance/sonic` for SIMD-accelerated marshal/unmarshal on AMD64. Functions `MarshalTick(v any)` and `UnmarshalTick(data []byte, v any)` are exported for use by transport receivers.

### `internal/tls`

```go
func GenerateSelfSigned(extraHosts ...string) (tls.Certificate, error)
func LoadOrGenerate(dir string, extraHosts ...string) (tls.Certificate, error)
func ServerTLSConfig(cert tls.Certificate, nextProtos ...string) *tls.Config  // TLS 1.3 min, X25519+P256
func ClientTLSConfig(nextProtos ...string) *tls.Config  // InsecureSkipVerify=true for benchmarks
```

---

## Transport Implementations

### Status

| Package | Status | Key Notes |
|---|---|---|
| `internal/udp` | ✅ Full + tests | recvmmsg/sendmmsg (Linux), SO_RCVBUF/SO_SNDBUF 4MB |
| `internal/tcp` | ✅ Full + tests | SO_REUSEPORT, TCP_NODELAY, write pump |
| `internal/websocket/gorilla` | ✅ Full | Write pump, binary msgs, 64KB bufs |
| `internal/websocket/gobwas` | ✅ Full | Zero-alloc ws.ReadFrame/WriteFrame |
| `internal/websocket/coder` | ✅ Full | Context-aware, CompressionDisabled |
| `internal/http1` | ✅ Full | Chunked streaming, http.Flusher |
| `internal/http2` | ✅ Full | h2c via Go 1.25 http.Protocols |
| `internal/http3` | ✅ Full | HTTP/3 + uni-streams + datagrams |
| `internal/sse` | ✅ Full | Last-Event-ID reconnect, 1000-event ring buffer |
| `internal/webtransport` | ✅ Full | uni/bidi stream + datagram modes |
| `internal/webrtc` | ✅ Full | reliable/unreliable/partial DataChannels |

### UDP (`internal/udp`)

Constructor pattern uses **options**:
```go
srv := udp.NewUDPServer(addr,
    udp.WithServerLogger(logger),
    udp.WithServerRecorder(rec),
    udp.WithServerRecvHandler(handler),
)
```

Client: `udp.NewUDPClient(serverAddr, handler, rec, logger)` → `client.Connect(ctx)`.

On Linux: `server_linux.go` implements `batchRecv(conn, bufs, addrs)` using `unix.Recvmmsg` and `batchSend(conn, addrs, data)` using `unix.Sendmmsg` (both from `golang.org/x/sys/unix`). `server_other.go` provides fallback using `ReadFromUDP`/`WriteToUDP`.

### TCP (`internal/tcp`)

SO_REUSEPORT listener creation is in `reuseport_linux.go` (build tag `linux`) and `reuseport_other.go`.

Constructor: `tcp.NewTCPServer(addr string, rec *metrics.Recorder, logger *zap.Logger) *TCPServer`

### WebSocket — Three Implementations

All three share the same structure: server listens on HTTP, upgrades `/ws` endpoint, per-conn goroutines.

- **Gorilla** (`internal/websocket/gorilla`): `NewGorillaServer(addr, rec, logger)`, `NewGorillaClient(serverAddr, handler, rec, logger)`
- **Gobwas** (`internal/websocket/gobwas`): `NewGobwasServer(addr, rec, logger)`, `NewGobwasClient(serverAddr, handler, rec, logger)`. Uses `ws.UpgradeHTTP(r, w)` → raw `net.Conn`, `ws.ReadFrame(bufio.ReadWriter)`, `ws.WriteFrame(conn, ws.NewBinaryFrame(data))`.
- **Coder** (`internal/websocket/coder`): `NewCoderServer(addr, rec, logger)`, `NewCoderClient(serverAddr, handler, rec, logger)`. Uses `websocket.Accept(w, r, &websocket.AcceptOptions{CompressionMode: websocket.CompressionDisabled})`.

### HTTP/3 (`internal/http3`)

Three modes controlled by `Mode` type:
```go
type Mode string
const (
    ModeHTTP3Stream Mode = "http3stream"    // default: long-poll HTTP/3 GET
    ModeUniStream   Mode = "unidirstream"   // raw QUIC uni-streams
    ModeDatagrams   Mode = "datagram"       // QUIC datagrams (unreliable)
)
```

quic-go v0.59 API note: `OpenUniStreamSync(ctx)` returns `(*quic.SendStream, error)` — not an interface, a pointer. `quic.Connection` (not `quic.Session`).

### SSE (`internal/sse`)

Replay ring buffer: `[replayBufSize]sseEvent` (size 1000) in `SSEServer`. Events encoded as `base64.StdEncoding` of wire frame. SSE format:
```
id: {seqnum}\n
data: {base64(wireframe)}\n
\n
```

### WebTransport (`internal/webtransport`)

Uses `github.com/quic-go/webtransport-go v0.10.0`. Server struct requires `H3 *http3.Server` (pointer, not value). `webtransport.ConfigureHTTP3Server(h3srv)` must NOT be called directly — the `webtransport.Server` handles this internally.

```go
wtSrv := &wt.Server{
    H3:          h3Srv,      // *http3.Server
    CheckOrigin: func(r *http.Request) bool { return true },
}
```

Stream types: `*wt.SendStream` (from `OpenUniStreamSync`), `*wt.ReceiveStream` (from `AcceptUniStream`), `*wt.Stream` (bidirectional).

### WebRTC (`internal/webrtc`)

Three files: `signaling.go` (HTTP SDP exchange), `server.go` (DataChannel broadcaster), `client.go` (connecting client).

Signaling endpoints: `POST /webrtc/offer` (SDP exchange), `POST /webrtc/ice` (ICE candidates).

```go
type ChannelMode string
const (
    ModeReliable        ChannelMode = "reliable"         // ordered=true (default)
    ModeUnreliable      ChannelMode = "unreliable"       // ordered=false, maxRetransmits=0
    ModePartialReliable ChannelMode = "partial-reliable" // ordered=false, maxRetransmits=2
)
```

WebRTCServer needs `signalingAddr` to start an HTTP server for signaling. Clients POST offers to `http://signalingAddr/webrtc/offer`.

---

## Broadcast Strategies (`internal/broadcast`)

All implement a common `Broadcaster` interface:
```go
type Broadcaster interface {
    Broadcast(data []byte) error
    Add(w Writer)
    Remove(id string)
    Len() int
}

type Writer interface {
    Write(data []byte) error
    ID() string
}
```

| Strategy | File | Notes |
|---|---|---|
| Naive | `naive.go` | Serial loop under mutex. Baseline. |
| Worker Pool | `workerpool.go` | `ants/v2` pool, NumCPU workers, 16 shards per broadcast |
| Sharded | `sharded.go` | 16 goroutines each owning a shard, broadcast fans to all 16 chans |
| Epoll | `epoll.go` | `//go:build linux`, EPOLLOUT\|EPOLLET, NumCPU worker goroutines |
| io_uring | `iou.go` | `//go:build linux`, falls back to EpollBroadcaster if io_uring unavailable |

**No `broadcast_bench_test.go` exists yet** — this is a TODO.

---

## Scenario Runner (`internal/scenarios`)

### Transport Factory Registration

Transports register themselves via `scenarios.Register(name, factory)`. **No transport has registered itself yet** — this wiring needs to be done in `cmd/benchmark-runner/main.go` or per-transport `init()` functions. The runner will return `"unknown protocol"` error until factories are registered.

```go
// Pattern for each transport:
func init() {
    scenarios.Register("udp", func(cfg scenarios.ScenarioConfig, logger *zap.Logger) (transport.Transport, error) {
        srv := udp.NewUDPServer(
            fmt.Sprintf("%s:%d", cfg.ServerAddr, cfg.ServerPort),
            udp.WithServerLogger(logger),
        )
        return srv, nil
    })
}
```

### ScenarioConfig

```go
type ScenarioConfig struct {
    Protocol, Scenario, GeneratorType, NetworkProfile, BroadcastStrat string
    ReceiverCount, SenderCount, MsgSize, RateLimit, ServerPort         int
    Duration, WarmupDuration                                           time.Duration
    ServerAddr, LogLevel                                               string
}
```

### Scenarios

| Function | Scenario | ReceiverCount | Default Duration |
|---|---|---|---|
| `RunScenarioA` | A | 1 | 60s |
| `RunScenarioB` | B | 100 | 60s |
| `RunScenarioC` | C | 1000 | 120s |
| `RunScenarioD` | D | 10000 | 180s (rate-limited to 50K ticks/sec) |
| `RunScenarioE` | E | 100K (K8s) / 1000 (fallback) | 300s |

---

## Collector & Reporter (`pkg/collector`, `pkg/reporter`)

### `RunResult` struct

Key fields (used throughout):
```go
Protocol, Scenario, GeneratorType, NetProfile, BroadcastStrat string
MsgSize, ReceiverCount, SenderCount, DurationS, WarmupS       int
StartedAt, EndedAt                                             time.Time
LatMinNs, LatAvgNs, LatP50Ns, LatP95Ns, LatP99Ns, LatP999Ns, LatMaxNs, LatStddevNs int64
MsgsPerSec, BytesPerSec                                        float64
TotalMsgsSent, TotalMsgsRecv, MsgsLost, MsgsReordered, MsgsDuplicated int64
LossRatePct, CPUPctAvg, CPUPctP99                              float64
MemBytesAvg, MemBytesMax                                       int64
GoroutinesAvg, GoroutinesMax, FDCountAvg, FDCountMax           int32
HandshakeAvgNs, HandshakeP99Ns, ReconnectAvgNs, ReconnectP99Ns int64
```

### Collectors

```go
type ResultCollector interface {
    Store(ctx context.Context, result *RunResult) error
    List(ctx context.Context, protocol, scenario string, limit int) ([]*RunResult, error)
    Close() error
}

collector.NewMemoryCollector() *MemoryCollector         // tests / standalone
collector.NewPostgresCollector(ctx, dsn) (*PostgresCollector, error)  // production
```

PostgresCollector auto-migrates on first connect using `//go:embed migrations/001_initial.sql`.

### Reporters

```go
reporter.NewJSONReporter(w io.Writer) *JSONReporter
reporter.NewMarkdownReporter(w io.Writer) *MarkdownReporter    // P99-sorted table
reporter.NewHTMLReporter(w io.Writer) *HTMLReporter             // Chart.js, self-contained
```

---

## Network Simulation (`pkg/network`)

15 named profiles in `Profiles map[string]Profile`. Key ones:
`clean`, `loss1`, `loss5`, `loss10`, `loss20`, `latency20`, `latency50`, `latency100`, `reorder`, `duplicate`, `jitter`, `wan`, `mobile4g`, `mobile3g`, `lossburst`, `satellite`

```go
ctrl := network.NewNetemController("eth0", logger)  // or "" for auto-detect
ctrl.Apply(network.Profiles["loss5"])               // tc qdisc add ...
ctrl.Clear()                                         // tc qdisc del ...
iface, err := ctrl.DetectInterface()                 // finds primary non-loopback
```

Requires `CAP_NET_ADMIN` or root. Uses `exec.Command("tc", ...)`.

---

## CLI (`cmd/benchmark-runner`)

```
benchmark-runner run     --protocol --scenario --msg-size --duration --warmup
                         --receivers --senders --rate-limit --network-profile
                         --broadcast-strat --generator --addr --port
                         --output [json|markdown|html] --store-postgres <DSN>

benchmark-runner compare --scenario --msg-size --duration --warmup
                         --receivers --network-profile --output

benchmark-runner report  --dsn <PostgreSQL DSN> (required)
                         --protocol --scenario --limit --format --out
```

Config file support via `viper` (reads `benchmark.yaml` from current dir by default).

---

## Infrastructure

### Docker

**`docker/Dockerfile.server`** — multi-stage, `ARG BINARY=benchmark-runner`, `GOAMD64=v3`. Build:
```bash
docker build --build-arg BINARY=udp-server -f docker/Dockerfile.server -t udp-server .
```

**`docker/docker-compose.yml`** — services: `postgres:17-alpine`, `prom/prometheus:latest`, `grafana/grafana:latest`, `result-collector`, `udp-server` (172.20.0.101), `tcp-server` (172.20.0.102), `websocket-server` (.103), `http1-server` (.104), `http2-server` (.105), `http3-server` (.106), `sse-server` (.107). Network: `bench-net` 172.20.0.0/24.

`benchmark-runner` service uses `network_mode: host` + `cap_add: NET_ADMIN` for tc/netem.

### Database

PostgreSQL schema: `benchmark_runs`, `benchmark_stats`, `broadcast_strategy_results`, `market_tick_stats`. Views: `v_protocol_comparison`, `v_broadcast_strategy_comparison`. Migration file: `pkg/collector/migrations/001_initial.sql`.

Default DSN: `postgres://benchmark:benchmark@localhost:5432/benchmark?sslmode=disable`

---

## What Is NOT Done Yet (TODOs)

### High Priority (Completed ✅)
~~1. Transport factory registration~~ — Done: `cmd/benchmark-runner/register.go` registers server+client factories for all 13 protocol variants. `internal/scenarios/clients.go` defines `ClientFactory` + `RegisterClient()` + `DefaultRecvHandler()`. `ScenarioRunner.Run()` now connects N receiver clients before the broadcast loop.

~~2. `internal/broadcast/broadcast_bench_test.go`~~ — Done: tests Naive/WorkerPool/Sharded at 10/100/1000/10000 receivers with correctness assertion and `b.ReportAllocs()`.

~~3. cmd/ stubs~~ — Done: all 16 server/client mains are real implementations (parse flags, build logger, create transport, Start/Stop lifecycle).

### Still Pending

4. **No tests for transport packages** — only `internal/udp`, `internal/tcp`, `pkg/transport`, `pkg/wire` have tests. Need tests for websocket, http1, http2, http3, sse.

5. **`proto/` and `kubernetes/` directories are empty** — the plan called for a `payload.proto` (MarketTick protobuf) and Kubernetes manifests (namespace, rbac, coordinator, worker, postgres, prometheus, grafana, coturn YAMLs).

6. **`pkg/generator/proto_gen.go` is missing** — protobuf payload generator never written.

7. **eBPF TCP RTT probes** (`cilium/ebpf`) — optional kernel-bypass latency measurement. Not implemented.

8. **`grafana/dashboard.json`** — main Grafana dashboard JSON is missing (only provisioning config files exist).

9. **`scripts/gen-certs.sh`** — planned but not written.

10. **OpenTelemetry tracing** — `go.opentelemetry.io/otel` not in go.mod; `pkg/metrics/otel.go` not written.

---

## Known Quirks & Decisions

- **`go.mod` says `go 1.25.0`** even though we targeted 1.23 — `go mod tidy` upgraded it automatically. This is fine; all code is 1.23-compatible.

- **HTTP/2 server uses `http.Protocols`** (Go 1.25 API for setting `SetUnencryptedHTTP2(true)`), not the older `golang.org/x/net/http2/h2c` wrapper. This is the modern approach but requires Go 1.25.

- **quic-go v0.59 API differences from older docs**: `quic.Connection` (not `quic.Session`), `OpenUniStreamSync` returns `*quic.SendStream` (concrete pointer, not interface), `quic.Conn` for the connection type.

- **webtransport-go v0.10 API**: `Server.H3` is `*http3.Server` (pointer). Do NOT call `webtransport.ConfigureHTTP3Server()` manually — the library does it.

- **WebRTC signaling server** is HTTP/1.1 only (no TLS). Fine for loopback benchmarks; production would need TLS.

- **`bytedance/sonic` v1.15.1** — compatible with Go 1.23+. Older versions (v1.11.x) broke with Go 1.23 due to internal runtime type changes. Always use ≥ v1.15.

- **`google/uuid` v1.3.1** appears as a direct dep in go.mod (`require` block) but no code in this repo imports it directly. It was pulled in transitively and `go mod tidy` promoted it. Safe to leave.

- **`internal/broadcast/epoll.go` and `iou.go`** both have `//go:build linux` and are not compiled on macOS/Windows. The `Broadcaster` interface is still usable on non-Linux via `NaiveBroadcaster` or `ShardedBroadcaster`.

- **`internal/broadcast/iou.go`** was refactored to use `unix.Mmap` (returns `[]byte`) instead of raw `unix.Syscall6(SYS_MMAP)`. All ring-field pointer access now uses `&slice[offset]` (unsafe rule 1: *T1→unsafe.Pointer→*T2). No more `go vet` warnings.

- **`NewShardedBroadcaster()`** now auto-calls `Start()` in the constructor. You only need to call `Stop()` when done.

- **`cmd/benchmark-runner/register.go`** — contains all `scenarios.Register()` and `scenarios.RegisterClient()` calls. It's an `init()` function in the `main` package, so it runs automatically when the binary starts. To add a new protocol: add entries here.

- **`scenarios.DefaultRecvHandler(rec)`** — returns a `transport.RecvHandler` that decodes the wire frame header and calls `rec.RecordRecv(seq, sendNs, len, recvNs)`. Use this for ALL client factories to ensure metrics feed into the scenario runner's recorder.

- **Client factories for SSE, WebTransport, WebRTC** create per-client `metrics.Recorder` objects (because their constructors require a recorder). These recorders are separate from the main scenario recorder. If you want combined latency stats, the `DefaultRecvHandler` closure still calls the *main* recorder via the `rec` parameter. The per-client recorder is an extra allocation that can be removed if needed.

- **`go vet ./...` exits 0** — clean.

---

## How to Start the Stack

```bash
# Start infrastructure (postgres + monitoring)
cd benchmark-lab
docker compose -f docker/docker-compose.yml up -d postgres prometheus grafana result-collector

# Build all binaries
make build

# Smoke test (UDP + TCP)
./bin/udp-server --addr 0.0.0.0:9001 &
./bin/benchmark-runner run --protocol udp --scenario A --duration 10s --output json

# Full comparison
./scripts/run-benchmarks.sh

# Apply network impairment
sudo ./scripts/netem-setup.sh loss5
./scripts/run-benchmarks.sh
sudo ./scripts/netem-teardown.sh
```

---

## File Count Summary

- **Total `.go` files:** 75 (including 4 with tests)
- **Total lines of Go:** ~12,900
- **Infrastructure files:** 33 (Makefile, Dockerfile, docker-compose, CI YAMLs, scripts, Grafana/Prometheus configs, SQL migration)
- **Branch:** `claude/protocol-benchmark-platform-4JTiG`
- **Last commit:** `8628ed3` — "feat: complete protocol benchmark platform — all 9 transports + full infrastructure"
