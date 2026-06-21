# Benchmark Lab

Protocol benchmark platform for real-time market data distribution. Compares UDP, TCP, WebSocket, HTTP/1, HTTP/2, HTTP/3, SSE, WebTransport, and WebRTC across configurable scenarios.

> **Methodology, per-protocol analysis, Grafana, and k6 validation:** see
> [`BENCHMARKING.md`](./BENCHMARKING.md) — it explains how fairness is guaranteed,
> what every metric means, and *why each protocol behaves the way it does*.

## Prerequisites

- Go 1.22+
- (Optional) Docker + Docker Compose — for Postgres, Prometheus, Grafana

## Quick Start

```powershell
# From benchmark-lab/
go run .\cmd\benchmark-runner\ run --protocol udp --scenario A --duration 10s
```

### With Live Web Dashboard

Add `--ui` to open a browser dashboard that shows metrics updating in real-time:

```powershell
go run .\cmd\benchmark-runner\ run --protocol udp --duration 30s --ui
```

Then open **http://localhost:7070** in your browser. You will see:
- Progress bar (warmup → measuring phases)
- Live throughput and P99 latency charts
- Key metrics updating every 500ms

Use `--ui-port` to change the port (default 7070).

## Run Commands

### Single Protocol

```powershell
go run .\cmd\benchmark-runner\ run --protocol <protocol> --scenario <A|B|C|D|E> --duration 10s --output markdown
```

Examples:

```powershell
# UDP, 10 seconds, live dashboard
go run .\cmd\benchmark-runner\ run --protocol udp --scenario A --duration 10s --ui

# TCP with 10 receivers, markdown output
go run .\cmd\benchmark-runner\ run --protocol tcp --scenario B --receivers 10 --duration 30s --output markdown

# WebSocket, HTML report saved to file
go run .\cmd\benchmark-runner\ run --protocol websocket-gorilla --scenario A --duration 20s --output html | Out-File results.html
```

### Compare All Protocols

Runs every transport sequentially and prints a side-by-side table:

```powershell
go run .\cmd\benchmark-runner\ compare --scenario A --duration 15s --receivers 5 --output markdown
```

### Generate Report from Postgres

```powershell
go run .\cmd\benchmark-runner\ report --dsn "postgres://user:pass@localhost:5432/benchmarks" --format markdown
```

## Flags Reference

### `run`

| Flag | Default | Description |
|---|---|---|
| `--protocol`, `-p` | `udp` | Transport to benchmark |
| `--scenario`, `-s` | `A` | Scenario A–E |
| `--duration` | `60s` | Measurement window |
| `--warmup` | `5s` | Warmup period (discarded) |
| `--receivers` | `1` | Number of subscriber connections |
| `--senders` | `1` | Number of publisher goroutines |
| `--msg-size` | `1024` | Payload size in bytes |
| `--rate-limit` | `0` | Max messages/sec (0 = flood) |
| `--broadcast-strat` | `naive` | `naive`, `sharded`, `workerpool` |
| `--generator` | `random` | `random`, `sequential`, `json`, `binary`, `market` |
| `--network-profile` | `clean` | `clean`, `wan`, `lossy` |
| `--addr` | `127.0.0.1` | Server bind address |
| `--port` | `9000` | Server base port |
| `--output` | `json` | `json`, `markdown`, `html` |
| `--store-postgres` | — | PostgreSQL DSN to persist results |
| `--ui` | `false` | Enable live web dashboard at localhost |
| `--ui-port` | `7070` | Port for the live dashboard |
| `--metrics` | `false` | Expose Prometheus `/metrics` (for Grafana) |
| `--metrics-port` | `9190` | Port for the Prometheus metrics endpoint |

### `compare`

| Flag | Default | Description |
|---|---|---|
| `--scenario`, `-s` | `A` | Scenario to run across all protocols |
| `--duration` | `30s` | Duration per protocol |
| `--warmup` | `5s` | Warmup duration |
| `--receivers` | `10` | Subscriber count |
| `--msg-size` | `1024` | Payload size in bytes |
| `--network-profile` | `clean` | Network impairment profile |
| `--output` | `markdown` | `json`, `markdown`, `html` |

## Supported Protocols

| Protocol | Flag value | Windows |
|---|---|---|
| UDP | `udp` | Yes |
| TCP | `tcp` | Yes |
| WebSocket (Gorilla) | `websocket-gorilla` | Yes |
| WebSocket (Gobwas) | `websocket-gobwas` | Yes |
| WebSocket (Coder) | `websocket-coder` | Yes |
| HTTP/1.1 | `http1` | Yes |
| HTTP/2 | `http2` | Yes |
| Server-Sent Events | `sse` | Yes |
| HTTP/3 (QUIC) | `http3` | Limited |
| WebTransport | `webtransport` | Limited |
| WebRTC | `webrtc` | Limited |

## Scenarios

| Scenario | Description |
|---|---|
| A | Single publisher → N subscribers, fixed rate |
| B | Burst: publisher floods at max speed |
| C | Multiple publishers → N subscribers |
| D | Large payload broadcast |
| E | Mixed payload sizes, variable rate |

## Build Binaries

```powershell
# From benchmark-lab/
New-Item -ItemType Directory -Force bin | Out-Null
go build -o bin\benchmark-runner.exe .\cmd\benchmark-runner\
go build -o bin\udp-server.exe       .\cmd\udp-server\
go build -o bin\udp-client.exe       .\cmd\udp-client\
# ... repeat for other server/client pairs
```

## Docker Stack (Postgres + Prometheus + Grafana)

```powershell
docker compose -f docker\docker-compose.yml up -d
```

Then run benchmarks with `--store-postgres` to persist results, and open Grafana at `http://localhost:3000`.

## Tests

```powershell
go test ./...
```
