-- Protocol Benchmark Platform — PostgreSQL Schema
-- Migration 001: Initial schema
--
-- Tables:
--   benchmark_runs           — metadata for each benchmark run
--   benchmark_stats          — computed latency/throughput/reliability/resource stats per run
--   broadcast_strategy_results — comparison data for different broadcast implementations
--
-- Indexes are optimized for the primary query patterns:
--   - Latest runs by protocol and scenario (dashboard overview)
--   - Runs by time range (trend analysis)
--   - Stats by run_id (detail drill-down)

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ─── benchmark_runs ──────────────────────────────────────────────────────────
-- One row per benchmark scenario execution.
CREATE TABLE IF NOT EXISTS benchmark_runs (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- What was tested
    protocol        TEXT NOT NULL,     -- e.g., 'udp', 'tcp', 'websocket-gorilla'
    scenario        TEXT NOT NULL,     -- 'A' (1:1) | 'B' (1:100) | 'C' (1:1k) | 'D' (1:10k) | 'E' (distributed)
    msg_size        INTEGER NOT NULL,  -- payload size in bytes (not including wire header)
    generator_type  TEXT NOT NULL,     -- 'random' | 'sequential' | 'json' | 'proto' | 'binary' | 'market'
    net_profile     TEXT NOT NULL DEFAULT 'clean', -- network condition: 'clean' | 'loss1' | 'latency20ms' | ...
    broadcast_strat TEXT NOT NULL DEFAULT 'naive', -- fanout strategy: 'naive' | 'workerpool' | 'sharded' | 'epoll'

    -- Run parameters
    receiver_count  INTEGER NOT NULL DEFAULT 1,
    sender_count    INTEGER NOT NULL DEFAULT 1,
    duration_s      INTEGER NOT NULL,   -- configured run duration in seconds
    warmup_s        INTEGER NOT NULL DEFAULT 5,
    rate_limit      INTEGER,            -- max messages/sec; NULL = flood

    -- Timing
    started_at      TIMESTAMPTZ NOT NULL,
    ended_at        TIMESTAMPTZ,
    actual_duration INTERVAL,           -- computed from ended_at - started_at

    -- Environment
    go_version      TEXT,               -- e.g., 'go1.23.0'
    os_arch         TEXT,               -- e.g., 'linux/amd64'
    cpu_model       TEXT,               -- e.g., 'AMD Ryzen 9 7950X' or 'Intel Core i9-13900K'
    goamd64         TEXT,               -- e.g., 'v3'

    -- Raw configuration as JSON for reproducibility
    config          JSONB NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_runs_protocol_scenario
    ON benchmark_runs(protocol, scenario);
CREATE INDEX IF NOT EXISTS idx_runs_started_at
    ON benchmark_runs(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_runs_protocol_started
    ON benchmark_runs(protocol, started_at DESC);

-- ─── benchmark_stats ─────────────────────────────────────────────────────────
-- Aggregated statistics computed at end-of-run.
-- One row per benchmark_run.
CREATE TABLE IF NOT EXISTS benchmark_stats (
    id      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    run_id  UUID NOT NULL REFERENCES benchmark_runs(id) ON DELETE CASCADE,

    -- Latency (nanoseconds) from HDR histogram
    -- All latencies are end-to-end: from wire.SendNs to receiver's RecvNs
    lat_min_ns      BIGINT,
    lat_avg_ns      BIGINT,
    lat_p50_ns      BIGINT,
    lat_p95_ns      BIGINT,
    lat_p99_ns      BIGINT,
    lat_p999_ns     BIGINT,
    lat_max_ns      BIGINT,
    lat_stddev_ns   BIGINT,

    -- Throughput
    msgs_per_sec    DOUBLE PRECISION,
    bytes_per_sec   DOUBLE PRECISION,
    total_msgs_sent BIGINT,
    total_msgs_recv BIGINT,

    -- Reliability (from SequenceTracker)
    msgs_lost       BIGINT,
    msgs_duplicated BIGINT,
    msgs_reordered  BIGINT,
    loss_rate_pct   DOUBLE PRECISION, -- msgs_lost / (msgs_sent) * 100

    -- Server resource usage (averages over run duration)
    cpu_pct_avg     DOUBLE PRECISION,
    cpu_pct_p99     DOUBLE PRECISION,
    mem_bytes_avg   BIGINT,
    mem_bytes_max   BIGINT,
    goroutines_avg  INTEGER,
    goroutines_max  INTEGER,
    fd_count_avg    INTEGER,
    fd_count_max    INTEGER,

    -- Connection lifecycle (nanoseconds, from HDR histogram)
    handshake_avg_ns    BIGINT,
    handshake_p99_ns    BIGINT,
    reconnect_avg_ns    BIGINT,
    reconnect_p99_ns    BIGINT,

    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stats_run_id
    ON benchmark_stats(run_id);

-- ─── broadcast_strategy_results ──────────────────────────────────────────────
-- Per-strategy broadcast performance data.
-- Multiple rows per run (one per strategy tested).
CREATE TABLE IF NOT EXISTS broadcast_strategy_results (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    run_id          UUID NOT NULL REFERENCES benchmark_runs(id) ON DELETE CASCADE,

    strategy        TEXT NOT NULL,    -- 'naive' | 'workerpool' | 'sharded' | 'epoll' | 'iou'
    receiver_count  INTEGER NOT NULL,

    -- Time to complete one Broadcast() call (nanoseconds)
    broadcast_ns_avg    BIGINT,
    broadcast_ns_p99    BIGINT,
    broadcast_ns_max    BIGINT,

    -- Resource cost of broadcast
    cpu_pct_during      DOUBLE PRECISION,
    mem_bytes_during    BIGINT,
    allocs_per_op       INTEGER,     -- from Go benchmark b.ReportAllocs()

    -- Delivery completeness
    delivery_rate_pct   DOUBLE PRECISION, -- % of receivers that got each message

    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_broadcast_run_id
    ON broadcast_strategy_results(run_id);
CREATE INDEX IF NOT EXISTS idx_broadcast_strategy_receivers
    ON broadcast_strategy_results(strategy, receiver_count);

-- ─── market_tick_stats ───────────────────────────────────────────────────────
-- Per-instrument market data delivery statistics.
CREATE TABLE IF NOT EXISTS market_tick_stats (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    run_id          UUID NOT NULL REFERENCES benchmark_runs(id) ON DELETE CASCADE,

    symbol          TEXT NOT NULL,
    asset_class     TEXT NOT NULL,  -- 'equity' | 'future' | 'option' | 'crypto' | 'fx'
    tick_rate_hz    DOUBLE PRECISION NOT NULL,

    -- Delivery stats
    ticks_published BIGINT,
    ticks_delivered BIGINT,
    ticks_lost      BIGINT,

    -- Tick-to-trade latency (nanoseconds)
    lat_p50_ns      BIGINT,
    lat_p99_ns      BIGINT,
    lat_p999_ns     BIGINT,

    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_market_tick_run_id
    ON market_tick_stats(run_id);
CREATE INDEX IF NOT EXISTS idx_market_tick_symbol
    ON market_tick_stats(symbol);

-- ─── Views for Grafana ────────────────────────────────────────────────────────
-- These views are used by Grafana data source queries.

CREATE OR REPLACE VIEW v_protocol_comparison AS
SELECT
    r.protocol,
    r.scenario,
    r.msg_size,
    r.net_profile,
    AVG(s.lat_p50_ns)       AS avg_p50_ns,
    AVG(s.lat_p99_ns)       AS avg_p99_ns,
    AVG(s.lat_p999_ns)      AS avg_p999_ns,
    AVG(s.msgs_per_sec)     AS avg_msgs_per_sec,
    AVG(s.bytes_per_sec)    AS avg_bytes_per_sec,
    AVG(s.loss_rate_pct)    AS avg_loss_rate_pct,
    AVG(s.cpu_pct_avg)      AS avg_cpu_pct,
    AVG(s.mem_bytes_avg)    AS avg_mem_bytes,
    COUNT(*)                AS run_count,
    MAX(r.started_at)       AS latest_run
FROM benchmark_runs r
JOIN benchmark_stats s ON s.run_id = r.id
GROUP BY r.protocol, r.scenario, r.msg_size, r.net_profile;

CREATE OR REPLACE VIEW v_broadcast_strategy_comparison AS
SELECT
    r.protocol,
    b.strategy,
    b.receiver_count,
    AVG(b.broadcast_ns_avg)   AS avg_broadcast_ns,
    AVG(b.broadcast_ns_p99)   AS avg_broadcast_p99_ns,
    AVG(b.cpu_pct_during)     AS avg_cpu_pct,
    AVG(b.allocs_per_op)      AS avg_allocs_per_op,
    COUNT(*)                  AS sample_count
FROM broadcast_strategy_results b
JOIN benchmark_runs r ON r.id = b.run_id
GROUP BY r.protocol, b.strategy, b.receiver_count
ORDER BY b.receiver_count, b.strategy;
