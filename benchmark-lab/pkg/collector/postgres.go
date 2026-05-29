package collector

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/001_initial.sql
var migrationSQL string

// PostgresCollector persists results to PostgreSQL using pgx/v5 batch writes.
type PostgresCollector struct {
	pool *pgxpool.Pool
}

// NewPostgresCollector creates a collector connected to the given DSN.
// The database schema is automatically migrated on first use.
func NewPostgresCollector(ctx context.Context, dsn string) (*PostgresCollector, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres collector: connect: %w", err)
	}
	c := &PostgresCollector{pool: pool}
	if err := c.RunMigrations(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres collector: migrate: %w", err)
	}
	return c, nil
}

// RunMigrations executes the embedded SQL schema.
func (c *PostgresCollector) RunMigrations(ctx context.Context) error {
	_, err := c.pool.Exec(ctx, migrationSQL)
	return err
}

// Store writes a benchmark result to the database in a single transaction.
func (c *PostgresCollector) Store(ctx context.Context, r *RunResult) error {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	actualDuration := r.EndedAt.Sub(r.StartedAt)

	var runID string
	err = tx.QueryRow(ctx, `
		INSERT INTO benchmark_runs (
			protocol, scenario, msg_size, generator_type, net_profile, broadcast_strat,
			receiver_count, sender_count, duration_s, warmup_s,
			started_at, ended_at, actual_duration,
			go_version, os_arch, cpu_model, config
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17
		) RETURNING id`,
		r.Protocol, r.Scenario, r.MsgSize, r.GeneratorType, r.NetProfile, r.BroadcastStrat,
		r.ReceiverCount, r.SenderCount, r.DurationS, r.WarmupS,
		r.StartedAt, r.EndedAt, actualDuration,
		r.GoVersion, r.OSArch, r.CPUModel, r.Config,
	).Scan(&runID)
	if err != nil {
		return fmt.Errorf("postgres collector: insert run: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO benchmark_stats (
			run_id,
			lat_min_ns, lat_avg_ns, lat_p50_ns, lat_p95_ns, lat_p99_ns, lat_p999_ns, lat_max_ns, lat_stddev_ns,
			msgs_per_sec, bytes_per_sec, total_msgs_sent, total_msgs_recv,
			msgs_lost, msgs_duplicated, msgs_reordered, loss_rate_pct,
			cpu_pct_avg, cpu_pct_p99, mem_bytes_avg, mem_bytes_max,
			goroutines_avg, goroutines_max, fd_count_avg, fd_count_max,
			handshake_avg_ns, handshake_p99_ns, reconnect_avg_ns, reconnect_p99_ns
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29
		)`,
		runID,
		r.LatMinNs, r.LatAvgNs, r.LatP50Ns, r.LatP95Ns, r.LatP99Ns, r.LatP999Ns, r.LatMaxNs, r.LatStddevNs,
		r.MsgsPerSec, r.BytesPerSec, r.TotalMsgsSent, r.TotalMsgsRecv,
		r.MsgsLost, r.MsgsDuplicated, r.MsgsReordered, r.LossRatePct,
		r.CPUPctAvg, r.CPUPctP99, r.MemBytesAvg, r.MemBytesMax,
		r.GoroutinesAvg, r.GoroutinesMax, r.FDCountAvg, r.FDCountMax,
		r.HandshakeAvgNs, r.HandshakeP99Ns, r.ReconnectAvgNs, r.ReconnectP99Ns,
	)
	if err != nil {
		return fmt.Errorf("postgres collector: insert stats: %w", err)
	}

	return tx.Commit(ctx)
}

// List returns up to limit results for the given protocol+scenario, newest first.
func (c *PostgresCollector) List(ctx context.Context, protocol, scenario string, limit int) ([]*RunResult, error) {
	if limit <= 0 {
		limit = 100
	}
	args := []any{limit}
	where := ""
	argIdx := 2
	if protocol != "" {
		where += fmt.Sprintf(" AND r.protocol = $%d", argIdx)
		args = append(args, protocol)
		argIdx++
	}
	if scenario != "" {
		where += fmt.Sprintf(" AND r.scenario = $%d", argIdx)
		args = append(args, scenario)
		argIdx++
	}

	query := fmt.Sprintf(`
		SELECT
			r.id, r.protocol, r.scenario, r.msg_size, r.generator_type, r.net_profile, r.broadcast_strat,
			r.receiver_count, r.sender_count, r.duration_s, r.warmup_s,
			r.started_at, r.ended_at, r.go_version, r.os_arch, r.cpu_model,
			s.lat_min_ns, s.lat_avg_ns, s.lat_p50_ns, s.lat_p95_ns, s.lat_p99_ns, s.lat_p999_ns, s.lat_max_ns, s.lat_stddev_ns,
			s.msgs_per_sec, s.bytes_per_sec, s.total_msgs_sent, s.total_msgs_recv,
			s.msgs_lost, s.msgs_duplicated, s.msgs_reordered, s.loss_rate_pct,
			s.cpu_pct_avg, s.cpu_pct_p99, s.mem_bytes_avg, s.mem_bytes_max,
			s.goroutines_avg, s.goroutines_max, s.fd_count_avg, s.fd_count_max,
			s.handshake_avg_ns, s.handshake_p99_ns, s.reconnect_avg_ns, s.reconnect_p99_ns
		FROM benchmark_runs r
		JOIN benchmark_stats s ON s.run_id = r.id
		WHERE true %s
		ORDER BY r.started_at DESC
		LIMIT $1`, where)

	rows, err := c.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []*RunResult
	for rows.Next() {
		r := &RunResult{}
		var endedAt *time.Time
		err := rows.Scan(
			&r.RunID, &r.Protocol, &r.Scenario, &r.MsgSize, &r.GeneratorType, &r.NetProfile, &r.BroadcastStrat,
			&r.ReceiverCount, &r.SenderCount, &r.DurationS, &r.WarmupS,
			&r.StartedAt, &endedAt, &r.GoVersion, &r.OSArch, &r.CPUModel,
			&r.LatMinNs, &r.LatAvgNs, &r.LatP50Ns, &r.LatP95Ns, &r.LatP99Ns, &r.LatP999Ns, &r.LatMaxNs, &r.LatStddevNs,
			&r.MsgsPerSec, &r.BytesPerSec, &r.TotalMsgsSent, &r.TotalMsgsRecv,
			&r.MsgsLost, &r.MsgsDuplicated, &r.MsgsReordered, &r.LossRatePct,
			&r.CPUPctAvg, &r.CPUPctP99, &r.MemBytesAvg, &r.MemBytesMax,
			&r.GoroutinesAvg, &r.GoroutinesMax, &r.FDCountAvg, &r.FDCountMax,
			&r.HandshakeAvgNs, &r.HandshakeP99Ns, &r.ReconnectAvgNs, &r.ReconnectP99Ns,
		)
		if err != nil {
			return nil, err
		}
		if endedAt != nil {
			r.EndedAt = *endedAt
		}
		results = append(results, r)
	}
	return results, rows.Err()
}

func (c *PostgresCollector) Close() error {
	c.pool.Close()
	return nil
}
