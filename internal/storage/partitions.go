package storage

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const deltaPartitionLockName = "market-data-backend:orderbook-delta-partitions"

// DeltaPartitionConfig controls hourly order-book delta partition lifecycle.
type DeltaPartitionConfig struct {
	Retention           time.Duration
	MaintenanceInterval time.Duration
	PrecreateHorizon    time.Duration
}

// OrderBookDeltaPartitionManager pre-creates hourly partitions and removes
// fully expired partitions. Database time is authoritative for every run.
type OrderBookDeltaPartitionManager struct {
	pool   *pgxpool.Pool
	cfg    DeltaPartitionConfig
	logger *slog.Logger
}

func NewOrderBookDeltaPartitionManager(
	pool *pgxpool.Pool, cfg DeltaPartitionConfig, logger *slog.Logger,
) (*OrderBookDeltaPartitionManager, error) {
	if pool == nil {
		return nil, fmt.Errorf("storage: delta partition manager requires a pool")
	}
	if cfg.Retention <= 0 {
		return nil, fmt.Errorf("storage: delta retention must be > 0")
	}
	if cfg.MaintenanceInterval <= 0 {
		return nil, fmt.Errorf("storage: partition maintenance interval must be > 0")
	}
	if cfg.PrecreateHorizon < 0 {
		return nil, fmt.Errorf("storage: partition precreate horizon must be >= 0")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &OrderBookDeltaPartitionManager{pool: pool, cfg: cfg, logger: logger}, nil
}

// Maintain performs one advisory-lock-protected maintenance pass.
func (m *OrderBookDeltaPartitionManager) Maintain(ctx context.Context) error {
	conn, err := m.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("storage: acquire partition maintenance connection: %w", err)
	}
	defer conn.Release()

	var locked bool
	if err := conn.QueryRow(ctx,
		`SELECT pg_try_advisory_lock(hashtextextended($1, 0))`,
		deltaPartitionLockName,
	).Scan(&locked); err != nil {
		return fmt.Errorf("storage: acquire partition advisory lock: %w", err)
	}
	if !locked {
		return nil
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := conn.Exec(unlockCtx,
			`SELECT pg_advisory_unlock(hashtextextended($1, 0))`,
			deltaPartitionLockName,
		); err != nil {
			m.logger.Warn("release partition advisory lock failed", "err", err)
		}
	}()

	var now time.Time
	if err := conn.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return fmt.Errorf("storage: read database time for partition maintenance: %w", err)
	}
	return m.maintainAt(ctx, conn, now.UTC())
}

// Run executes maintenance at the configured cadence until ctx is canceled.
// A failed pass is logged and retried on the next tick.
func (m *OrderBookDeltaPartitionManager) Run(ctx context.Context) {
	ticker := time.NewTicker(m.cfg.MaintenanceInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.Maintain(ctx); err != nil && ctx.Err() == nil {
				m.logger.Error("orderbook delta partition maintenance failed", "err", err)
			}
		}
	}
}

func (m *OrderBookDeltaPartitionManager) maintainAt(
	ctx context.Context, conn *pgxpool.Conn, now time.Time,
) error {
	var schema string
	if err := conn.QueryRow(ctx, "SELECT current_schema()").Scan(&schema); err != nil {
		return fmt.Errorf("storage: read current schema: %w", err)
	}

	current := now.UTC().Truncate(time.Hour)
	horizon := now.Add(m.cfg.PrecreateHorizon)
	for start := current; start.Before(horizon) || start.Equal(current); start = start.Add(time.Hour) {
		if err := createDeltaPartition(ctx, conn, schema, start); err != nil {
			return err
		}
	}

	cutoff := now.Add(-m.cfg.Retention)
	rows, err := conn.Query(ctx,
		`SELECT partition_name
		   FROM orderbook_delta_partitions
		  WHERE range_end <= $1
		  ORDER BY range_end`,
		cutoff,
	)
	if err != nil {
		return fmt.Errorf("storage: list expired delta partitions: %w", err)
	}
	var expired []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return fmt.Errorf("storage: scan expired delta partition: %w", err)
		}
		expired = append(expired, name)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("storage: iterate expired delta partitions: %w", err)
	}
	rows.Close()

	for _, name := range expired {
		if err := dropDeltaPartition(ctx, conn, schema, name); err != nil {
			return err
		}
	}
	return nil
}

func createDeltaPartition(
	ctx context.Context, conn *pgxpool.Conn, schema string, start time.Time,
) error {
	start = start.UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)
	name := "orderbook_deltas_" + start.Format("20060102_15")
	child := pgx.Identifier{schema, name}.Sanitize()
	parent := pgx.Identifier{schema, "orderbook_deltas"}.Sanitize()
	ddl := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s PARTITION OF %s FOR VALUES FROM ('%s') TO ('%s')",
		child, parent, start.Format(time.RFC3339), end.Format(time.RFC3339),
	)
	if _, err := conn.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("storage: create delta partition %s: %w", name, err)
	}
	if _, err := conn.Exec(ctx,
		`INSERT INTO orderbook_delta_partitions
		    (partition_name, range_start, range_end)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (partition_name) DO UPDATE SET
		    range_start = EXCLUDED.range_start,
		    range_end = EXCLUDED.range_end`,
		name, start, end,
	); err != nil {
		return fmt.Errorf("storage: record delta partition %s: %w", name, err)
	}
	return nil
}

func dropDeltaPartition(
	ctx context.Context, conn *pgxpool.Conn, schema, name string,
) error {
	child := pgx.Identifier{schema, name}.Sanitize()
	parent := pgx.Identifier{schema, "orderbook_deltas"}.Sanitize()

	var attached bool
	if err := conn.QueryRow(ctx,
		`SELECT EXISTS (
		    SELECT 1
		      FROM pg_inherits i
		      JOIN pg_class child ON child.oid = i.inhrelid
		      JOIN pg_namespace ns ON ns.oid = child.relnamespace
		     WHERE ns.nspname = $1 AND child.relname = $2
		)`,
		schema, name,
	).Scan(&attached); err != nil {
		return fmt.Errorf("storage: inspect delta partition %s: %w", name, err)
	}
	if attached {
		if _, err := conn.Exec(ctx,
			fmt.Sprintf("ALTER TABLE %s DETACH PARTITION %s CONCURRENTLY", parent, child),
		); err != nil {
			return fmt.Errorf("storage: detach delta partition %s: %w", name, err)
		}
	}
	if _, err := conn.Exec(ctx, "DROP TABLE IF EXISTS "+child); err != nil {
		return fmt.Errorf("storage: drop delta partition %s: %w", name, err)
	}
	if _, err := conn.Exec(ctx,
		"DELETE FROM orderbook_delta_partitions WHERE partition_name = $1", name,
	); err != nil {
		return fmt.Errorf("storage: forget delta partition %s: %w", name, err)
	}
	return nil
}
