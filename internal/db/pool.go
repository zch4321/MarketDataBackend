// Package db owns the PostgreSQL connection pool and the embedded SQL migration
// runner.
package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PoolConfig is the subset of configuration needed to build a pgx pool.
type PoolConfig struct {
	DSN            string
	MaxConns       int32
	MinConns       int32
	ConnectTimeout time.Duration
	// SearchPath, when set, pins every connection in the pool to a schema.
	// Used by integration tests for isolation; empty means default.
	SearchPath string
}

// NewPool parses the DSN, builds a pgxpool and verifies connectivity with a
// bounded Ping. An invalid DSN fails fast without any network access.
func NewPool(ctx context.Context, cfg PoolConfig) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("db: parse dsn: %w", err)
	}
	if cfg.MaxConns > 0 {
		poolCfg.MaxConns = cfg.MaxConns
	}
	if cfg.MinConns > 0 {
		poolCfg.MinConns = cfg.MinConns
	}
	if cfg.SearchPath != "" {
		if poolCfg.ConnConfig.RuntimeParams == nil {
			poolCfg.ConnConfig.RuntimeParams = map[string]string{}
		}
		poolCfg.ConnConfig.RuntimeParams["search_path"] = cfg.SearchPath
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("db: create pool: %w", err)
	}

	timeout := cfg.ConnectTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	pingCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("db: ping: %w", err)
	}
	return pool, nil
}
