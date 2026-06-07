package db

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// testDSN returns the integration-test DSN or skips the test when none is
// configured. It only honours TEST_DATABASE_DSN (never the app's DATABASE_DSN)
// so a developer/CI DATABASE_DSN pointing at a real database can never be hit
// by the destructive schema-creating tests.
func testDSN(t *testing.T) string {
	t.Helper()
	if v := os.Getenv("TEST_DATABASE_DSN"); v != "" {
		return v
	}
	t.Skip("set TEST_DATABASE_DSN to run PostgreSQL integration tests")
	return ""
}

// newSchemaPool creates a throwaway schema, returns a pool whose connections
// are pinned to it, and registers cleanup that drops the schema afterwards.
// This gives every test full isolation and lets tests run in parallel.
func newSchemaPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := testDSN(t)
	ctx := context.Background()

	schema := fmt.Sprintf("mdtest_%d_%d", time.Now().UnixNano(), rand.Intn(1_000_000))
	ident := pgx.Identifier{schema}.Sanitize()

	boot, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("bootstrap pool: %v", err)
	}
	if _, err := boot.Exec(ctx, "CREATE SCHEMA "+ident); err != nil {
		boot.Close()
		t.Fatalf("create schema: %v", err)
	}
	boot.Close()

	pool, err := NewPool(ctx, PoolConfig{
		DSN:            dsn,
		MaxConns:       4,
		SearchPath:     schema,
		ConnectTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("schema pool: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
		cleanup, err := pgxpool.New(ctx, dsn)
		if err != nil {
			return
		}
		defer cleanup.Close()
		_, _ = cleanup.Exec(ctx, "DROP SCHEMA IF EXISTS "+ident+" CASCADE")
	})
	return pool
}

func tableExists(t *testing.T, pool *pgxpool.Pool, name string) bool {
	t.Helper()
	var reg *string
	if err := pool.QueryRow(context.Background(), "SELECT to_regclass($1)", name).Scan(&reg); err != nil {
		t.Fatalf("to_regclass(%s): %v", name, err)
	}
	return reg != nil
}

func isSQLState(err error, code string) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == code
	}
	return false
}

// isUniqueViolation reports a 23505 unique_violation.
func isUniqueViolation(err error) bool { return isSQLState(err, "23505") }

// isCheckViolation reports a 23514 check_violation.
func isCheckViolation(err error) bool { return isSQLState(err, "23514") }
