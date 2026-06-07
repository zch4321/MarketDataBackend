package metadata

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"MarketDataBackend/internal/db"
	"MarketDataBackend/migrations"
)

// testDSN returns the integration-test DSN or skips the test when none is
// configured. Only TEST_DATABASE_DSN is honoured so a real DATABASE_DSN is
// never used by these schema-creating tests.
func testDSN(t *testing.T) string {
	t.Helper()
	if v := os.Getenv("TEST_DATABASE_DSN"); v != "" {
		return v
	}
	t.Skip("set TEST_DATABASE_DSN to run PostgreSQL integration tests")
	return ""
}

// newStore creates an isolated schema, applies all migrations and returns a
// PostgresStore pinned to it. The schema is dropped on cleanup, so tests are
// fully isolated and may run in parallel.
func newStore(t *testing.T) (*PostgresStore, *pgxpool.Pool) {
	t.Helper()
	dsn := testDSN(t)
	ctx := context.Background()

	schema := fmt.Sprintf("mdmeta_%d_%d", time.Now().UnixNano(), rand.Intn(1_000_000))
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

	pool, err := db.NewPool(ctx, db.PoolConfig{
		DSN:            dsn,
		MaxConns:       6,
		SearchPath:     schema,
		ConnectTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("schema pool: %v", err)
	}

	migrator, err := db.NewMigrator(pool, migrations.FS)
	if err != nil {
		pool.Close()
		t.Fatalf("new migrator: %v", err)
	}
	if err := migrator.Up(ctx); err != nil {
		pool.Close()
		t.Fatalf("migrate up: %v", err)
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
	return NewPostgresStore(pool), pool
}
