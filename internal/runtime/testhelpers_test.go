package runtime

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"MarketDataBackend/internal/db"
	"MarketDataBackend/internal/metadata"
	"MarketDataBackend/internal/model"
	"MarketDataBackend/migrations"
)

// testDSN returns the integration-test DSN or skips. Only TEST_DATABASE_DSN is
// honoured so a real DATABASE_DSN is never used by these schema-creating tests.
func testDSN(t *testing.T) string {
	t.Helper()
	if v := os.Getenv("TEST_DATABASE_DSN"); v != "" {
		return v
	}
	t.Skip("set TEST_DATABASE_DSN to run PostgreSQL integration tests")
	return ""
}

// newStore creates an isolated schema, applies all migrations and returns a
// PostgresStore pinned to it plus the underlying pool. The schema is dropped on
// cleanup so tests are fully isolated and may run in parallel.
func newStore(t *testing.T) (*metadata.PostgresStore, *pgxpool.Pool) {
	t.Helper()
	dsn := testDSN(t)
	ctx := context.Background()

	schema := fmt.Sprintf("mdrt_%d_%d", time.Now().UnixNano(), rand.Intn(1_000_000))
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
		MaxConns:       8,
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
	return metadata.NewPostgresStore(pool), pool
}

// createRunnableGroup creates a running market group with a single trade input
// and returns its group_id.
func createRunnableGroup(t *testing.T, store metadata.Store, symbol string) string {
	t.Helper()
	g := model.MarketGroup{
		Exchange:   "binance",
		MarketType: "spot",
		Symbol:     symbol,
		Inputs: []model.GroupInput{
			{StreamKey: "trade", KafkaTopic: "md.trade." + symbol, KafkaGroupID: "rt", Enabled: true},
		},
	}
	if err := store.CreateGroup(context.Background(), g); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	return model.NewGroupID("binance", "spot", symbol)
}

// fastNodeConfig returns a node config with short cadences so lease/heartbeat
// behaviour is observable within a few seconds.
func fastNodeConfig(nodeID string) NodeConfig {
	return NodeConfig{
		NodeID:            nodeID,
		Hostname:          "test-host",
		MaxGroups:         10,
		MaxWeight:         100,
		LeaseTTL:          time.Second,
		RenewInterval:     200 * time.Millisecond,
		ReconcileInterval: 40 * time.Millisecond,
		HeartbeatInterval: 40 * time.Millisecond,
	}
}

// runNode starts a node loop in the background and returns the node plus an
// idempotent stop function (also registered as cleanup) that cancels it and
// waits for a clean shutdown.
func runNode(t *testing.T, store metadata.Store, cfg NodeConfig) (*Node, func()) {
	t.Helper()
	node := NewNode(store, cfg, discardLogger())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := node.Run(ctx); err != nil {
			t.Errorf("node %s Run: %v", cfg.NodeID, err)
		}
	}()

	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			<-done
		})
	}
	t.Cleanup(stop)
	return node, stop
}

// eventually polls cond until it returns true or the timeout elapses.
func eventually(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s: %s", timeout, msg)
}

func contains(ss []string, v string) bool {
	for _, s := range ss {
		if s == v {
			return true
		}
	}
	return false
}

// groupActualStatus returns the actual_status of the group's first input, or ""
// when nothing has been reported yet.
func groupActualStatus(t *testing.T, store metadata.Store, gid string) string {
	t.Helper()
	sts, err := store.ListStreamRuntimeStatus(context.Background(), gid)
	if err != nil {
		t.Fatalf("ListStreamRuntimeStatus: %v", err)
	}
	if len(sts) == 0 {
		return ""
	}
	return sts[0].ActualStatus
}

func nodeRegistered(t *testing.T, pool *pgxpool.Pool, nodeID string) bool {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(),
		"SELECT count(*) FROM runtime_nodes WHERE node_id = $1", nodeID).Scan(&n); err != nil {
		t.Fatalf("query runtime_nodes: %v", err)
	}
	return n == 1
}

func nodeHeartbeat(t *testing.T, pool *pgxpool.Pool, nodeID string) time.Time {
	t.Helper()
	var ts time.Time
	if err := pool.QueryRow(context.Background(),
		"SELECT last_heartbeat_at FROM runtime_nodes WHERE node_id = $1", nodeID).Scan(&ts); err != nil {
		t.Fatalf("query heartbeat: %v", err)
	}
	return ts
}
