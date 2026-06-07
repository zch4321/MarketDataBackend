package api

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"MarketDataBackend/internal/db"
	"MarketDataBackend/internal/metadata"
	"MarketDataBackend/internal/model"
	"MarketDataBackend/migrations"
)

// testDSN returns the integration-test DSN or skips when none is configured.
// Only TEST_DATABASE_DSN is honoured so a real DATABASE_DSN is never used by
// these schema-creating tests.
func testDSN(t *testing.T) string {
	t.Helper()
	if v := os.Getenv("TEST_DATABASE_DSN"); v != "" {
		return v
	}
	t.Skip("set TEST_DATABASE_DSN to run PostgreSQL integration tests")
	return ""
}

// newPGStore builds an isolated-schema PostgresStore for integration tests.
func newPGStore(t *testing.T) (*metadata.PostgresStore, *pgxpool.Pool) {
	t.Helper()
	dsn := testDSN(t)
	ctx := context.Background()

	schema := fmt.Sprintf("mdapi_%d_%d", time.Now().UnixNano(), rand.Intn(1_000_000))
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
		DSN: dsn, MaxConns: 6, SearchPath: schema, ConnectTimeout: 5 * time.Second,
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

// TestAPIIntegrationCreateAndAggregate verifies the API writes are readable by
// the store and that GET aggregates lease + runtime status correctly.
func TestAPIIntegrationCreateAndAggregate(t *testing.T) {
	store, _ := newPGStore(t)
	h := New(store, nil, nil).Handler()
	ctx := context.Background()
	gid := "binance:spot:BTCUSDT"

	// 1. Create via the API.
	if rec := do(t, h, http.MethodPost, "/groups", validCreateBody("BTCUSDT")); rec.Code != http.StatusCreated {
		t.Fatalf("create: status = %d (%s)", rec.Code, rec.Body.String())
	}

	// 2. API-written data is readable directly through the MetadataStore.
	inputs, err := store.ListGroupInputs(ctx, gid)
	if err != nil {
		t.Fatalf("ListGroupInputs: %v", err)
	}
	if len(inputs) != 2 {
		t.Fatalf("store inputs = %d, want 2", len(inputs))
	}

	// 3. Seed a lease and a runtime status through the store, then GET aggregates.
	if ok, err := store.TryAcquireGroupLease(ctx, gid, "node-a", 30*time.Second); err != nil || !ok {
		t.Fatalf("acquire lease: ok=%v err=%v", ok, err)
	}
	lag := int64(99)
	if err := store.ReportStreamRuntimeStatus(ctx, model.StreamRuntimeStatus{
		InputID:      model.NewInputID(gid, "trade"),
		GroupID:      gid,
		StreamKey:    "trade",
		NodeID:       "node-a",
		ActualStatus: model.ActualStatusRunning,
		KafkaLag:     &lag,
	}); err != nil {
		t.Fatalf("ReportStreamRuntimeStatus: %v", err)
	}

	rec := do(t, h, http.MethodGet, "/groups/"+gid, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("get: status = %d", rec.Code)
	}
	g := decode[groupResponse](t, rec)
	if g.Lease == nil || g.Lease.NodeID != "node-a" {
		t.Fatalf("lease not aggregated: %+v", g.Lease)
	}

	var trade, kline *inputResponse
	for i := range g.Inputs {
		switch g.Inputs[i].StreamKey {
		case "trade":
			trade = &g.Inputs[i]
		case "kline_1m":
			kline = &g.Inputs[i]
		}
	}
	if trade == nil || trade.Runtime == nil || trade.Runtime.ActualStatus != model.ActualStatusRunning {
		t.Fatalf("trade runtime not aggregated: %+v", trade)
	}
	// No status reported for kline -> pending default.
	if kline == nil || kline.Runtime == nil || kline.Runtime.ActualStatus != model.ActualStatusPending {
		t.Fatalf("kline runtime = %+v, want pending", kline)
	}
}

// TestAPIIntegrationPauseResume verifies desired_status changes round-trip
// through PostgreSQL.
func TestAPIIntegrationPauseResume(t *testing.T) {
	store, _ := newPGStore(t)
	h := New(store, nil, nil).Handler()
	ctx := context.Background()
	gid := "binance:spot:BTCUSDT"

	if rec := do(t, h, http.MethodPost, "/groups", validCreateBody("BTCUSDT")); rec.Code != http.StatusCreated {
		t.Fatalf("create: %d", rec.Code)
	}

	if rec := do(t, h, http.MethodPost, "/groups/"+gid+"/pause", nil); rec.Code != http.StatusOK {
		t.Fatalf("pause group: %d", rec.Code)
	}
	g, err := store.GetGroup(ctx, gid)
	if err != nil {
		t.Fatalf("GetGroup: %v", err)
	}
	if g.DesiredStatus != model.DesiredStatusPaused {
		t.Fatalf("group desired_status = %s, want paused", g.DesiredStatus)
	}

	if rec := do(t, h, http.MethodPost, "/groups/"+gid+"/inputs/trade/pause", nil); rec.Code != http.StatusOK {
		t.Fatalf("pause input: %d", rec.Code)
	}
	inputs, err := store.ListGroupInputs(ctx, gid)
	if err != nil {
		t.Fatalf("ListGroupInputs: %v", err)
	}
	for _, in := range inputs {
		want := model.DesiredStatusRunning
		if in.StreamKey == "trade" {
			want = model.DesiredStatusPaused
		}
		if in.DesiredStatus != want {
			t.Errorf("input %s desired_status = %s, want %s", in.StreamKey, in.DesiredStatus, want)
		}
	}
}
