package db

import (
	"context"
	"encoding/json"
	"testing"

	"MarketDataBackend/internal/model"
	"MarketDataBackend/migrations"
)

var allTables = []string{
	"market_groups",
	"group_inputs",
	"runtime_nodes",
	"market_leases",
	"stream_runtime_status",
	"trades",
	"klines",
	"orderbook_deltas",
	"orderbook_snapshots",
	"stream_write_progress",
	"orderbook_delta_partitions",
}

func TestMigrateUpCreatesAllTables(t *testing.T) {
	pool := newSchemaPool(t)
	ctx := context.Background()
	m, err := NewMigrator(pool, migrations.FS)
	if err != nil {
		t.Fatalf("NewMigrator: %v", err)
	}
	if err := m.Up(ctx); err != nil {
		t.Fatalf("Up: %v", err)
	}

	for _, tbl := range allTables {
		if !tableExists(t, pool, tbl) {
			t.Errorf("expected table %q to exist after Up", tbl)
		}
	}

	version, err := m.Version(ctx)
	if err != nil {
		t.Fatalf("Version: %v", err)
	}
	if version != 4 {
		t.Errorf("Version = %d, want 4", version)
	}

	// Up is idempotent: a second run applies nothing and keeps the version.
	if err := m.Up(ctx); err != nil {
		t.Fatalf("second Up: %v", err)
	}
}

func TestMigrateDownRollsBack(t *testing.T) {
	pool := newSchemaPool(t)
	ctx := context.Background()
	m, err := NewMigrator(pool, migrations.FS)
	if err != nil {
		t.Fatalf("NewMigrator: %v", err)
	}
	if err := m.Up(ctx); err != nil {
		t.Fatalf("Up: %v", err)
	}
	if err := m.Down(ctx); err != nil {
		t.Fatalf("Down: %v", err)
	}

	for _, tbl := range allTables {
		if tableExists(t, pool, tbl) {
			t.Errorf("expected table %q to be gone after Down", tbl)
		}
	}
	version, err := m.Version(ctx)
	if err != nil {
		t.Fatalf("Version: %v", err)
	}
	if version != 0 {
		t.Errorf("Version = %d, want 0 after Down", version)
	}
}

func TestUniqueConstraints(t *testing.T) {
	pool := newSchemaPool(t)
	ctx := context.Background()
	m, _ := NewMigrator(pool, migrations.FS)
	if err := m.Up(ctx); err != nil {
		t.Fatalf("Up: %v", err)
	}

	// market_groups: UNIQUE (exchange, market_type, symbol)
	insertGroup(t, pool, "g1", "binance", "spot", "BTCUSDT")
	if err := tryInsertGroup(ctx, pool, "g2", "binance", "spot", "BTCUSDT"); !isUniqueViolation(err) {
		t.Errorf("duplicate market group: got %v, want unique violation", err)
	}

	// group_inputs: UNIQUE (group_id, stream_key)
	insertInput(t, pool, "i1", "g1", "trade", model.StreamKindTrade)
	if err := tryInsertInput(ctx, pool, "i2", "g1", "trade", model.StreamKindTrade); !isUniqueViolation(err) {
		t.Errorf("duplicate group input: got %v, want unique violation", err)
	}

	// trades: UNIQUE (input_id, kafka_partition, kafka_offset)
	insertTrade(t, pool, "g1", "i1", "rt1", 0, 100)
	if err := tryInsertTrade(ctx, pool, "g1", "i1", "rt-other", 0, 100); !isUniqueViolation(err) {
		t.Errorf("duplicate kafka offset: got %v, want unique violation", err)
	}
	// trades: UNIQUE (input_id, raw_trade_id) WHERE raw_trade_id IS NOT NULL
	if err := tryInsertTrade(ctx, pool, "g1", "i1", "rt1", 0, 101); !isUniqueViolation(err) {
		t.Errorf("duplicate raw_trade_id: got %v, want unique violation", err)
	}
	// NULL raw_trade_id is allowed multiple times (partial unique index).
	insertTradeNullRaw(t, pool, "g1", "i1", 0, 200)
	insertTradeNullRaw(t, pool, "g1", "i1", 0, 201)

	// klines: UNIQUE (group_id, source, interval, open_time)
	insertKline(t, pool, "g1", "i1", "1m", "2026-06-06T00:00:00Z")
	if err := tryInsertKline(ctx, pool, "g1", "i1", "1m", "2026-06-06T00:00:00Z"); !isUniqueViolation(err) {
		t.Errorf("duplicate kline key: got %v, want unique violation", err)
	}

	// orderbook_snapshots: UNIQUE (input_id, snapshot_time)
	insertSnapshot(t, pool, "g1", "i1", "2026-06-06T00:00:00Z", `[]`, `[]`)
	if err := tryInsertSnapshot(ctx, pool, "g1", "i1", "2026-06-06T00:00:00Z", `[]`, `[]`); !isUniqueViolation(err) {
		t.Errorf("duplicate snapshot key: got %v, want unique violation", err)
	}
}

func TestCheckConstraints(t *testing.T) {
	pool := newSchemaPool(t)
	ctx := context.Background()
	m, _ := NewMigrator(pool, migrations.FS)
	if err := m.Up(ctx); err != nil {
		t.Fatalf("Up: %v", err)
	}

	// invalid desired_status on market_groups
	_, err := pool.Exec(ctx,
		`INSERT INTO market_groups (group_id, exchange, market_type, symbol, desired_status)
		 VALUES ('gbad','binance','spot','ETHUSDT','bogus')`)
	if !isCheckViolation(err) {
		t.Errorf("invalid desired_status: got %v, want check violation", err)
	}

	insertGroup(t, pool, "g1", "binance", "spot", "BTCUSDT")

	// invalid stream_kind on group_inputs
	_, err = pool.Exec(ctx,
		`INSERT INTO group_inputs (input_id, group_id, stream_key, stream_kind, kafka_topic, kafka_group_id, desired_status)
		 VALUES ('ibad','g1','weird','weird','t','gid','running')`)
	if !isCheckViolation(err) {
		t.Errorf("invalid stream_kind: got %v, want check violation", err)
	}

	// invalid trade side
	_, err = pool.Exec(ctx,
		`INSERT INTO trades (group_id, input_id, event_time, price, quantity, side, kafka_topic, kafka_partition, kafka_offset)
		 VALUES ('g1','i1', now(), 1, 1, 'sideways', 't', 0, 1)`)
	if !isCheckViolation(err) {
		t.Errorf("invalid trade side: got %v, want check violation", err)
	}
}

func TestJSONBRoundTrip(t *testing.T) {
	pool := newSchemaPool(t)
	ctx := context.Background()
	m, _ := NewMigrator(pool, migrations.FS)
	if err := m.Up(ctx); err != nil {
		t.Fatalf("Up: %v", err)
	}

	bids := `[{"price":"100.5","quantity":"2"},{"price":"100.4","quantity":"1"}]`
	asks := `[{"price":"100.6","quantity":"3"}]`
	insertSnapshot(t, pool, "g1", "iX", "2026-06-06T01:00:00Z", bids, asks)

	var gotBids, gotAsks []byte
	err := pool.QueryRow(ctx,
		`SELECT bids, asks FROM orderbook_snapshots WHERE input_id = 'iX' AND snapshot_time = '2026-06-06T01:00:00Z'::timestamptz`).
		Scan(&gotBids, &gotAsks)
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}

	var levels []model.PriceLevel
	if err := json.Unmarshal(gotBids, &levels); err != nil {
		t.Fatalf("unmarshal bids: %v", err)
	}
	if len(levels) != 2 || levels[0].Price != "100.5" || levels[0].Quantity != "2" {
		t.Errorf("bids round-trip mismatch: %+v", levels)
	}
	if err := json.Unmarshal(gotAsks, &levels); err != nil {
		t.Fatalf("unmarshal asks: %v", err)
	}
	if len(levels) != 1 || levels[0].Price != "100.6" {
		t.Errorf("asks round-trip mismatch: %+v", levels)
	}
}
