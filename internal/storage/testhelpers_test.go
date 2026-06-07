package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"MarketDataBackend/internal/db"
	"MarketDataBackend/internal/model"
	"MarketDataBackend/migrations"
)

const (
	testGroupID = "binance:spot:BTCUSDT"
	testInputID = "binance:spot:BTCUSDT:trade"
)

// testDSN returns the integration-test DSN or skips the test when none is
// configured. Only TEST_DATABASE_DSN is honoured so a real DATABASE_DSN is
// never used by these schema-creating tests.
func testDSN(t testing.TB) string {
	t.Helper()
	if v := os.Getenv("TEST_DATABASE_DSN"); v != "" {
		return v
	}
	t.Skip("set TEST_DATABASE_DSN to run PostgreSQL integration tests")
	return ""
}

// newStorage creates an isolated schema, applies all migrations and returns a
// PostgresStorage pinned to it. The schema is dropped on cleanup so tests are
// fully isolated and may run in parallel.
func newStorage(t testing.TB) (*PostgresStorage, *pgxpool.Pool) {
	t.Helper()
	dsn := testDSN(t)
	ctx := context.Background()

	schema := fmt.Sprintf("mdstore_%d_%d", time.Now().UnixNano(), rand.Intn(1_000_000))
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
	return NewPostgresStorage(pool), pool
}

// --- sample row builders -------------------------------------------------

func sampleTrade(partition int, offset int64, rawID string) model.Trade {
	return model.Trade{
		GroupID:        testGroupID,
		InputID:        testInputID,
		EventTime:      time.Unix(1_700_000_000, 0).UTC(),
		TradeID:        rawID,
		RawTradeID:     rawID,
		Price:          "100.5",
		Quantity:       "1.25",
		Side:           model.TradeSideBuy,
		KafkaTopic:     "md.trade",
		KafkaPartition: partition,
		KafkaOffset:    offset,
	}
}

func sampleKline(openUnix int64, closePrice string, revision int64, closed bool) model.Kline {
	open := time.Unix(openUnix, 0).UTC()
	return model.Kline{
		GroupID:        testGroupID,
		InputID:        testInputID,
		Source:         model.KlineSourceExchange,
		Interval:       "1m",
		OpenTime:       open,
		CloseTime:      open.Add(time.Minute),
		Open:           "100",
		High:           "110",
		Low:            "90",
		Close:          closePrice,
		Volume:         "10",
		QuoteVolume:    "1000",
		TradeCount:     5,
		IsClosed:       closed,
		Revision:       revision,
		KafkaTopic:     "md.kline",
		KafkaPartition: 0,
		KafkaOffset:    openUnix,
	}
}

func sampleDelta(partition int, offset int64, rawID string, sequence *int64) model.OrderBookDelta {
	return model.OrderBookDelta{
		GroupID:        testGroupID,
		InputID:        testInputID,
		EventTime:      time.Unix(1_700_000_000, 0).UTC(),
		RawEventID:     rawID,
		Sequence:       sequence,
		Bids:           []model.PriceLevel{{Price: "100.1", Quantity: "2"}},
		Asks:           []model.PriceLevel{{Price: "100.2", Quantity: "3"}},
		KafkaTopic:     "md.depth",
		KafkaPartition: partition,
		KafkaOffset:    offset,
	}
}

func sampleSnapshot(snapUnix int64, bids, asks []model.PriceLevel) model.OrderBookSnapshot {
	return model.OrderBookSnapshot{
		GroupID:      testGroupID,
		InputID:      testInputID,
		SnapshotTime: time.Unix(snapUnix, 0).UTC(),
		Bids:         bids,
		Asks:         asks,
	}
}

// --- read helpers --------------------------------------------------------

func countRows(t *testing.T, pool *pgxpool.Pool, table string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func levelsJSON(t *testing.T, pool *pgxpool.Pool, query string, args ...any) []model.PriceLevel {
	t.Helper()
	var raw []byte
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&raw); err != nil {
		t.Fatalf("read jsonb levels: %v", err)
	}
	var levels []model.PriceLevel
	if err := json.Unmarshal(raw, &levels); err != nil {
		t.Fatalf("unmarshal jsonb levels: %v", err)
	}
	return levels
}
