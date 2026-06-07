package db

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Helpers used by the M1 integration tests. The insertX variants fail the test
// on error; the tryInsertX variants return the error so the caller can assert
// the expected constraint violation.

func tryInsertGroup(ctx context.Context, pool *pgxpool.Pool, id, exch, mtype, sym string) error {
	_, err := pool.Exec(ctx,
		`INSERT INTO market_groups (group_id, exchange, market_type, symbol, desired_status)
		 VALUES ($1, $2, $3, $4, 'running')`, id, exch, mtype, sym)
	return err
}

func insertGroup(t *testing.T, pool *pgxpool.Pool, id, exch, mtype, sym string) {
	t.Helper()
	if err := tryInsertGroup(context.Background(), pool, id, exch, mtype, sym); err != nil {
		t.Fatalf("insert group %s: %v", id, err)
	}
}

func tryInsertInput(ctx context.Context, pool *pgxpool.Pool, id, groupID, streamKey, streamKind string) error {
	_, err := pool.Exec(ctx,
		`INSERT INTO group_inputs (input_id, group_id, stream_key, stream_kind, kafka_topic, kafka_group_id, desired_status)
		 VALUES ($1, $2, $3, $4, 'topic', 'gid', 'running')`, id, groupID, streamKey, streamKind)
	return err
}

func insertInput(t *testing.T, pool *pgxpool.Pool, id, groupID, streamKey, streamKind string) {
	t.Helper()
	if err := tryInsertInput(context.Background(), pool, id, groupID, streamKey, streamKind); err != nil {
		t.Fatalf("insert input %s: %v", id, err)
	}
}

func tryInsertTrade(ctx context.Context, pool *pgxpool.Pool, groupID, inputID, rawTradeID string, partition int, offset int64) error {
	_, err := pool.Exec(ctx,
		`INSERT INTO trades (group_id, input_id, event_time, raw_trade_id, price, quantity, side, kafka_topic, kafka_partition, kafka_offset)
		 VALUES ($1, $2, now(), $3, 100.5, 1.25, 'buy', 'topic', $4, $5)`,
		groupID, inputID, rawTradeID, partition, offset)
	return err
}

func insertTrade(t *testing.T, pool *pgxpool.Pool, groupID, inputID, rawTradeID string, partition int, offset int64) {
	t.Helper()
	if err := tryInsertTrade(context.Background(), pool, groupID, inputID, rawTradeID, partition, offset); err != nil {
		t.Fatalf("insert trade: %v", err)
	}
}

func insertTradeNullRaw(t *testing.T, pool *pgxpool.Pool, groupID, inputID string, partition int, offset int64) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO trades (group_id, input_id, event_time, price, quantity, side, kafka_topic, kafka_partition, kafka_offset)
		 VALUES ($1, $2, now(), 100.5, 1.25, 'buy', 'topic', $3, $4)`,
		groupID, inputID, partition, offset)
	if err != nil {
		t.Fatalf("insert trade (null raw): %v", err)
	}
}

func tryInsertKline(ctx context.Context, pool *pgxpool.Pool, groupID, inputID, interval, openTime string) error {
	_, err := pool.Exec(ctx,
		`INSERT INTO klines (group_id, input_id, source, interval, open_time, close_time, open, high, low, close, volume, kafka_topic, kafka_partition, kafka_offset)
		 VALUES ($1, $2, 'exchange', $3, $4::timestamptz, $4::timestamptz + interval '1 minute', 1, 2, 0.5, 1.5, 10, 'topic', 0, 1)`,
		groupID, inputID, interval, openTime)
	return err
}

func insertKline(t *testing.T, pool *pgxpool.Pool, groupID, inputID, interval, openTime string) {
	t.Helper()
	if err := tryInsertKline(context.Background(), pool, groupID, inputID, interval, openTime); err != nil {
		t.Fatalf("insert kline: %v", err)
	}
}

func tryInsertSnapshot(ctx context.Context, pool *pgxpool.Pool, groupID, inputID, snapshotTime, bids, asks string) error {
	_, err := pool.Exec(ctx,
		`INSERT INTO orderbook_snapshots (group_id, input_id, snapshot_time, bids, asks)
		 VALUES ($1, $2, $3::timestamptz, $4::jsonb, $5::jsonb)`,
		groupID, inputID, snapshotTime, bids, asks)
	return err
}

func insertSnapshot(t *testing.T, pool *pgxpool.Pool, groupID, inputID, snapshotTime, bids, asks string) {
	t.Helper()
	if err := tryInsertSnapshot(context.Background(), pool, groupID, inputID, snapshotTime, bids, asks); err != nil {
		t.Fatalf("insert snapshot: %v", err)
	}
}
