package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"MarketDataBackend/internal/model"
)

func TestWriteTradesIdempotent(t *testing.T) {
	st, pool := newStorage(t)
	ctx := context.Background()

	// Batch insert of two distinct trades.
	if err := st.WriteTrades(ctx, []model.Trade{
		sampleTrade(0, 1, "raw-1"),
		sampleTrade(0, 2, "raw-2"),
	}); err != nil {
		t.Fatalf("WriteTrades: %v", err)
	}
	if got := countRows(t, pool, "trades"); got != 2 {
		t.Fatalf("trades = %d, want 2", got)
	}

	// Replaying the same raw_trade_id (different offset) must not duplicate.
	dupRaw := sampleTrade(0, 99, "raw-1")
	if err := st.WriteTrades(ctx, []model.Trade{dupRaw}); err != nil {
		t.Fatalf("WriteTrades dup raw: %v", err)
	}
	if got := countRows(t, pool, "trades"); got != 2 {
		t.Fatalf("after dup raw_trade_id trades = %d, want 2", got)
	}

	// Replaying the same kafka offset must not duplicate either.
	dupOffset := sampleTrade(0, 1, "raw-other")
	if err := st.WriteTrades(ctx, []model.Trade{dupOffset}); err != nil {
		t.Fatalf("WriteTrades dup offset: %v", err)
	}
	if got := countRows(t, pool, "trades"); got != 2 {
		t.Fatalf("after dup offset trades = %d, want 2", got)
	}
}

func TestWriteTradesNullRawIdempotentByOffset(t *testing.T) {
	st, pool := newStorage(t)
	ctx := context.Background()

	// Trades without a raw_trade_id fall back to (input, partition, offset).
	first := sampleTrade(0, 10, "")
	if err := st.WriteTrades(ctx, []model.Trade{first}); err != nil {
		t.Fatalf("WriteTrades: %v", err)
	}
	// Same offset replayed -> deduped by the offset unique index even though
	// raw_trade_id is NULL (the partial raw index does not apply).
	if err := st.WriteTrades(ctx, []model.Trade{first}); err != nil {
		t.Fatalf("WriteTrades replay: %v", err)
	}
	if got := countRows(t, pool, "trades"); got != 1 {
		t.Fatalf("trades = %d, want 1", got)
	}

	// Two NULL-raw trades on different offsets coexist.
	if err := st.WriteTrades(ctx, []model.Trade{sampleTrade(0, 11, "")}); err != nil {
		t.Fatalf("WriteTrades second null raw: %v", err)
	}
	if got := countRows(t, pool, "trades"); got != 2 {
		t.Fatalf("trades = %d, want 2", got)
	}
}

func TestWriteKlinesUpsert(t *testing.T) {
	st, pool := newStorage(t)
	ctx := context.Background()
	const openUnix = 1_700_000_000

	// First insert of an unclosed candle.
	if err := st.WriteKlines(ctx, []model.Kline{sampleKline(openUnix, "105", 1, false)}); err != nil {
		t.Fatalf("WriteKlines insert: %v", err)
	}
	if got := countRows(t, pool, "klines"); got != 1 {
		t.Fatalf("klines = %d, want 1", got)
	}

	// Unclosed candle updated in place: same key, new close, still one row.
	if err := st.WriteKlines(ctx, []model.Kline{sampleKline(openUnix, "108", 2, false)}); err != nil {
		t.Fatalf("WriteKlines update: %v", err)
	}
	if got := countRows(t, pool, "klines"); got != 1 {
		t.Fatalf("after update klines = %d, want 1", got)
	}
	if gotClose, closed := readKline(t, pool, openUnix); gotClose != "108" || closed {
		t.Fatalf("unclosed update: close=%s closed=%v, want 108/false", gotClose, closed)
	}

	// Closing the candle transitions is_closed and stores the final OHLCV.
	if err := st.WriteKlines(ctx, []model.Kline{sampleKline(openUnix, "112", 3, true)}); err != nil {
		t.Fatalf("WriteKlines close: %v", err)
	}
	if gotClose, closed := readKline(t, pool, openUnix); gotClose != "112" || !closed {
		t.Fatalf("close: close=%s closed=%v, want 112/true", gotClose, closed)
	}

	// Replaying a closed candle with the same revision is a no-op (idempotent),
	// and a stale unclosed update can never re-open it.
	if err := st.WriteKlines(ctx, []model.Kline{sampleKline(openUnix, "999", 3, false)}); err != nil {
		t.Fatalf("WriteKlines stale replay: %v", err)
	}
	if gotClose, closed := readKline(t, pool, openUnix); gotClose != "112" || !closed {
		t.Fatalf("closed candle was mutated: close=%s closed=%v, want 112/true", gotClose, closed)
	}
}

func TestWriteKlinesRevisionGuard(t *testing.T) {
	st, pool := newStorage(t)
	ctx := context.Background()
	const openUnix = 1_700_000_500

	// Insert a high-revision unclosed candle.
	if err := st.WriteKlines(ctx, []model.Kline{sampleKline(openUnix, "200", 10, false)}); err != nil {
		t.Fatalf("WriteKlines insert: %v", err)
	}
	// A stale, lower-revision unclosed message (replay / out-of-order) must NOT
	// overwrite the newer candle.
	if err := st.WriteKlines(ctx, []model.Kline{sampleKline(openUnix, "150", 8, false)}); err != nil {
		t.Fatalf("WriteKlines stale: %v", err)
	}
	if gotClose, closed := readKline(t, pool, openUnix); gotClose != "200" || closed {
		t.Fatalf("stale lower revision overwrote candle: close=%s closed=%v, want 200/false", gotClose, closed)
	}
	// An equal-revision unclosed update is still allowed (same in-progress candle).
	if err := st.WriteKlines(ctx, []model.Kline{sampleKline(openUnix, "210", 10, false)}); err != nil {
		t.Fatalf("WriteKlines same revision: %v", err)
	}
	if gotClose, _ := readKline(t, pool, openUnix); gotClose != "210" {
		t.Fatalf("equal-revision update ignored: close=%s, want 210", gotClose)
	}
}

func TestWriteOrderBookDeltasIdempotent(t *testing.T) {
	st, pool := newStorage(t)
	ctx := context.Background()

	seq1 := int64(1001)
	if err := st.WriteOrderBookDeltas(ctx, []model.OrderBookDelta{
		sampleDelta(0, 1, "evt-1", &seq1),
		sampleDelta(0, 2, "evt-2", nil), // sequence may be nil
	}); err != nil {
		t.Fatalf("WriteOrderBookDeltas: %v", err)
	}
	if got := countRows(t, pool, "orderbook_deltas"); got != 2 {
		t.Fatalf("deltas = %d, want 2", got)
	}

	// Duplicate raw_event_id (different offset) is dropped.
	if err := st.WriteOrderBookDeltas(ctx, []model.OrderBookDelta{sampleDelta(0, 50, "evt-1", &seq1)}); err != nil {
		t.Fatalf("WriteOrderBookDeltas dup raw: %v", err)
	}
	// Duplicate kafka offset is dropped.
	if err := st.WriteOrderBookDeltas(ctx, []model.OrderBookDelta{sampleDelta(0, 1, "evt-x", &seq1)}); err != nil {
		t.Fatalf("WriteOrderBookDeltas dup offset: %v", err)
	}
	if got := countRows(t, pool, "orderbook_deltas"); got != 2 {
		t.Fatalf("after dup deltas = %d, want 2", got)
	}

	// bids/asks round-trip through jsonb.
	bids := levelsJSON(t, pool,
		"SELECT bids FROM orderbook_deltas WHERE input_id = $1 AND kafka_offset = 1", testInputID)
	if len(bids) != 1 || bids[0].Price != "100.1" || bids[0].Quantity != "2" {
		t.Fatalf("delta bids round-trip wrong: %+v", bids)
	}
}

func TestWriteOrderBookSnapshots(t *testing.T) {
	st, pool := newStorage(t)
	ctx := context.Background()
	const snapUnix = 1_700_000_000

	bids := []model.PriceLevel{{Price: "100.2", Quantity: "1"}, {Price: "100.1", Quantity: "2"}}
	asks := []model.PriceLevel{{Price: "100.3", Quantity: "3"}, {Price: "100.4", Quantity: "4"}}
	if err := st.WriteOrderBookSnapshots(ctx, []model.OrderBookSnapshot{sampleSnapshot(snapUnix, bids, asks)}); err != nil {
		t.Fatalf("WriteOrderBookSnapshots: %v", err)
	}
	if got := countRows(t, pool, "orderbook_snapshots"); got != 1 {
		t.Fatalf("snapshots = %d, want 1", got)
	}

	// Duplicate (input_id, snapshot_time) is dropped.
	if err := st.WriteOrderBookSnapshots(ctx, []model.OrderBookSnapshot{sampleSnapshot(snapUnix, bids, asks)}); err != nil {
		t.Fatalf("WriteOrderBookSnapshots dup: %v", err)
	}
	if got := countRows(t, pool, "orderbook_snapshots"); got != 1 {
		t.Fatalf("after dup snapshots = %d, want 1", got)
	}

	// bids/asks read back in the order they were written (descending / ascending).
	gotBids := levelsJSON(t, pool,
		"SELECT bids FROM orderbook_snapshots WHERE input_id = $1", testInputID)
	gotAsks := levelsJSON(t, pool,
		"SELECT asks FROM orderbook_snapshots WHERE input_id = $1", testInputID)
	if len(gotBids) != 2 || gotBids[0].Price != "100.2" {
		t.Fatalf("snapshot bids wrong: %+v", gotBids)
	}
	if len(gotAsks) != 2 || gotAsks[0].Price != "100.3" {
		t.Fatalf("snapshot asks wrong: %+v", gotAsks)
	}
}

func TestWriteSnapshotEmptyLevels(t *testing.T) {
	st, pool := newStorage(t)
	ctx := context.Background()

	// nil bids/asks must serialise to an empty JSON array, not null.
	if err := st.WriteOrderBookSnapshots(ctx, []model.OrderBookSnapshot{sampleSnapshot(1, nil, nil)}); err != nil {
		t.Fatalf("WriteOrderBookSnapshots empty: %v", err)
	}
	bids := levelsJSON(t, pool, "SELECT bids FROM orderbook_snapshots WHERE input_id = $1", testInputID)
	if len(bids) != 0 {
		t.Fatalf("empty bids = %+v, want []", bids)
	}
}

func TestWriteEmptySliceIsNoop(t *testing.T) {
	st, pool := newStorage(t)
	ctx := context.Background()

	if err := st.WriteTrades(ctx, nil); err != nil {
		t.Errorf("WriteTrades(nil): %v", err)
	}
	if err := st.WriteKlines(ctx, []model.Kline{}); err != nil {
		t.Errorf("WriteKlines(empty): %v", err)
	}
	if err := st.WriteOrderBookDeltas(ctx, nil); err != nil {
		t.Errorf("WriteOrderBookDeltas(nil): %v", err)
	}
	if err := st.WriteOrderBookSnapshots(ctx, nil); err != nil {
		t.Errorf("WriteOrderBookSnapshots(nil): %v", err)
	}
	for _, table := range []string{"trades", "klines", "orderbook_deltas", "orderbook_snapshots"} {
		if got := countRows(t, pool, table); got != 0 {
			t.Errorf("%s = %d after empty writes, want 0", table, got)
		}
	}
}

func TestWriteInvalidNumericReturnsError(t *testing.T) {
	st, pool := newStorage(t)
	ctx := context.Background()

	bad := sampleTrade(0, 1, "raw-bad")
	bad.Price = "not-a-number"
	if err := st.WriteTrades(ctx, []model.Trade{bad}); err == nil {
		t.Fatal("WriteTrades with invalid numeric: want error, got nil")
	}
	// The failed batch leaves no partial rows behind.
	if got := countRows(t, pool, "trades"); got != 0 {
		t.Fatalf("trades = %d after failed write, want 0", got)
	}
}

func TestWriteContextCanceled(t *testing.T) {
	st, _ := newStorage(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before the call

	err := st.WriteTrades(ctx, []model.Trade{sampleTrade(0, 1, "raw-1")})
	if err == nil {
		t.Fatal("WriteTrades with canceled context: want error, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestMetricWritesNotImplemented(t *testing.T) {
	// No database needed: the metric paths are stubs until M9.
	st := NewPostgresStorage(nil)
	ctx := context.Background()

	if err := st.WriteTradeMetrics(ctx, nil); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("WriteTradeMetrics err = %v, want ErrNotImplemented", err)
	}
	if err := st.WriteBookMetrics(ctx, nil); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("WriteBookMetrics err = %v, want ErrNotImplemented", err)
	}
	if err := st.WriteCrossMetrics(ctx, nil); !errors.Is(err, ErrNotImplemented) {
		t.Errorf("WriteCrossMetrics err = %v, want ErrNotImplemented", err)
	}
	if err := st.Close(); err != nil {
		t.Errorf("Close err = %v, want nil", err)
	}
}

// readKline returns the close price and is_closed flag for the candle opened at
// openUnix.
func readKline(t *testing.T, pool *pgxpool.Pool, openUnix int64) (string, bool) {
	t.Helper()
	var (
		closePrice string
		closed     bool
	)
	open := time.Unix(openUnix, 0).UTC()
	if err := pool.QueryRow(context.Background(),
		"SELECT close, is_closed FROM klines WHERE open_time = $1", open).
		Scan(&closePrice, &closed); err != nil {
		t.Fatalf("read kline: %v", err)
	}
	return closePrice, closed
}
