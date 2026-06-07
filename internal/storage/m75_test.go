package storage

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"MarketDataBackend/internal/model"
)

func seedBatchInput(t testing.TB, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	if _, err := pool.Exec(ctx,
		`INSERT INTO market_groups
		    (group_id, exchange, market_type, symbol, desired_status)
		 VALUES ($1, 'test', 'spot', 'M75', 'running')`,
		testGroupID,
	); err != nil {
		t.Fatalf("insert batch group: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO group_inputs
		    (input_id, group_id, stream_key, stream_kind, kafka_topic,
		     kafka_group_id, desired_status)
		 VALUES ($1, $2, 'trade', 'trade', 'md.trade', 'm75', 'running')`,
		testInputID, testGroupID,
	); err != nil {
		t.Fatalf("insert batch input: %v", err)
	}
}

func TestWriteFactBatchStoresRawDeltaAndProgressAtomically(t *testing.T) {
	st, pool := newStorage(t)
	seedBatchInput(t, pool)
	ctx := context.Background()

	raw1 := []byte(`{"event_time":"2026-06-07T10:00:00Z","sequence":100,"bids":[["1","2"]],"asks":[["3","4"]]}`)
	raw2 := []byte("{\n  \"event_time\":\"2026-06-07T10:00:01Z\",\n  \"sequence\":101,\n  \"bids\":[[\"1\",\"0\"]],\n  \"asks\":[[\"3\",\"5\"]]\n}")
	seq100, seq101 := int64(100), int64(101)
	d1 := sampleDelta(0, 10, "evt-100", &seq100)
	d1.RawPayload = raw1
	d2 := sampleDelta(0, 11, "evt-101", &seq101)
	d2.RawPayload = raw2

	batch := FactBatch{
		OrderBookDeltas: []model.OrderBookDelta{d1, d2},
		Progress: model.StreamWriteProgress{
			InputID:        testInputID,
			KafkaPartition: 0,
			DurableOffset:  11,
			LastRawEventID: "evt-101",
			LastSequence:   &seq101,
		},
	}
	if err := st.WriteFactBatch(ctx, batch); err != nil {
		t.Fatalf("WriteFactBatch: %v", err)
	}
	if got := countRows(t, pool, "orderbook_deltas"); got != 2 {
		t.Fatalf("delta rows = %d, want 2", got)
	}

	var gotRaw []byte
	if err := pool.QueryRow(ctx,
		`SELECT raw_payload
		   FROM orderbook_deltas
		  WHERE input_id = $1 AND kafka_offset = 11`,
		testInputID,
	).Scan(&gotRaw); err != nil {
		t.Fatalf("read raw payload: %v", err)
	}
	if !bytes.Equal(gotRaw, raw2) {
		t.Fatalf("raw payload changed:\n got %q\nwant %q", gotRaw, raw2)
	}

	progress, ok, err := st.LoadStreamWriteProgress(ctx, testInputID, 0)
	if err != nil || !ok {
		t.Fatalf("LoadStreamWriteProgress: ok=%v err=%v", ok, err)
	}
	if progress.DurableOffset != 11 || progress.LastRawEventID != "evt-101" ||
		progress.LastSequence == nil || *progress.LastSequence != 101 {
		t.Fatalf("progress = %+v", progress)
	}

	// A database-success/Kafka-commit-failure replay is a complete no-op.
	if err := st.WriteFactBatch(ctx, batch); err != nil {
		t.Fatalf("WriteFactBatch replay: %v", err)
	}
	if got := countRows(t, pool, "orderbook_deltas"); got != 2 {
		t.Fatalf("delta rows after replay = %d, want 2", got)
	}
}

func TestWriteFactBatchMissingPartitionRollsBackProgress(t *testing.T) {
	st, pool := newStorage(t)
	seedBatchInput(t, pool)
	ctx := context.Background()

	seq := int64(1)
	delta := sampleDelta(0, 1, "future", &seq)
	delta.RawPayload = []byte(`{"future":true}`)
	delta.IngestedAt = time.Now().UTC().Add(24 * time.Hour)
	err := st.WriteFactBatch(ctx, FactBatch{
		OrderBookDeltas: []model.OrderBookDelta{delta},
		Progress: model.StreamWriteProgress{
			InputID: testInputID, KafkaPartition: 0, DurableOffset: 1,
			LastRawEventID: "future", LastSequence: &seq,
		},
	})
	if err == nil {
		t.Fatal("WriteFactBatch without target partition: want error")
	}
	if got := countRows(t, pool, "orderbook_deltas"); got != 0 {
		t.Fatalf("delta rows after failed batch = %d, want 0", got)
	}
	if _, ok, loadErr := st.LoadStreamWriteProgress(ctx, testInputID, 0); loadErr != nil || ok {
		t.Fatalf("progress after failed batch: ok=%v err=%v", ok, loadErr)
	}
}

func TestDeltaPartitionMaintenanceDropsOnlyFullyExpiredHours(t *testing.T) {
	st, pool := newStorage(t)
	seedBatchInput(t, pool)
	ctx := context.Background()
	now := time.Now().UTC()
	oldStart := now.Add(-100 * time.Hour).Truncate(time.Hour)

	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer conn.Release()
	var schema string
	if err := conn.QueryRow(ctx, "SELECT current_schema()").Scan(&schema); err != nil {
		t.Fatalf("current_schema: %v", err)
	}
	if err := createDeltaPartition(ctx, conn, schema, oldStart); err != nil {
		t.Fatalf("create old partition: %v", err)
	}

	seq := int64(1)
	delta := sampleDelta(0, 1, "old", &seq)
	delta.RawPayload = []byte(`{"old":true}`)
	delta.IngestedAt = oldStart.Add(time.Minute)
	if err := st.WriteFactBatch(ctx, FactBatch{
		OrderBookDeltas: []model.OrderBookDelta{delta},
		Progress: model.StreamWriteProgress{
			InputID: testInputID, KafkaPartition: 0, DurableOffset: 1,
			LastRawEventID: "old", LastSequence: &seq,
		},
	}); err != nil {
		t.Fatalf("write old delta: %v", err)
	}

	manager, err := NewOrderBookDeltaPartitionManager(pool, DeltaPartitionConfig{
		Retention:           72 * time.Hour,
		MaintenanceInterval: time.Hour,
		PrecreateHorizon:    3 * time.Hour,
	}, nil)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	if err := manager.maintainAt(ctx, conn, now); err != nil {
		t.Fatalf("maintainAt: %v", err)
	}

	oldName := "orderbook_deltas_" + oldStart.Format("20060102_15")
	var regclass *string
	if err := pool.QueryRow(ctx, "SELECT to_regclass($1)", fmt.Sprintf("%s.%s", schema, oldName)).
		Scan(&regclass); err != nil {
		t.Fatalf("to_regclass old partition: %v", err)
	}
	if regclass != nil {
		t.Fatalf("expired partition still exists: %s", *regclass)
	}
	if got := countRows(t, pool, "orderbook_deltas"); got != 0 {
		t.Fatalf("rows after expired partition drop = %d, want 0", got)
	}

	currentName := "orderbook_deltas_" + now.Truncate(time.Hour).Format("20060102_15")
	if err := pool.QueryRow(ctx, "SELECT to_regclass($1)", fmt.Sprintf("%s.%s", schema, currentName)).
		Scan(&regclass); err != nil {
		t.Fatalf("to_regclass current partition: %v", err)
	}
	if regclass == nil {
		t.Fatal("current partition was not retained/precreated")
	}

	var futureCount int
	if err := pool.QueryRow(ctx,
		`SELECT count(*)
		   FROM orderbook_delta_partitions
		  WHERE range_start >= $1 AND range_start < $2`,
		now.Truncate(time.Hour), now.Add(3*time.Hour),
	).Scan(&futureCount); err != nil {
		t.Fatalf("count current/future partitions: %v", err)
	}
	if futureCount < 3 {
		t.Fatalf("current/future partitions = %d, want at least 3", futureCount)
	}
}

func TestOrderBookDeltasIsRangePartitioned(t *testing.T) {
	_, pool := newStorage(t)
	var strategy string
	if err := pool.QueryRow(context.Background(),
		`SELECT partstrat
		   FROM pg_partitioned_table
		  WHERE partrelid = 'orderbook_deltas'::regclass`,
	).Scan(&strategy); err != nil {
		t.Fatalf("read partition strategy: %v", err)
	}
	if strategy != "r" {
		t.Fatalf("partition strategy = %q, want range", strategy)
	}
}

func BenchmarkWriteFactBatchOrderBookDeltas(b *testing.B) {
	st, pool := newStorage(b)
	seedBatchInput(b, pool)
	ctx := context.Background()
	const rowsPerBatch = 2000
	raw := []byte(`{"event_time":"2026-06-07T10:00:00Z","sequence":1,"bids":[["100.1","1"],["100.0","2"]],"asks":[["100.2","1"],["100.3","2"]]}`)

	b.ReportMetric(rowsPerBatch, "rows/batch")
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		rows := make([]model.OrderBookDelta, 0, rowsPerBatch)
		base := int64(iteration * rowsPerBatch)
		for i := 0; i < rowsPerBatch; i++ {
			offset := base + int64(i)
			sequence := offset + 1
			row := sampleDelta(0, offset, fmt.Sprintf("bench-%d", offset), &sequence)
			row.RawPayload = raw
			rows = append(rows, row)
		}
		lastSequence := base + rowsPerBatch
		batch := FactBatch{
			OrderBookDeltas: rows,
			Progress: model.StreamWriteProgress{
				InputID: testInputID, KafkaPartition: 0,
				DurableOffset:  base + rowsPerBatch - 1,
				LastRawEventID: fmt.Sprintf("bench-%d", base+rowsPerBatch-1),
				LastSequence:   &lastSequence,
			},
		}
		b.StartTimer()
		if err := st.WriteFactBatch(ctx, batch); err != nil {
			b.Fatalf("WriteFactBatch: %v", err)
		}
	}
}
