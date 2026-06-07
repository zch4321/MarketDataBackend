// Package storage defines the market-data storage interface and its
// PostgreSQL implementation. The contract is fixed in M1; PostgresStorage
// (idempotent fact-table writes) lands in M3. Derived-metric write paths stay
// stubbed with ErrNotImplemented until M9.
package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"MarketDataBackend/internal/model"
)

// PostgresStorage is the PostgreSQL-backed implementation of MarketDataStorage.
// Writes are idempotent under at-least-once consumption: every fact table has a
// unique key that turns a replayed message into a no-op (or, for klines, an
// in-place update of the unclosed candle). The pool's lifecycle is owned by the
// caller (it is shared with the metadata store), so Close is a no-op.
type PostgresStorage struct {
	pool *pgxpool.Pool
}

// Compile-time assertion that PostgresStorage satisfies the interface.
var _ MarketDataStorage = (*PostgresStorage)(nil)

// NewPostgresStorage wraps an existing pgx pool.
func NewPostgresStorage(pool *pgxpool.Pool) *PostgresStorage {
	return &PostgresStorage{pool: pool}
}

// WriteFactBatch atomically stores one input/partition's validated fact prefix
// and advances its durable offset. Rows at or below the already-durable offset
// are filtered so a Kafka commit failure can replay the batch without creating
// duplicate partitioned facts.
func (s *PostgresStorage) WriteFactBatch(ctx context.Context, batch FactBatch) error {
	p := batch.Progress
	if err := validateFactBatch(batch); err != nil {
		return err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("storage: write fact batch: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	if _, err := tx.Exec(ctx,
		`INSERT INTO stream_write_progress
		    (input_id, kafka_partition, durable_offset)
		 VALUES ($1, $2, -1)
		 ON CONFLICT (input_id, kafka_partition) DO NOTHING`,
		p.InputID, p.KafkaPartition,
	); err != nil {
		return fmt.Errorf("storage: write fact batch: initialize progress: %w", err)
	}

	var durable int64
	if err := tx.QueryRow(ctx,
		`SELECT durable_offset
		   FROM stream_write_progress
		  WHERE input_id = $1 AND kafka_partition = $2
		  FOR UPDATE`,
		p.InputID, p.KafkaPartition,
	).Scan(&durable); err != nil {
		return fmt.Errorf("storage: write fact batch: lock progress: %w", err)
	}

	if p.DurableOffset <= durable {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("storage: write fact batch replay commit: %w", err)
		}
		return nil
	}

	dbNow, err := databaseNow(ctx, tx)
	if err != nil {
		return fmt.Errorf("storage: write fact batch: database time: %w", err)
	}

	trades := tradesAfterOffset(batch.Trades, p, durable)
	if err := writeTradesTx(ctx, tx, trades); err != nil {
		return err
	}
	klines := klinesAfterOffset(batch.Klines, p, durable)
	if err := writeKlinesTx(ctx, tx, klines); err != nil {
		return err
	}
	deltas := deltasAfterOffset(batch.OrderBookDeltas, p, durable)
	if err := writeOrderBookDeltasCopy(ctx, tx, deltas, dbNow); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx,
		`UPDATE stream_write_progress
		    SET durable_offset = $3,
		        last_raw_event_id = $4,
		        last_update_id = $5,
		        last_sequence = $6,
		        updated_at = clock_timestamp()
		  WHERE input_id = $1 AND kafka_partition = $2`,
		p.InputID, p.KafkaPartition, p.DurableOffset,
		nullIfEmpty(p.LastRawEventID), p.LastUpdateID, p.LastSequence,
	); err != nil {
		return fmt.Errorf("storage: write fact batch: update progress: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("storage: write fact batch: commit: %w", err)
	}
	return nil
}

func validateFactBatch(batch FactBatch) error {
	p := batch.Progress
	if p.InputID == "" {
		return fmt.Errorf("storage: write fact batch: input_id is required")
	}
	if p.KafkaPartition < 0 {
		return fmt.Errorf("storage: write fact batch: kafka_partition must be >= 0")
	}
	if p.DurableOffset < 0 {
		return fmt.Errorf("storage: write fact batch: durable_offset must be >= 0")
	}
	kinds := 0
	if len(batch.Trades) > 0 {
		kinds++
	}
	if len(batch.Klines) > 0 {
		kinds++
	}
	if len(batch.OrderBookDeltas) > 0 {
		kinds++
	}
	if kinds > 1 {
		return fmt.Errorf("storage: write fact batch: mixed stream kinds are not allowed")
	}
	validate := func(inputID string, partition int, offset int64) error {
		if inputID != p.InputID || partition != p.KafkaPartition {
			return fmt.Errorf("fact coordinates do not match progress")
		}
		if offset < 0 || offset > p.DurableOffset {
			return fmt.Errorf("fact offset %d is outside durable prefix ending at %d",
				offset, p.DurableOffset)
		}
		return nil
	}
	var previous int64 = -1
	for _, row := range batch.Trades {
		if err := validate(row.InputID, row.KafkaPartition, row.KafkaOffset); err != nil {
			return fmt.Errorf("storage: write fact batch: trade: %w", err)
		}
		if row.KafkaOffset <= previous {
			return fmt.Errorf("storage: write fact batch: trade offsets are not increasing")
		}
		previous = row.KafkaOffset
	}
	previous = -1
	for _, row := range batch.Klines {
		if err := validate(row.InputID, row.KafkaPartition, row.KafkaOffset); err != nil {
			return fmt.Errorf("storage: write fact batch: kline: %w", err)
		}
		if row.KafkaOffset <= previous {
			return fmt.Errorf("storage: write fact batch: kline offsets are not increasing")
		}
		previous = row.KafkaOffset
	}
	previous = -1
	for _, row := range batch.OrderBookDeltas {
		if err := validate(row.InputID, row.KafkaPartition, row.KafkaOffset); err != nil {
			return fmt.Errorf("storage: write fact batch: orderbook delta: %w", err)
		}
		if row.KafkaOffset <= previous {
			return fmt.Errorf("storage: write fact batch: orderbook delta offsets are not increasing")
		}
		previous = row.KafkaOffset
	}
	return nil
}

// LoadStreamWriteProgress returns the durable database cursor for one Kafka
// input partition. The bool is false until the first fact batch is committed.
func (s *PostgresStorage) LoadStreamWriteProgress(
	ctx context.Context, inputID string, partition int,
) (model.StreamWriteProgress, bool, error) {
	var p model.StreamWriteProgress
	err := s.pool.QueryRow(ctx,
		`SELECT input_id, kafka_partition, durable_offset,
		        COALESCE(last_raw_event_id, ''),
		        last_update_id, last_sequence, updated_at
		   FROM stream_write_progress
		  WHERE input_id = $1 AND kafka_partition = $2`,
		inputID, partition,
	).Scan(
		&p.InputID, &p.KafkaPartition, &p.DurableOffset, &p.LastRawEventID,
		&p.LastUpdateID, &p.LastSequence, &p.UpdatedAt,
	)
	if err == pgx.ErrNoRows {
		return model.StreamWriteProgress{}, false, nil
	}
	if err != nil {
		return model.StreamWriteProgress{}, false,
			fmt.Errorf("storage: load stream write progress: %w", err)
	}
	return p, true, nil
}

// WriteTrades inserts trade facts with INSERT ... ON CONFLICT DO NOTHING. Both
// idempotency keys are honoured: a replayed (input_id, partition, offset) or a
// duplicate (input_id, raw_trade_id) is silently dropped. An empty slice is a
// no-op.
func (s *PostgresStorage) WriteTrades(ctx context.Context, rows []model.Trade) error {
	if len(rows) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, r := range rows {
		batch.Queue(
			`INSERT INTO trades
			   (group_id, input_id, event_time, exchange_time, local_receive_time,
			    trade_id, raw_trade_id, price, quantity, side, is_aggregated,
			    kafka_topic, kafka_partition, kafka_offset, schema_version)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
			 ON CONFLICT DO NOTHING`,
			r.GroupID, r.InputID, r.EventTime, r.ExchangeTime, r.LocalReceiveTime,
			nullIfEmpty(r.TradeID), nullIfEmpty(r.RawTradeID), r.Price, r.Quantity,
			r.Side, r.IsAggregated, r.KafkaTopic, r.KafkaPartition, r.KafkaOffset,
			schemaVersionOrDefault(r.SchemaVersion),
		)
	}
	return s.exec(ctx, batch, "write trades")
}

// WriteKlines upserts candlesticks keyed by (group_id, source, interval,
// open_time). The update is revision-guarded so at-least-once replays and
// out-of-order messages cannot move a candle backwards:
//   - an open candle is updated only by an equal-or-newer revision, so a stale
//     lower-revision unclosed message is ignored; and
//   - a closed candle is overwritten only by a strictly newer revision, so a
//     replayed closed candle is idempotent and a late unclosed update can never
//     re-open it.
func (s *PostgresStorage) WriteKlines(ctx context.Context, rows []model.Kline) error {
	if len(rows) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, r := range rows {
		batch.Queue(
			`INSERT INTO klines
			   (group_id, input_id, source, interval, open_time, close_time,
			    open, high, low, close, volume, quote_volume, trade_count,
			    is_closed, revision, kafka_topic, kafka_partition, kafka_offset)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
			 ON CONFLICT (group_id, source, interval, open_time) DO UPDATE SET
			    input_id        = EXCLUDED.input_id,
			    close_time      = EXCLUDED.close_time,
			    open            = EXCLUDED.open,
			    high            = EXCLUDED.high,
			    low             = EXCLUDED.low,
			    close           = EXCLUDED.close,
			    volume          = EXCLUDED.volume,
			    quote_volume    = EXCLUDED.quote_volume,
			    trade_count     = EXCLUDED.trade_count,
			    is_closed       = EXCLUDED.is_closed,
			    revision        = EXCLUDED.revision,
			    kafka_topic     = EXCLUDED.kafka_topic,
			    kafka_partition = EXCLUDED.kafka_partition,
			    kafka_offset    = EXCLUDED.kafka_offset,
			    updated_at      = now()
			 WHERE (klines.is_closed = false AND EXCLUDED.revision >= klines.revision)
			    OR EXCLUDED.revision > klines.revision`,
			r.GroupID, r.InputID, sourceOrDefault(r.Source), r.Interval, r.OpenTime, r.CloseTime,
			r.Open, r.High, r.Low, r.Close, r.Volume, nullIfEmpty(r.QuoteVolume), r.TradeCount,
			r.IsClosed, r.Revision, r.KafkaTopic, r.KafkaPartition, r.KafkaOffset,
		)
	}
	return s.exec(ctx, batch, "write klines")
}

// WriteOrderBookDeltas retains the pre-M7.5 direct-write API. The runtime uses
// WriteFactBatch; this path performs an explicit global existence check because
// a partitioned table cannot enforce the old cross-partition unique indexes.
func (s *PostgresStorage) WriteOrderBookDeltas(ctx context.Context, rows []model.OrderBookDelta) error {
	if len(rows) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	now := time.Time{}
	for _, r := range rows {
		if r.IngestedAt.IsZero() && now.IsZero() {
			if err := s.pool.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
				return fmt.Errorf("storage: write deltas: database time: %w", err)
			}
		}
		ingestedAt := r.IngestedAt
		if ingestedAt.IsZero() {
			ingestedAt = now
		}
		bids, err := marshalLevels(r.Bids)
		if err != nil {
			return fmt.Errorf("storage: write deltas: bids: %w", err)
		}
		asks, err := marshalLevels(r.Asks)
		if err != nil {
			return fmt.Errorf("storage: write deltas: asks: %w", err)
		}
		batch.Queue(
			`INSERT INTO orderbook_deltas
			   (group_id, input_id, event_time, exchange_time, local_receive_time,
			    raw_event_id, first_update_id, last_update_id, prev_update_id, sequence,
			    bids, asks, raw_payload, kafka_topic, kafka_partition, kafka_offset,
			    ingested_at)
			 SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
			        $14, $15, $16, $17
			  WHERE NOT EXISTS (
			        SELECT 1
			          FROM orderbook_deltas existing
			         WHERE existing.input_id = $2
			           AND (
			                (existing.kafka_partition = $15 AND existing.kafka_offset = $16)
			                OR ($6::text IS NOT NULL AND existing.raw_event_id = $6)
			           )
			  )`,
			r.GroupID, r.InputID, r.EventTime, r.ExchangeTime, r.LocalReceiveTime,
			nullIfEmpty(r.RawEventID), r.FirstUpdateID, r.LastUpdateID, r.PrevUpdateID, r.Sequence,
			bids, asks, nonNilBytes(r.RawPayload), r.KafkaTopic, r.KafkaPartition, r.KafkaOffset,
			ingestedAt,
		)
	}
	return s.exec(ctx, batch, "write orderbook deltas")
}

// WriteOrderBookSnapshots inserts point-in-time snapshots keyed by
// (input_id, snapshot_time) with ON CONFLICT DO NOTHING. bids must already be
// in descending price order and asks ascending; that ordering is the caller's
// responsibility (validated at the application layer).
func (s *PostgresStorage) WriteOrderBookSnapshots(ctx context.Context, rows []model.OrderBookSnapshot) error {
	if len(rows) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, r := range rows {
		bids, err := marshalLevels(r.Bids)
		if err != nil {
			return fmt.Errorf("storage: write snapshots: bids: %w", err)
		}
		asks, err := marshalLevels(r.Asks)
		if err != nil {
			return fmt.Errorf("storage: write snapshots: asks: %w", err)
		}
		batch.Queue(
			`INSERT INTO orderbook_snapshots
			   (group_id, input_id, snapshot_time, sequence, bids, asks)
			 VALUES ($1, $2, $3, $4, $5, $6)
			 ON CONFLICT (input_id, snapshot_time) DO NOTHING`,
			r.GroupID, r.InputID, r.SnapshotTime, r.Sequence, bids, asks,
		)
	}
	return s.exec(ctx, batch, "write orderbook snapshots")
}

// WriteTradeMetrics is implemented in M9.
func (s *PostgresStorage) WriteTradeMetrics(ctx context.Context, rows []model.TradeMetric) error {
	return ErrNotImplemented
}

// WriteBookMetrics is implemented in M9.
func (s *PostgresStorage) WriteBookMetrics(ctx context.Context, rows []model.BookMetric) error {
	return ErrNotImplemented
}

// WriteCrossMetrics is implemented in M9.
func (s *PostgresStorage) WriteCrossMetrics(ctx context.Context, rows []model.CrossMetric) error {
	return ErrNotImplemented
}

// Close is a no-op: the pool is owned by the caller (shared with the metadata
// store) and closed there.
func (s *PostgresStorage) Close() error { return nil }

// exec sends a batch and drains every result so a constraint or encoding error
// on any queued statement surfaces. pgx runs the batch within an implicit
// transaction, so a mid-batch failure leaves no partial rows behind.
func (s *PostgresStorage) exec(ctx context.Context, batch *pgx.Batch, op string) error {
	br := s.pool.SendBatch(ctx, batch)
	for i := 0; i < batch.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			_ = br.Close()
			return fmt.Errorf("storage: %s: %w", op, err)
		}
	}
	if err := br.Close(); err != nil {
		return fmt.Errorf("storage: %s: close batch: %w", op, err)
	}
	return nil
}

func databaseNow(ctx context.Context, tx pgx.Tx) (time.Time, error) {
	var now time.Time
	err := tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now)
	return now.UTC(), err
}

func tradesAfterOffset(
	rows []model.Trade, p model.StreamWriteProgress, durable int64,
) []model.Trade {
	out := make([]model.Trade, 0, len(rows))
	for _, row := range rows {
		if row.InputID == p.InputID && row.KafkaPartition == p.KafkaPartition &&
			row.KafkaOffset > durable && row.KafkaOffset <= p.DurableOffset {
			out = append(out, row)
		}
	}
	return out
}

func klinesAfterOffset(
	rows []model.Kline, p model.StreamWriteProgress, durable int64,
) []model.Kline {
	out := make([]model.Kline, 0, len(rows))
	for _, row := range rows {
		if row.InputID == p.InputID && row.KafkaPartition == p.KafkaPartition &&
			row.KafkaOffset > durable && row.KafkaOffset <= p.DurableOffset {
			out = append(out, row)
		}
	}
	return out
}

func deltasAfterOffset(
	rows []model.OrderBookDelta, p model.StreamWriteProgress, durable int64,
) []model.OrderBookDelta {
	out := make([]model.OrderBookDelta, 0, len(rows))
	for _, row := range rows {
		if row.InputID == p.InputID && row.KafkaPartition == p.KafkaPartition &&
			row.KafkaOffset > durable && row.KafkaOffset <= p.DurableOffset {
			out = append(out, row)
		}
	}
	return out
}

func writeTradesTx(ctx context.Context, tx pgx.Tx, rows []model.Trade) error {
	if len(rows) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, r := range rows {
		batch.Queue(
			`INSERT INTO trades
			   (group_id, input_id, event_time, exchange_time, local_receive_time,
			    trade_id, raw_trade_id, price, quantity, side, is_aggregated,
			    kafka_topic, kafka_partition, kafka_offset, schema_version)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
			 ON CONFLICT DO NOTHING`,
			r.GroupID, r.InputID, r.EventTime, r.ExchangeTime, r.LocalReceiveTime,
			nullIfEmpty(r.TradeID), nullIfEmpty(r.RawTradeID), r.Price, r.Quantity,
			r.Side, r.IsAggregated, r.KafkaTopic, r.KafkaPartition, r.KafkaOffset,
			schemaVersionOrDefault(r.SchemaVersion),
		)
	}
	return execTxBatch(ctx, tx, batch, "write fact batch trades")
}

func writeKlinesTx(ctx context.Context, tx pgx.Tx, rows []model.Kline) error {
	if len(rows) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, r := range rows {
		batch.Queue(
			`INSERT INTO klines
			   (group_id, input_id, source, interval, open_time, close_time,
			    open, high, low, close, volume, quote_volume, trade_count,
			    is_closed, revision, kafka_topic, kafka_partition, kafka_offset)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
			 ON CONFLICT (group_id, source, interval, open_time) DO UPDATE SET
			    input_id = EXCLUDED.input_id,
			    close_time = EXCLUDED.close_time,
			    open = EXCLUDED.open,
			    high = EXCLUDED.high,
			    low = EXCLUDED.low,
			    close = EXCLUDED.close,
			    volume = EXCLUDED.volume,
			    quote_volume = EXCLUDED.quote_volume,
			    trade_count = EXCLUDED.trade_count,
			    is_closed = EXCLUDED.is_closed,
			    revision = EXCLUDED.revision,
			    kafka_topic = EXCLUDED.kafka_topic,
			    kafka_partition = EXCLUDED.kafka_partition,
			    kafka_offset = EXCLUDED.kafka_offset,
			    updated_at = now()
			 WHERE (klines.is_closed = false AND EXCLUDED.revision >= klines.revision)
			    OR EXCLUDED.revision > klines.revision`,
			r.GroupID, r.InputID, sourceOrDefault(r.Source), r.Interval, r.OpenTime, r.CloseTime,
			r.Open, r.High, r.Low, r.Close, r.Volume, nullIfEmpty(r.QuoteVolume), r.TradeCount,
			r.IsClosed, r.Revision, r.KafkaTopic, r.KafkaPartition, r.KafkaOffset,
		)
	}
	return execTxBatch(ctx, tx, batch, "write fact batch klines")
}

func writeOrderBookDeltasCopy(
	ctx context.Context, tx pgx.Tx, rows []model.OrderBookDelta, dbNow time.Time,
) error {
	if len(rows) == 0 {
		return nil
	}
	copyRows := make([][]any, 0, len(rows))
	for _, r := range rows {
		bids, err := marshalLevels(r.Bids)
		if err != nil {
			return fmt.Errorf("storage: write fact batch deltas: bids: %w", err)
		}
		asks, err := marshalLevels(r.Asks)
		if err != nil {
			return fmt.Errorf("storage: write fact batch deltas: asks: %w", err)
		}
		ingestedAt := r.IngestedAt
		if ingestedAt.IsZero() {
			ingestedAt = dbNow
		}
		copyRows = append(copyRows, []any{
			r.GroupID, r.InputID, r.EventTime, r.ExchangeTime, r.LocalReceiveTime,
			nullIfEmpty(r.RawEventID), r.FirstUpdateID, r.LastUpdateID, r.PrevUpdateID,
			r.Sequence, json.RawMessage(bids), json.RawMessage(asks),
			nonNilBytes(r.RawPayload), r.KafkaTopic, r.KafkaPartition, r.KafkaOffset,
			ingestedAt,
		})
	}
	_, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{"orderbook_deltas"},
		[]string{
			"group_id", "input_id", "event_time", "exchange_time", "local_receive_time",
			"raw_event_id", "first_update_id", "last_update_id", "prev_update_id",
			"sequence", "bids", "asks", "raw_payload", "kafka_topic",
			"kafka_partition", "kafka_offset", "ingested_at",
		},
		pgx.CopyFromRows(copyRows),
	)
	if err != nil {
		return fmt.Errorf("storage: write fact batch deltas: copy: %w", err)
	}
	return nil
}

func execTxBatch(
	ctx context.Context, tx pgx.Tx, batch *pgx.Batch, op string,
) error {
	br := tx.SendBatch(ctx, batch)
	for i := 0; i < batch.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			_ = br.Close()
			return fmt.Errorf("storage: %s: %w", op, err)
		}
	}
	if err := br.Close(); err != nil {
		return fmt.Errorf("storage: %s: close batch: %w", op, err)
	}
	return nil
}

func nonNilBytes(value []byte) []byte {
	if value == nil {
		return []byte{}
	}
	return value
}

// marshalLevels encodes price levels as a jsonb array string. A nil slice
// becomes "[]" rather than "null" so the column always holds a JSON array.
func marshalLevels(levels []model.PriceLevel) (string, error) {
	if levels == nil {
		levels = []model.PriceLevel{}
	}
	b, err := json.Marshal(levels)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// nullIfEmpty maps "" to a NULL parameter so partial unique indexes (e.g.
// raw_trade_id) and nullable text columns behave correctly.
func nullIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func schemaVersionOrDefault(v int) int {
	if v <= 0 {
		return 1
	}
	return v
}

func sourceOrDefault(s string) string {
	if s == "" {
		return model.KlineSourceExchange
	}
	return s
}

// --- M8 query methods ----------------------------------------------------

const (
	defaultQueryLimit = 200
	maxQueryLimit     = 1000
)

func validateQueryLimit(limit int) int {
	if limit <= 0 {
		return defaultQueryLimit
	}
	if limit > maxQueryLimit {
		return maxQueryLimit
	}
	return limit
}

// QueryTrades returns trades in [from, to) ordered by event_time ascending.
// cursor is the exclusive lower event_time bound for the next page.
func (s *PostgresStorage) QueryTrades(
	ctx context.Context, groupID string, from, to time.Time,
	limit int, cursor *time.Time,
) (QueryResult[model.Trade], error) {
	limit = validateQueryLimit(limit)
	fetchLimit := limit + 1

	rows, err := s.pool.Query(ctx,
		`SELECT group_id, input_id, event_time, exchange_time, local_receive_time,
		        trade_id, raw_trade_id, price, quantity, side, is_aggregated,
		        kafka_topic, kafka_partition, kafka_offset, schema_version, ingested_at
		   FROM trades
		  WHERE group_id = $1
		    AND event_time >= $2
		    AND event_time < $3
		    AND ($4::timestamptz IS NULL OR event_time > $4)
		  ORDER BY event_time ASC, id ASC
		  LIMIT $5`,
		groupID, from, to, cursor, fetchLimit,
	)
	if err != nil {
		return QueryResult[model.Trade]{},
			fmt.Errorf("storage: query trades: %w", err)
	}
	defer rows.Close()

	var result QueryResult[model.Trade]
	for rows.Next() {
		var t model.Trade
		if err := rows.Scan(
			&t.GroupID, &t.InputID, &t.EventTime, &t.ExchangeTime,
			&t.LocalReceiveTime, &t.TradeID, &t.RawTradeID,
			&t.Price, &t.Quantity, &t.Side, &t.IsAggregated,
			&t.KafkaTopic, &t.KafkaPartition, &t.KafkaOffset,
			&t.SchemaVersion, &t.IngestedAt,
		); err != nil {
			return QueryResult[model.Trade]{},
				fmt.Errorf("storage: query trades: scan: %w", err)
		}
		result.Rows = append(result.Rows, t)
	}
	if err := rows.Err(); err != nil {
		return QueryResult[model.Trade]{},
			fmt.Errorf("storage: query trades: rows: %w", err)
	}

	if len(result.Rows) > limit {
		next := result.Rows[limit].EventTime
		result.NextCursor = &next
		result.Rows = result.Rows[:limit]
	}
	return result, nil
}

// QueryKlines returns klines in [from, to) ordered by open_time ascending.
func (s *PostgresStorage) QueryKlines(
	ctx context.Context, groupID string, from, to time.Time,
	limit int, cursor *time.Time,
) (QueryResult[model.Kline], error) {
	limit = validateQueryLimit(limit)
	fetchLimit := limit + 1

	rows, err := s.pool.Query(ctx,
		`SELECT group_id, input_id, source, interval, open_time, close_time,
		        open, high, low, close, volume, quote_volume, trade_count,
		        is_closed, revision, kafka_topic, kafka_partition, kafka_offset,
		        updated_at
		   FROM klines
		  WHERE group_id = $1
		    AND open_time >= $2
		    AND open_time < $3
		    AND ($4::timestamptz IS NULL OR open_time > $4)
		  ORDER BY open_time ASC
		  LIMIT $5`,
		groupID, from, to, cursor, fetchLimit,
	)
	if err != nil {
		return QueryResult[model.Kline]{},
			fmt.Errorf("storage: query klines: %w", err)
	}
	defer rows.Close()

	var result QueryResult[model.Kline]
	for rows.Next() {
		var k model.Kline
		if err := rows.Scan(
			&k.GroupID, &k.InputID, &k.Source, &k.Interval,
			&k.OpenTime, &k.CloseTime,
			&k.Open, &k.High, &k.Low, &k.Close,
			&k.Volume, &k.QuoteVolume, &k.TradeCount,
			&k.IsClosed, &k.Revision,
			&k.KafkaTopic, &k.KafkaPartition, &k.KafkaOffset,
			&k.UpdatedAt,
		); err != nil {
			return QueryResult[model.Kline]{},
				fmt.Errorf("storage: query klines: scan: %w", err)
		}
		result.Rows = append(result.Rows, k)
	}
	if err := rows.Err(); err != nil {
		return QueryResult[model.Kline]{},
			fmt.Errorf("storage: query klines: rows: %w", err)
	}

	if len(result.Rows) > limit {
		next := result.Rows[limit].OpenTime
		result.NextCursor = &next
		result.Rows = result.Rows[:limit]
	}
	return result, nil
}

// QuerySnapshots returns snapshots in [from, to) ordered by snapshot_time ascending.
func (s *PostgresStorage) QuerySnapshots(
	ctx context.Context, groupID string, from, to time.Time,
	limit int, cursor *time.Time,
) (QueryResult[model.OrderBookSnapshot], error) {
	limit = validateQueryLimit(limit)
	fetchLimit := limit + 1

	rows, err := s.pool.Query(ctx,
		`SELECT group_id, input_id, snapshot_time, sequence, bids, asks, created_at
		   FROM orderbook_snapshots
		  WHERE group_id = $1
		    AND snapshot_time >= $2
		    AND snapshot_time < $3
		    AND ($4::timestamptz IS NULL OR snapshot_time > $4)
		  ORDER BY snapshot_time ASC, snapshot_id ASC
		  LIMIT $5`,
		groupID, from, to, cursor, fetchLimit,
	)
	if err != nil {
		return QueryResult[model.OrderBookSnapshot]{},
			fmt.Errorf("storage: query snapshots: %w", err)
	}
	defer rows.Close()

	var result QueryResult[model.OrderBookSnapshot]
	for rows.Next() {
		var sn model.OrderBookSnapshot
		var bidsRaw, asksRaw []byte
		if err := rows.Scan(
			&sn.GroupID, &sn.InputID, &sn.SnapshotTime,
			&sn.Sequence, &bidsRaw, &asksRaw, &sn.CreatedAt,
		); err != nil {
			return QueryResult[model.OrderBookSnapshot]{},
				fmt.Errorf("storage: query snapshots: scan: %w", err)
		}
		if err := json.Unmarshal(bidsRaw, &sn.Bids); err != nil {
			return QueryResult[model.OrderBookSnapshot]{},
				fmt.Errorf("storage: query snapshots: unmarshal bids: %w", err)
		}
		if err := json.Unmarshal(asksRaw, &sn.Asks); err != nil {
			return QueryResult[model.OrderBookSnapshot]{},
				fmt.Errorf("storage: query snapshots: unmarshal asks: %w", err)
		}
		result.Rows = append(result.Rows, sn)
	}
	if err := rows.Err(); err != nil {
		return QueryResult[model.OrderBookSnapshot]{},
			fmt.Errorf("storage: query snapshots: rows: %w", err)
	}

	if len(result.Rows) > limit {
		next := result.Rows[limit].SnapshotTime
		result.NextCursor = &next
		result.Rows = result.Rows[:limit]
	}
	return result, nil
}
