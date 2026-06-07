package storage

import (
	"context"
	"errors"
	"time"

	"MarketDataBackend/internal/model"
)

// ErrNotImplemented is returned by the derived-metric write paths until M9.
var ErrNotImplemented = errors.New("storage: not implemented")

// FactBatch contains one input/partition's highest contiguous validated Kafka
// prefix. Progress is committed atomically with all included facts.
type FactBatch struct {
	Trades          []model.Trade
	Klines          []model.Kline
	OrderBookDeltas []model.OrderBookDelta
	Progress        model.StreamWriteProgress
}

// QueryResult wraps a page of results and a cursor for the next page.
// NextCursor is a composite keyset cursor (RFC3339 + secondary key) that
// avoids skipping rows at page boundaries or when multiple records share
// the same timestamp.
type QueryResult[T any] struct {
	Rows       []T
	NextCursor string // empty when there are no more pages
}

// MarketDataStorage writes normalized market-data facts and derived metrics.
type MarketDataStorage interface {
	WriteFactBatch(ctx context.Context, batch FactBatch) error
	LoadStreamWriteProgress(
		ctx context.Context, inputID string, partition int,
	) (model.StreamWriteProgress, bool, error)

	WriteTrades(ctx context.Context, rows []model.Trade) error
	WriteKlines(ctx context.Context, rows []model.Kline) error
	WriteOrderBookDeltas(ctx context.Context, rows []model.OrderBookDelta) error
	WriteOrderBookSnapshots(ctx context.Context, rows []model.OrderBookSnapshot) error

	WriteTradeMetrics(ctx context.Context, rows []model.TradeMetric) error
	WriteBookMetrics(ctx context.Context, rows []model.BookMetric) error
	WriteCrossMetrics(ctx context.Context, rows []model.CrossMetric) error

	// M10 dead-letter path.
	WritePoisonRecord(ctx context.Context, record model.PoisonRecord) error

	// M8 query methods.  All accept a mandatory time range [from, to).
	// cursor is a composite keyset for the next page (empty for the first
	// page); limit caps the number of rows (0 uses the default of 200).
	QueryTrades(
		ctx context.Context, groupID string, from, to time.Time,
		limit int, cursor string,
	) (QueryResult[model.Trade], error)
	QueryKlines(
		ctx context.Context, groupID string, from, to time.Time,
		limit int, cursor string,
	) (QueryResult[model.Kline], error)
	QuerySnapshots(
		ctx context.Context, groupID string, from, to time.Time,
		limit int, cursor string,
	) (QueryResult[model.OrderBookSnapshot], error)

	Close() error
}
