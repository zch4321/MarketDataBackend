// Package model holds the domain types shared by the control-plane and the
// stream-runtime. Numeric market values (price/quantity/volume) are kept as
// strings so they can round-trip through PostgreSQL numeric columns without
// losing precision to float64. Higher milestones may swap these for a decimal
// type, but the column contract stays the same.
package model

import "time"

// Desired status values stored in market_groups.desired_status and
// group_inputs.desired_status.
const (
	DesiredStatusRunning  = "running"
	DesiredStatusPaused   = "paused"
	DesiredStatusDisabled = "disabled"
)

// Stream kinds stored in group_inputs.stream_kind.
const (
	StreamKindTrade             = "trade"
	StreamKindKline             = "kline"
	StreamKindOrderBookDelta    = "orderbook_delta"
	StreamKindOrderBookSnapshot = "orderbook_snapshot"
)

// Market types stored in market_groups.market_type.
const (
	MarketTypeSpot    = "spot"
	MarketTypeMargin  = "margin"
	MarketTypeFutures = "futures"
	MarketTypeSwap    = "swap"
	MarketTypeOption  = "option"
)

// Trade side values.
const (
	TradeSideBuy  = "buy"
	TradeSideSell = "sell"
)

// Kline source values.
const (
	KlineSourceExchange = "exchange"
	KlineSourceComputed = "computed"
)

// Runtime node status values.
const (
	NodeStatusAlive    = "alive"
	NodeStatusDraining = "draining"
	NodeStatusDead     = "dead"
)

// Observed (actual) stream status values reported by the runtime.
const (
	ActualStatusPending  = "pending"
	ActualStatusStarting = "starting"
	ActualStatusRunning  = "running"
	ActualStatusPaused   = "paused"
	ActualStatusError    = "error"
	ActualStatusStopped  = "stopped"
)

// DesiredStatuses is the canonical set of allowed desired_status values.
var DesiredStatuses = map[string]struct{}{
	DesiredStatusRunning:  {},
	DesiredStatusPaused:   {},
	DesiredStatusDisabled: {},
}

// StreamKinds is the canonical set of allowed stream_kind values.
var StreamKinds = map[string]struct{}{
	StreamKindTrade:             {},
	StreamKindKline:             {},
	StreamKindOrderBookDelta:    {},
	StreamKindOrderBookSnapshot: {},
}

// MarketTypes is the canonical set of allowed market_type values.
var MarketTypes = map[string]struct{}{
	MarketTypeSpot:    {},
	MarketTypeMargin:  {},
	MarketTypeFutures: {},
	MarketTypeSwap:    {},
	MarketTypeOption:  {},
}

// IsValidDesiredStatus reports whether s is an allowed desired_status.
func IsValidDesiredStatus(s string) bool {
	_, ok := DesiredStatuses[s]
	return ok
}

// IsValidStreamKind reports whether s is an allowed stream_kind.
func IsValidStreamKind(s string) bool {
	_, ok := StreamKinds[s]
	return ok
}

// IsValidMarketType reports whether s is an allowed market_type.
func IsValidMarketType(s string) bool {
	_, ok := MarketTypes[s]
	return ok
}

// NodeStatuses is the canonical set of allowed runtime node status values.
var NodeStatuses = map[string]struct{}{
	NodeStatusAlive:    {},
	NodeStatusDraining: {},
	NodeStatusDead:     {},
}

// IsValidNodeStatus reports whether s is an allowed runtime node status.
func IsValidNodeStatus(s string) bool {
	_, ok := NodeStatuses[s]
	return ok
}

// MarketGroup is one market-symbol (exchange + market_type + symbol).
type MarketGroup struct {
	GroupID       string
	Exchange      string
	MarketType    string
	Symbol        string
	BaseAsset     string
	QuoteAsset    string
	DesiredStatus string
	Weight        int
	Inputs        []GroupInput
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// GroupInput is one input stream under a MarketGroup.
type GroupInput struct {
	InputID       string
	GroupID       string
	StreamKey     string
	StreamKind    string
	Interval      string
	Enabled       bool
	KafkaCluster  string
	KafkaTopic    string
	KafkaGroupID  string
	DesiredStatus string
	SchemaVersion int
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// RuntimeNode is a live md-stream-runtime process.
type RuntimeNode struct {
	NodeID          string
	Hostname        string
	PodName         string
	Status          string
	MaxGroups       int
	CurrentGroups   int
	MaxWeight       int
	CurrentWeight   int
	LastHeartbeatAt time.Time
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// RuntimeCapacity is the mutable capacity portion reported on heartbeat.
type RuntimeCapacity struct {
	CurrentGroups int
	CurrentWeight int
}

// GroupLease describes the current owner of a market group's lease. It is used
// by the control-plane API to explain which runtime node is processing a group.
type GroupLease struct {
	GroupID        string
	NodeID         string
	LeaseExpiresAt time.Time
	Version        int64
	AcquiredAt     time.Time
	UpdatedAt      time.Time
}

// StreamRuntimeStatus is the observed runtime state of a single input stream.
type StreamRuntimeStatus struct {
	InputID             string
	GroupID             string
	StreamKey           string
	NodeID              string
	ActualStatus        string
	KafkaPartition      *int
	KafkaLag            *int64
	CommittedOffset     *int64
	HighWatermarkOffset *int64
	LastEventTime       *time.Time
	LastProcessedTime   *time.Time
	LastError           string
	UpdatedAt           time.Time
}

// PriceLevel is a single order book level. Price and Quantity are decimal
// strings.
type PriceLevel struct {
	Price    string `json:"price"`
	Quantity string `json:"quantity"`
}

// Trade is a normalized trade fact row.
type Trade struct {
	ID               int64 // auto-increment primary key
	GroupID          string
	InputID          string
	EventTime        time.Time
	ExchangeTime     *time.Time
	LocalReceiveTime *time.Time
	TradeID          string
	RawTradeID       string
	Price            string
	Quantity         string
	Side             string
	IsAggregated     bool
	KafkaTopic       string
	KafkaPartition   int
	KafkaOffset      int64
	SchemaVersion    int
	IngestedAt       time.Time
}

// Kline is a normalized candlestick fact row.
type Kline struct {
	GroupID        string
	InputID        string
	Source         string
	Interval       string
	OpenTime       time.Time
	CloseTime      time.Time
	Open           string
	High           string
	Low            string
	Close          string
	Volume         string
	QuoteVolume    string
	TradeCount     int64
	IsClosed       bool
	Revision       int64
	KafkaTopic     string
	KafkaPartition int
	KafkaOffset    int64
	UpdatedAt      time.Time
}

// OrderBookDelta is a normalized order book increment fact row.
type OrderBookDelta struct {
	GroupID          string
	InputID          string
	EventTime        time.Time
	ExchangeTime     *time.Time
	LocalReceiveTime *time.Time
	RawEventID       string
	FirstUpdateID    *int64
	LastUpdateID     *int64
	PrevUpdateID     *int64
	Sequence         *int64
	Bids             []PriceLevel
	Asks             []PriceLevel
	RawPayload       []byte
	KafkaTopic       string
	KafkaPartition   int
	KafkaOffset      int64
	IngestedAt       time.Time
}

// StreamWriteProgress is the database-backed high-water mark committed in the
// same transaction as a fact batch. It makes a database-success/Kafka-commit
// failure replayable without duplicating partitioned order-book facts.
type StreamWriteProgress struct {
	InputID        string
	KafkaPartition int
	DurableOffset  int64
	LastRawEventID string
	LastUpdateID   *int64
	LastSequence   *int64
	UpdatedAt      time.Time
}

// OrderBookSnapshot is a point-in-time order book fact row. bids are stored in
// descending price order, asks ascending. depth_limit was removed in M8 (migration
// 000005); internally-generated snapshots are always full-depth.
type OrderBookSnapshot struct {
	SnapshotID   int64 // auto-increment primary key
	GroupID      string
	InputID      string
	SnapshotTime time.Time
	Sequence     *int64
	Bids         []PriceLevel
	Asks         []PriceLevel
	CreatedAt    time.Time
}

// TradeMetric, BookMetric and CrossMetric are derived metric rows. They are
// only sketched here; their write paths land in M9.
type TradeMetric struct {
	GroupID       string
	BucketTime    time.Time
	MetricVersion int
	Volume        string
	QuoteVolume   string
	TradeCount    int64
	VWAP          string
	BuyVolume     string
	SellVolume    string
	ComputedAt    time.Time
}

type BookMetric struct {
	GroupID       string
	BucketTime    time.Time
	MetricVersion int
	AvgSpread     string
	MinSpread     string
	MaxSpread     string
	AvgDepth      string
	AvgImbalance  string
	ComputedAt    time.Time
}

type CrossMetric struct {
	GroupID       string
	BucketTime    time.Time
	MetricVersion int
	ComputedAt    time.Time
}

// PoisonRecord is a Kafka message that could not be decoded or validated.
// It is persisted atomically so the runtime can safely skip the offset without
// losing visibility of the problematic payload (M10 dead-letter path).
type PoisonRecord struct {
	InputID      string
	GroupID      string
	StreamKind   string
	Topic        string
	Partition    int
	Offset       int64
	ErrorMessage string
	RawPayload   []byte
	RecordedAt   time.Time // set by the database layer (DEFAULT now())
}
