package model

import (
	"fmt"
	"strings"
)

// klineStreamKeyPrefix is the stream_key prefix used for exchange kline inputs.
// A kline stream_key is "kline_<interval>", e.g. "kline_1m".
const klineStreamKeyPrefix = "kline_"

// NewGroupID builds the canonical group_id for a market symbol as
// "<exchange>:<market_type>:<symbol>", e.g. "binance:spot:BTCUSDT".
func NewGroupID(exchange, marketType, symbol string) string {
	return fmt.Sprintf("%s:%s:%s",
		strings.TrimSpace(exchange),
		strings.TrimSpace(marketType),
		strings.TrimSpace(symbol),
	)
}

// NewInputID builds the canonical input_id for an input stream as
// "<group_id>:<stream_key>". (group_id, stream_key) is unique per group, so the
// derived id is stable and collision free.
func NewInputID(groupID, streamKey string) string {
	return groupID + ":" + streamKey
}

// DefaultKafkaGroupID builds the default Kafka consumer group for an input as
// "md-runtime.<group_id>.<stream_key>". Deriving it from stable identifiers
// keeps a single input pinned to one consumer group across restarts and avoids
// client-side misconfiguration.
func DefaultKafkaGroupID(groupID, streamKey string) string {
	return "md-runtime." + groupID + "." + streamKey
}

// StreamKeyFor builds the stream_key for a (stream_kind, interval) pair. kline
// requires a non-empty interval; the other kinds must not carry one.
func StreamKeyFor(kind, interval string) (string, error) {
	switch kind {
	case StreamKindTrade, StreamKindOrderBookDelta, StreamKindOrderBookSnapshot:
		if interval != "" {
			return "", fmt.Errorf("model: stream_kind %q must not carry an interval", kind)
		}
		return kind, nil
	case StreamKindKline:
		if interval == "" {
			return "", fmt.Errorf("model: stream_kind kline requires an interval")
		}
		return klineStreamKeyPrefix + interval, nil
	default:
		return "", fmt.Errorf("model: invalid stream_kind %q", kind)
	}
}

// ParseStreamKey derives the (stream_kind, interval) pair from a stream_key. It
// is the inverse of StreamKeyFor.
func ParseStreamKey(streamKey string) (kind, interval string, err error) {
	switch streamKey {
	case StreamKindTrade, StreamKindOrderBookDelta, StreamKindOrderBookSnapshot:
		return streamKey, "", nil
	}
	if strings.HasPrefix(streamKey, klineStreamKeyPrefix) {
		interval = strings.TrimPrefix(streamKey, klineStreamKeyPrefix)
		if interval == "" {
			return "", "", fmt.Errorf("model: kline stream_key %q is missing its interval", streamKey)
		}
		return StreamKindKline, interval, nil
	}
	return "", "", fmt.Errorf("model: unrecognized stream_key %q", streamKey)
}
