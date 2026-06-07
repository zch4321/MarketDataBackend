package runtime

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"

	"MarketDataBackend/internal/kafka"
	"MarketDataBackend/internal/model"
)

// tradeMessage is the normalized trade payload carried on the
// md.normalized.<exchange>.<market_type>.<symbol>.trade topics. Numeric and id
// fields use flexString so a producer may encode them as either a JSON string
// or a JSON number; timestamps use flexTime so they may be RFC3339 strings or
// epoch-millisecond numbers.
type tradeMessage struct {
	EventTime        flexTime   `json:"event_time"`
	ExchangeTime     *flexTime  `json:"exchange_time"`
	LocalReceiveTime *flexTime  `json:"local_receive_time"`
	TradeID          flexString `json:"trade_id"`
	RawTradeID       flexString `json:"raw_trade_id"`
	Price            flexString `json:"price"`
	Quantity         flexString `json:"quantity"`
	Side             string     `json:"side"`
	IsAggregated     bool       `json:"is_aggregated"`
}

// decodeTrade unmarshals a raw Kafka value into a tradeMessage. It rejects
// unknown fields so a producer/consumer schema drift surfaces as an error
// instead of silently dropping data.
func decodeTrade(data []byte) (tradeMessage, error) {
	var tm tradeMessage
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&tm); err != nil {
		return tradeMessage{}, fmt.Errorf("decode trade: %w", err)
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return tradeMessage{}, fmt.Errorf("decode trade: multiple JSON values")
		}
		return tradeMessage{}, fmt.Errorf("decode trade: trailing data: %w", err)
	}
	return tm, nil
}

// validate checks the required fields and returns the normalized side. It is
// the schema-validation step: a missing event_time, an unparseable price or
// quantity, or an unknown side are all rejected.
func (tm tradeMessage) validate() (side string, err error) {
	if !tm.EventTime.set {
		return "", fmt.Errorf("trade: missing event_time")
	}
	if strings.TrimSpace(string(tm.Price)) == "" {
		return "", fmt.Errorf("trade: missing price")
	}
	if !validPositiveNumeric(string(tm.Price)) {
		return "", fmt.Errorf("trade: invalid price %q", string(tm.Price))
	}
	if strings.TrimSpace(string(tm.Quantity)) == "" {
		return "", fmt.Errorf("trade: missing quantity")
	}
	if !validPositiveNumeric(string(tm.Quantity)) {
		return "", fmt.Errorf("trade: invalid quantity %q", string(tm.Quantity))
	}
	side, err = normalizeSide(tm.Side)
	if err != nil {
		return "", err
	}
	return side, nil
}

// toTrade maps a validated message and its Kafka coordinates to a Trade fact
// row. The Kafka coordinates are always populated so the (input_id, partition,
// offset) fallback idempotency key works even when raw_trade_id is absent.
func (tm tradeMessage) toTrade(in model.GroupInput, msg kafka.Message, side string) model.Trade {
	t := model.Trade{
		GroupID:        in.GroupID,
		InputID:        inputID(in),
		EventTime:      tm.EventTime.t,
		TradeID:        strings.TrimSpace(string(tm.TradeID)),
		RawTradeID:     strings.TrimSpace(string(tm.RawTradeID)),
		Price:          strings.TrimSpace(string(tm.Price)),
		Quantity:       strings.TrimSpace(string(tm.Quantity)),
		Side:           side,
		IsAggregated:   tm.IsAggregated,
		KafkaTopic:     msg.Topic,
		KafkaPartition: msg.Partition,
		KafkaOffset:    msg.Offset,
		SchemaVersion:  schemaVersionOrDefault(in.SchemaVersion),
	}
	if tm.ExchangeTime != nil && tm.ExchangeTime.set {
		v := tm.ExchangeTime.t
		t.ExchangeTime = &v
	}
	if tm.LocalReceiveTime != nil && tm.LocalReceiveTime.set {
		v := tm.LocalReceiveTime.t
		t.LocalReceiveTime = &v
	}
	return t
}

// buildTrade decodes, validates and maps a Kafka message to a Trade row. A
// decode or validation failure is returned to the caller, which records it as a
// last_error and skips the poison message.
func buildTrade(in model.GroupInput, msg kafka.Message) (model.Trade, error) {
	tm, err := decodeTrade(msg.Value)
	if err != nil {
		return model.Trade{}, err
	}
	side, err := tm.validate()
	if err != nil {
		return model.Trade{}, err
	}
	return tm.toTrade(in, msg, side), nil
}

// normalizeSide maps the common buy/sell spellings used by exchanges to the
// canonical model values. An unrecognized side is an error.
func normalizeSide(s string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "buy", "b", "bid":
		return model.TradeSideBuy, nil
	case "sell", "s", "ask":
		return model.TradeSideSell, nil
	default:
		return "", fmt.Errorf("trade: invalid side %q", s)
	}
}

var decimalPattern = regexp.MustCompile(
	`^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$`,
)

// validPositiveNumeric accepts PostgreSQL-compatible decimal literals while
// rejecting zero, negatives, fractions such as "1/2", NaN and infinities.
func validPositiveNumeric(s string) bool {
	s = strings.TrimSpace(s)
	if !decimalPattern.MatchString(s) {
		return false
	}
	v, _, err := big.ParseFloat(s, 10, 256, big.ToNearestEven)
	return err == nil && v.Sign() > 0
}

func validNonNegativeNumeric(s string) bool {
	s = strings.TrimSpace(s)
	if !decimalPattern.MatchString(s) {
		return false
	}
	v, _, err := big.ParseFloat(s, 10, 256, big.ToNearestEven)
	return err == nil && v.Sign() >= 0
}

func compareNumeric(a, b string) int {
	av, _, _ := big.ParseFloat(strings.TrimSpace(a), 10, 256, big.ToNearestEven)
	bv, _, _ := big.ParseFloat(strings.TrimSpace(b), 10, 256, big.ToNearestEven)
	return av.Cmp(bv)
}

// schemaVersionOrDefault mirrors the storage layer: a non-positive schema
// version defaults to 1.
func schemaVersionOrDefault(v int) int {
	if v <= 0 {
		return 1
	}
	return v
}

// inputID returns the input's id, deriving it from (group_id, stream_key) when
// the stored id is empty.
func inputID(in model.GroupInput) string {
	if in.InputID != "" {
		return in.InputID
	}
	return model.NewInputID(in.GroupID, in.StreamKey)
}

// consumerGroupID returns the Kafka consumer group for an input, falling back to
// the canonical default so a single input stays pinned to one group.
func consumerGroupID(in model.GroupInput) string {
	if in.KafkaGroupID != "" {
		return in.KafkaGroupID
	}
	return model.DefaultKafkaGroupID(in.GroupID, in.StreamKey)
}

// flexString unmarshals from either a JSON string or a JSON number, always
// yielding the textual form. This tolerates exchanges that encode ids and
// decimal values inconsistently.
type flexString string

func (f *flexString) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	if len(b) == 0 || string(b) == "null" {
		*f = ""
		return nil
	}
	if b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		*f = flexString(s)
		return nil
	}
	if b[0] == '{' || b[0] == '[' || string(b) == "true" || string(b) == "false" {
		return fmt.Errorf("expected string or number")
	}
	*f = flexString(strings.Trim(string(b), `"`))
	return nil
}

type flexInt64 struct {
	value int64
}

func (f *flexInt64) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	if len(b) == 0 || string(b) == "null" {
		return nil
	}
	if b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		b = []byte(strings.TrimSpace(s))
	}
	value, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return fmt.Errorf("expected integer: %w", err)
	}
	f.value = value
	return nil
}

// flexTime unmarshals from an RFC3339(/nano) string, a numeric epoch in
// milliseconds, or a numeric string. set reports whether a value was present so
// an omitted optional timestamp stays nil rather than becoming the zero time.
type flexTime struct {
	t   time.Time
	set bool
}

func (f *flexTime) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	if len(b) == 0 || string(b) == "null" {
		return nil
	}
	if b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		if strings.TrimSpace(s) == "" {
			return nil
		}
		if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
			f.t, f.set = t.UTC(), true
			return nil
		}
		if ms, err := strconv.ParseInt(s, 10, 64); err == nil {
			f.t, f.set = time.UnixMilli(ms).UTC(), true
			return nil
		}
		return fmt.Errorf("invalid time %q", s)
	}
	ms, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		// Tolerate a fractional epoch by truncating to whole milliseconds.
		if fv, ferr := strconv.ParseFloat(string(b), 64); ferr == nil {
			f.t, f.set = time.UnixMilli(int64(fv)).UTC(), true
			return nil
		}
		return fmt.Errorf("invalid epoch time %q: %w", string(b), err)
	}
	f.t, f.set = time.UnixMilli(ms).UTC(), true
	return nil
}
