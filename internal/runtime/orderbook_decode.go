package runtime

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"MarketDataBackend/internal/kafka"
	"MarketDataBackend/internal/model"
)

type orderBookDeltaMessage struct {
	EventTime        flexTime    `json:"event_time"`
	ExchangeTime     *flexTime   `json:"exchange_time"`
	LocalReceiveTime *flexTime   `json:"local_receive_time"`
	RawEventID       flexString  `json:"raw_event_id"`
	FirstUpdateID    *flexInt64  `json:"first_update_id"`
	LastUpdateID     *flexInt64  `json:"last_update_id"`
	PrevUpdateID     *flexInt64  `json:"prev_update_id"`
	Sequence         *flexInt64  `json:"sequence"`
	Bids             priceLevels `json:"bids"`
	Asks             priceLevels `json:"asks"`
}

func decodeOrderBookDelta(data []byte) (orderBookDeltaMessage, error) {
	var dm orderBookDeltaMessage
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&dm); err != nil {
		return orderBookDeltaMessage{}, fmt.Errorf("decode orderbook delta: %w", err)
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return orderBookDeltaMessage{}, fmt.Errorf(
				"decode orderbook delta: multiple JSON values",
			)
		}
		return orderBookDeltaMessage{}, fmt.Errorf(
			"decode orderbook delta: trailing data: %w", err,
		)
	}
	return dm, nil
}

func (dm orderBookDeltaMessage) validate() error {
	if !dm.EventTime.set {
		return fmt.Errorf("orderbook delta: missing event_time")
	}
	for _, field := range []struct {
		name  string
		value *flexInt64
	}{
		{"first_update_id", dm.FirstUpdateID},
		{"last_update_id", dm.LastUpdateID},
		{"prev_update_id", dm.PrevUpdateID},
		{"sequence", dm.Sequence},
	} {
		if field.value != nil && field.value.value < 0 {
			return fmt.Errorf("orderbook delta: %s must be non-negative", field.name)
		}
	}
	if dm.FirstUpdateID != nil && dm.LastUpdateID != nil &&
		dm.FirstUpdateID.value > dm.LastUpdateID.value {
		return fmt.Errorf("orderbook delta: first_update_id must not exceed last_update_id")
	}
	if len(dm.Bids) == 0 && len(dm.Asks) == 0 {
		return fmt.Errorf("orderbook delta: bids and asks cannot both be empty")
	}
	if err := validatePriceLevels("bids", dm.Bids); err != nil {
		return err
	}
	if err := validatePriceLevels("asks", dm.Asks); err != nil {
		return err
	}
	return nil
}

func (dm orderBookDeltaMessage) toOrderBookDelta(
	in model.GroupInput, msg kafka.Message,
) model.OrderBookDelta {
	delta := model.OrderBookDelta{
		GroupID:        in.GroupID,
		InputID:        inputID(in),
		EventTime:      dm.EventTime.t,
		RawEventID:     strings.TrimSpace(string(dm.RawEventID)),
		FirstUpdateID:  int64Ptr(dm.FirstUpdateID),
		LastUpdateID:   int64Ptr(dm.LastUpdateID),
		PrevUpdateID:   int64Ptr(dm.PrevUpdateID),
		Sequence:       int64Ptr(dm.Sequence),
		Bids:           append([]model.PriceLevel(nil), dm.Bids...),
		Asks:           append([]model.PriceLevel(nil), dm.Asks...),
		KafkaTopic:     msg.Topic,
		KafkaPartition: msg.Partition,
		KafkaOffset:    msg.Offset,
	}
	if dm.ExchangeTime != nil && dm.ExchangeTime.set {
		value := dm.ExchangeTime.t
		delta.ExchangeTime = &value
	}
	if dm.LocalReceiveTime != nil && dm.LocalReceiveTime.set {
		value := dm.LocalReceiveTime.t
		delta.LocalReceiveTime = &value
	}
	return delta
}

func buildOrderBookDelta(
	in model.GroupInput, msg kafka.Message,
) (model.OrderBookDelta, error) {
	dm, err := decodeOrderBookDelta(msg.Value)
	if err != nil {
		return model.OrderBookDelta{}, err
	}
	if err := dm.validate(); err != nil {
		return model.OrderBookDelta{}, err
	}
	return dm.toOrderBookDelta(in, msg), nil
}

func validatePriceLevels(side string, levels []model.PriceLevel) error {
	for i, level := range levels {
		if !validPositiveNumeric(level.Price) {
			return fmt.Errorf(
				"orderbook delta: invalid %s[%d] price %q", side, i, level.Price,
			)
		}
		if !validNonNegativeNumeric(level.Quantity) {
			return fmt.Errorf(
				"orderbook delta: invalid %s[%d] quantity %q",
				side, i, level.Quantity,
			)
		}
	}
	return nil
}

// priceLevels accepts both the canonical object representation and the common
// compact two-element representation:
// [{"price":"100","quantity":"2"}] or [["100","2"]].
type priceLevels []model.PriceLevel

func (p *priceLevels) UnmarshalJSON(data []byte) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		*p = nil
		return nil
	}
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("price levels must be an array: %w", err)
	}
	levels := make([]model.PriceLevel, 0, len(raw))
	for i, item := range raw {
		level, err := decodePriceLevel(item)
		if err != nil {
			return fmt.Errorf("price level %d: %w", i, err)
		}
		levels = append(levels, level)
	}
	*p = levels
	return nil
}

func decodePriceLevel(data []byte) (model.PriceLevel, error) {
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return model.PriceLevel{}, fmt.Errorf("empty value")
	}
	if data[0] == '[' {
		var values []flexString
		if err := json.Unmarshal(data, &values); err != nil {
			return model.PriceLevel{}, err
		}
		if len(values) != 2 {
			return model.PriceLevel{}, fmt.Errorf("compact level must have price and quantity")
		}
		return model.PriceLevel{
			Price:    strings.TrimSpace(string(values[0])),
			Quantity: strings.TrimSpace(string(values[1])),
		}, nil
	}

	var level struct {
		Price    flexString `json:"price"`
		Quantity flexString `json:"quantity"`
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&level); err != nil {
		return model.PriceLevel{}, err
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return model.PriceLevel{}, fmt.Errorf("trailing data")
	}
	return model.PriceLevel{
		Price:    strings.TrimSpace(string(level.Price)),
		Quantity: strings.TrimSpace(string(level.Quantity)),
	}, nil
}

func int64Ptr(value *flexInt64) *int64 {
	if value == nil {
		return nil
	}
	v := value.value
	return &v
}
