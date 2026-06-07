package runtime

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"MarketDataBackend/internal/kafka"
	"MarketDataBackend/internal/model"
)

// orderBookSnapshotMessage is the normalized orderbook snapshot payload
// delivered on the orderbook_snapshot Kafka topic.  It is a complete
// point-in-time view produced by the upstream adapter.  Numeric ids and
// timestamps use the same flexString / flexTime helpers as the other
// message types so producers are free to encode them as strings or numbers.
type orderBookSnapshotMessage struct {
	EventTime        flexTime    `json:"event_time"`
	ExchangeTime     *flexTime   `json:"exchange_time"`
	LocalReceiveTime *flexTime   `json:"local_receive_time"`
	Sequence         *flexInt64  `json:"sequence"`
	Bids             priceLevels `json:"bids"`
	Asks             priceLevels `json:"asks"`
}

func decodeOrderBookSnapshot(data []byte) (orderBookSnapshotMessage, error) {
	var sm orderBookSnapshotMessage
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&sm); err != nil {
		return orderBookSnapshotMessage{},
			fmt.Errorf("decode orderbook snapshot: %w", err)
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return orderBookSnapshotMessage{},
				fmt.Errorf("decode orderbook snapshot: multiple JSON values")
		}
		return orderBookSnapshotMessage{},
			fmt.Errorf("decode orderbook snapshot: trailing data: %w", err)
	}
	return sm, nil
}

func (sm orderBookSnapshotMessage) validate() error {
	if !sm.EventTime.set {
		return fmt.Errorf("orderbook snapshot: missing event_time")
	}
	if sm.Sequence != nil && sm.Sequence.value < 0 {
		return fmt.Errorf("orderbook snapshot: sequence must be non-negative")
	}
	if len(sm.Bids) == 0 && len(sm.Asks) == 0 {
		return fmt.Errorf("orderbook snapshot: bids and asks cannot both be empty")
	}
	if err := validatePriceLevels("bids", sm.Bids); err != nil {
		return err
	}
	if err := validatePriceLevels("asks", sm.Asks); err != nil {
		return err
	}
	return nil
}

func (sm orderBookSnapshotMessage) toOrderBookSnapshot(
	in model.GroupInput, msg kafka.Message,
) model.OrderBookSnapshot {
	snap := model.OrderBookSnapshot{
		GroupID:      in.GroupID,
		InputID:      inputID(in),
		SnapshotTime: sm.EventTime.t,
		Sequence:     int64Ptr(sm.Sequence),
	}
	snap.Bids = make([]model.PriceLevel, len(sm.Bids))
	copy(snap.Bids, sm.Bids)
	snap.Asks = make([]model.PriceLevel, len(sm.Asks))
	copy(snap.Asks, sm.Asks)
	return snap
}

func buildOrderBookSnapshot(
	in model.GroupInput, msg kafka.Message,
) (model.OrderBookSnapshot, error) {
	sm, err := decodeOrderBookSnapshot(msg.Value)
	if err != nil {
		return model.OrderBookSnapshot{}, err
	}
	if err := sm.validate(); err != nil {
		return model.OrderBookSnapshot{}, err
	}
	return sm.toOrderBookSnapshot(in, msg), nil
}

// int64Ptr is defined in orderbook_decode.go and converts *flexInt64 → *int64.
