package runtime

import (
	"testing"

	"MarketDataBackend/internal/kafka"
	"MarketDataBackend/internal/model"
)

func validSnapshotJSON() []byte {
	return []byte(`{
		"event_time": "2023-11-18T10:00:00Z",
		"sequence": 1000,
		"bids": [
			{"price": "50000", "quantity": "2"},
			{"price": "49900", "quantity": "5"}
		],
		"asks": [
			{"price": "50100", "quantity": "3"}
		]
	}`)
}

func validSnapshotJSONWithExchangeTime() []byte {
	return []byte(`{
		"event_time": "2023-11-18T10:00:00Z",
		"exchange_time": "2023-11-18T10:00:00.500Z",
		"sequence": 1000,
		"bids": [{"price": "50000", "quantity": "2"}],
		"asks": [{"price": "50100", "quantity": "3"}]
	}`)
}

func compactSnapshotJSON() []byte {
	return []byte(`{
		"event_time": "2023-11-18T10:00:00Z",
		"sequence": 1000,
		"bids": [["50000", "2"], ["49900", "5"]],
		"asks": [["50100", "3"]]
	}`)
}

func makeSnapshotMsg(data []byte) kafka.Message {
	return kafka.Message{
		Topic:     "md.normalized.snap",
		Partition: 0,
		Offset:    42,
		Value:     data,
	}
}

func makeSnapshotInput() model.GroupInput {
	return model.GroupInput{
		InputID:    "binance:spot:BTCUSDT:orderbook_snapshot",
		GroupID:    "binance:spot:BTCUSDT",
		StreamKey:  "orderbook_snapshot",
		StreamKind: model.StreamKindOrderBookSnapshot,
	}
}

// --- decode ---------------------------------------------------------------

func TestDecodeOrderBookSnapshot_Valid(t *testing.T) {
	msg := makeSnapshotMsg(validSnapshotJSON())
	snap, err := buildOrderBookSnapshot(makeSnapshotInput(), msg)
	if err != nil {
		t.Fatalf("buildOrderBookSnapshot: %v", err)
	}
	if snap.GroupID != "binance:spot:BTCUSDT" {
		t.Errorf("GroupID = %q", snap.GroupID)
	}
	if *snap.Sequence != 1000 {
		t.Errorf("Sequence = %d, want 1000", *snap.Sequence)
	}
	if len(snap.Bids) != 2 {
		t.Errorf("bids = %d, want 2", len(snap.Bids))
	}
	if len(snap.Asks) != 1 {
		t.Errorf("asks = %d, want 1", len(snap.Asks))
	}
}

func TestDecodeOrderBookSnapshot_CompactLevels(t *testing.T) {
	msg := makeSnapshotMsg(compactSnapshotJSON())
	snap, err := buildOrderBookSnapshot(makeSnapshotInput(), msg)
	if err != nil {
		t.Fatalf("compact snapshot: %v", err)
	}
	if snap.Bids[0].Price != "50000" || snap.Bids[0].Quantity != "2" {
		t.Errorf("compact bids[0] = %+v", snap.Bids[0])
	}
}

func TestDecodeOrderBookSnapshot_WithExchangeTime(t *testing.T) {
	msg := makeSnapshotMsg(validSnapshotJSONWithExchangeTime())
	snap, err := buildOrderBookSnapshot(makeSnapshotInput(), msg)
	if err != nil {
		t.Fatalf("snapshot with exchange_time: %v", err)
	}
	if len(snap.Bids) != 1 {
		t.Errorf("bids = %d, want 1", len(snap.Bids))
	}
}

func TestDecodeOrderBookSnapshot_InvalidJSON(t *testing.T) {
	msg := makeSnapshotMsg([]byte(`not json`))
	_, err := buildOrderBookSnapshot(makeSnapshotInput(), msg)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestDecodeOrderBookSnapshot_MissingEventTime(t *testing.T) {
	msg := makeSnapshotMsg([]byte(`{
		"sequence": 1,
		"bids": [{"price": "100", "quantity": "1"}],
		"asks": []
	}`))
	_, err := buildOrderBookSnapshot(makeSnapshotInput(), msg)
	if err == nil {
		t.Fatal("expected error for missing event_time")
	}
}

func TestDecodeOrderBookSnapshot_EmptyBidsAndAsks(t *testing.T) {
	msg := makeSnapshotMsg([]byte(`{
		"event_time": "2023-11-18T10:00:00Z",
		"sequence": 1,
		"bids": [],
		"asks": []
	}`))
	_, err := buildOrderBookSnapshot(makeSnapshotInput(), msg)
	if err == nil {
		t.Fatal("expected error for empty bids and asks")
	}
}

func TestDecodeOrderBookSnapshot_NegativeSequence(t *testing.T) {
	msg := makeSnapshotMsg([]byte(`{
		"event_time": "2023-11-18T10:00:00Z",
		"sequence": -1,
		"bids": [{"price": "100", "quantity": "1"}],
		"asks": [{"price": "101", "quantity": "1"}]
	}`))
	_, err := buildOrderBookSnapshot(makeSnapshotInput(), msg)
	if err == nil {
		t.Fatal("expected error for negative sequence")
	}
}

func TestDecodeOrderBookSnapshot_InvalidBidPrice(t *testing.T) {
	msg := makeSnapshotMsg([]byte(`{
		"event_time": "2023-11-18T10:00:00Z",
		"sequence": 1,
		"bids": [{"price": "bad", "quantity": "1"}],
		"asks": [{"price": "101", "quantity": "1"}]
	}`))
	_, err := buildOrderBookSnapshot(makeSnapshotInput(), msg)
	if err == nil {
		t.Fatal("expected error for invalid bid price")
	}
}

func TestDecodeOrderBookSnapshot_TrailingData(t *testing.T) {
	msg := makeSnapshotMsg([]byte(`{
		"event_time": "2023-11-18T10:00:00Z",
		"sequence": 1,
		"bids": [{"price": "100", "quantity": "1"}],
		"asks": [{"price": "101", "quantity": "1"}]
	}{}`))
	_, err := buildOrderBookSnapshot(makeSnapshotInput(), msg)
	if err == nil {
		t.Fatal("expected error for trailing data")
	}
}
