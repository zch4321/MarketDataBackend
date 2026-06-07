package runtime

import (
	"testing"
	"time"

	"MarketDataBackend/internal/model"
)

func sampleTestSnapshot(seq int64) model.OrderBookSnapshot {
	return model.OrderBookSnapshot{
		GroupID:      "binance:spot:BTCUSDT",
		InputID:      "binance:spot:BTCUSDT:orderbook_snapshot",
		SnapshotTime: time.Unix(1_700_000_000, 0).UTC(),
		Sequence:     int64PtrVal(seq),
		Bids: []model.PriceLevel{
			{Price: "50000", Quantity: "2"},
			{Price: "49900", Quantity: "5"},
			{Price: "49800", Quantity: "10"},
		},
		Asks: []model.PriceLevel{
			{Price: "50100", Quantity: "3"},
			{Price: "50200", Quantity: "1"},
		},
	}
}

func sampleDelta(firstUpdateID, lastUpdateID int64, seq ...int64) model.OrderBookDelta {
	var sequence *int64
	if len(seq) > 0 {
		sequence = int64PtrVal(seq[0])
	} else {
		sequence = int64PtrVal(lastUpdateID)
	}
	return model.OrderBookDelta{
		GroupID:       "binance:spot:BTCUSDT",
		InputID:       "binance:spot:BTCUSDT:orderbook_delta",
		EventTime:     time.Unix(1_700_000_001, 0).UTC(),
		FirstUpdateID: int64PtrVal(firstUpdateID),
		LastUpdateID:  int64PtrVal(lastUpdateID),
		Sequence:      sequence,
		Bids: []model.PriceLevel{
			{Price: "50000", Quantity: "3"},
			{Price: "49950", Quantity: "1"},
		},
		Asks: []model.PriceLevel{
			{Price: "50100", Quantity: "0"},
		},
	}
}

func sampleDeltaSimple(lastUpdateID int64) model.OrderBookDelta {
	return sampleDelta(lastUpdateID, lastUpdateID, lastUpdateID)
}

func int64PtrVal(v int64) *int64 { return &v }

// --- applySnapshot --------------------------------------------------------

func TestOrderBookApplySnapshot_InitializesBidsAsks(t *testing.T) {
	s := newOrderBookState("binance:spot:BTCUSDT", "snapinput")
	snap := sampleTestSnapshot(100)

	if err := s.applySnapshot(snap); err != nil {
		t.Fatalf("applySnapshot: %v", err)
	}
	if !s.isReady() {
		t.Fatal("expected ready after snapshot")
	}
	if got := len(s.bids); got != 3 {
		t.Errorf("bids count = %d, want 3", got)
	}
	if got := len(s.asks); got != 2 {
		t.Errorf("asks count = %d, want 2", got)
	}
	if s.bids["50000"] != "2" {
		t.Errorf("bids[50000] = %q, want 2", s.bids["50000"])
	}
}

func TestOrderBookApplySnapshot_ReplacesPreviousState(t *testing.T) {
	s := newOrderBookState("g", "si")
	_ = s.applySnapshot(sampleTestSnapshot(100))
	_ = s.applySnapshot(sampleTestSnapshot(200)) // different sequence

	if *s.sequence != 200 {
		t.Errorf("sequence = %d, want 200", *s.sequence)
	}
}

func TestOrderBookApplySnapshot_SkipsZeroQuantity(t *testing.T) {
	s := newOrderBookState("g", "si")
	snap := sampleTestSnapshot(100)
	snap.Bids = []model.PriceLevel{
		{Price: "50000", Quantity: "0"},
		{Price: "49900", Quantity: "0.0"},
		{Price: "49800", Quantity: "1"},
	}
	if err := s.applySnapshot(snap); err != nil {
		t.Fatalf("applySnapshot: %v", err)
	}
	if len(s.bids) != 1 {
		t.Errorf("bids = %d, want 1 non-zero level", len(s.bids))
	}
	if s.bids["49800"] != "1" {
		t.Errorf("bids[49800] = %q, want 1", s.bids["49800"])
	}
}

// --- applyDelta -----------------------------------------------------------

func TestOrderBookApplyDelta_UpdatesBidsAndRemovesZeroAsk(t *testing.T) {
	s := newOrderBookState("g", "si")
	_ = s.applySnapshot(sampleTestSnapshot(100))

	delta := sampleDeltaSimple(101)
	applied, err := s.applyDelta(delta)
	if err != nil {
		t.Fatalf("applyDelta: %v", err)
	}
	if !applied {
		t.Fatal("delta was not applied")
	}
	if s.bids["50000"] != "3" {
		t.Errorf("bids[50000] = %q, want 3", s.bids["50000"])
	}
	if s.bids["49950"] != "1" {
		t.Errorf("bids[49950] = %q, want 1", s.bids["49950"])
	}
	if _, exists := s.asks["50100"]; exists {
		t.Error("asks[50100] should have been removed (quantity=0)")
	}
}

func TestOrderBookApplyDelta_BuffersBeforeSnapshot(t *testing.T) {
	s := newOrderBookState("g", "si")
	delta := sampleDeltaSimple(101)
	applied, err := s.applyDelta(delta)
	if err != nil {
		t.Fatalf("applyDelta: %v", err)
	}
	if applied {
		t.Error("delta should not be applied before snapshot")
	}
	if len(s.pending) != 1 {
		t.Fatalf("pending = %d, want 1", len(s.pending))
	}
}

func TestOrderBookApplyDelta_DrainsPendingAfterSnapshot(t *testing.T) {
	s := newOrderBookState("g", "si")

	// Buffer two deltas before the snapshot.
	_, _ = s.applyDelta(sampleDelta(11, 11, 11))
	_, _ = s.applyDelta(sampleDelta(12, 12, 12))

	// Apply snapshot that is ahead of the pending deltas (sequence 10 vs 11).
	_ = s.applySnapshot(sampleTestSnapshot(10))

	// Next delta (seq 13) should drain pending deltas (11,12) then apply itself.
	delta := sampleDelta(13, 13, 13)
	applied, err := s.applyDelta(delta)
	if err != nil {
		t.Fatalf("applyDelta after snapshot drained: %v", err)
	}
	if !applied {
		t.Fatal("final delta was not applied")
	}
	if len(s.pending) != 0 {
		t.Errorf("pending = %d, want 0", len(s.pending))
	}
	if *s.lastUpdateID != 13 {
		t.Errorf("lastUpdateID = %d, want 13", *s.lastUpdateID)
	}
}

// --- sequence gap ---------------------------------------------------------

func TestOrderBookSequenceGap_ReturnsError(t *testing.T) {
	s := newOrderBookState("g", "si")
	_ = s.applySnapshot(sampleTestSnapshot(100))

	// Good: 100 → 101
	_, err := s.applyDelta(sampleDeltaSimple(101))
	if err != nil {
		t.Fatalf("first delta: %v", err)
	}

	// Gap: expected 102, got 105
	_, err = s.applyDelta(sampleDelta(105, 105, 105))
	if err == nil {
		t.Fatal("expected sequence gap error")
	}
}

func TestOrderBookSequenceGap_TriggersReset(t *testing.T) {
	s := newOrderBookState("g", "si")
	_ = s.applySnapshot(sampleTestSnapshot(100))
	_, _ = s.applyDelta(sampleDeltaSimple(101))

	// Simulate what onDeltaApplied does on a gap.
	_, err := s.applyDelta(sampleDelta(105, 105, 105))
	if err != nil {
		s.reset()
	}

	if s.isReady() {
		t.Fatal("expected not ready after reset")
	}
	if len(s.bids) != 0 || len(s.asks) != 0 {
		t.Errorf("bids=%d asks=%d, want empty", len(s.bids), len(s.asks))
	}
	if s.sequence != nil {
		t.Error("sequence should be nil after reset")
	}
}

// --- generateSnapshot -----------------------------------------------------

func TestOrderBookGenerateSnapshot_BidsDescAsksAsc(t *testing.T) {
	s := newOrderBookState("g", "si")
	_ = s.applySnapshot(sampleTestSnapshot(100))

	snap := s.generateSnapshot(time.Unix(1_700_000_100, 0).UTC())
	if len(snap.Bids) != 3 {
		t.Fatalf("bids = %d, want 3", len(snap.Bids))
	}
	if len(snap.Asks) != 2 {
		t.Fatalf("asks = %d, want 2", len(snap.Asks))
	}
	// Bids must be descending.
	for i := 1; i < len(snap.Bids); i++ {
		if compareNumeric(snap.Bids[i-1].Price, snap.Bids[i].Price) <= 0 {
			t.Errorf("bids[%d]=%s >= bids[%d]=%s", i-1, snap.Bids[i-1].Price, i, snap.Bids[i].Price)
		}
	}
	// Asks must be ascending.
	for i := 1; i < len(snap.Asks); i++ {
		if compareNumeric(snap.Asks[i-1].Price, snap.Asks[i].Price) >= 0 {
			t.Errorf("asks[%d]=%s <= asks[%d]=%s", i-1, snap.Asks[i-1].Price, i, snap.Asks[i].Price)
		}
	}
}

func TestOrderBookGenerateSnapshot_NotReadyReturnsEmpty(t *testing.T) {
	s := newOrderBookState("g", "si")
	snap := s.generateSnapshot(time.Now().UTC())
	if len(snap.Bids) != 0 || len(snap.Asks) != 0 {
		t.Errorf("not-ready snapshot should be empty, got bids=%d asks=%d",
			len(snap.Bids), len(snap.Asks))
	}
}

func TestOrderBookGenerateSnapshot_ZeroQuantityRemoved(t *testing.T) {
	s := newOrderBookState("g", "si")
	_ = s.applySnapshot(sampleTestSnapshot(100))
	// Apply delta that removes ask 50100 (qty=0) and adds a new level.
	_, _ = s.applyDelta(sampleDeltaSimple(101))

	snap := s.generateSnapshot(time.Now().UTC())
	// Should have 4 bids (3 original + 1 new), 1 ask (original 2 minus 1 removed).
	if len(snap.Bids) != 4 {
		t.Errorf("bids = %d, want 4", len(snap.Bids))
	}
	if len(snap.Asks) != 1 {
		t.Errorf("asks = %d, want 1 (one removed)", len(snap.Asks))
	}
}

// --- drainPending edge cases ----------------------------------------------

func TestOrderBookDrainPending_OldDeltasSkipped(t *testing.T) {
	s := newOrderBookState("g", "si")

	// Buffer a delta that will be older than the snapshot.
	_, _ = s.applyDelta(sampleDelta(5, 5, 5))

	// Snapshot at sequence 10 — the buffered delta (5) is behind.
	_ = s.applySnapshot(sampleTestSnapshot(10))

	// Apply a delta at 11 — drainPending should skip seq 5 and apply 11.
	_, err := s.applyDelta(sampleDelta(11, 11, 11))
	if err != nil {
		t.Fatalf("applyDelta: %v", err)
	}
	if len(s.pending) != 0 {
		t.Errorf("pending = %d, want 0", len(s.pending))
	}
}

// --- bid/ask ordering validation ------------------------------------------

func TestValidateBidAskOrder_BidsDesc(t *testing.T) {
	good := []model.PriceLevel{
		{Price: "50000"}, {Price: "49900"},
	}
	if err := validateBidAskOrder(good, nil); err != nil {
		t.Errorf("good bids: %v", err)
	}
	bad := []model.PriceLevel{
		{Price: "49900"}, {Price: "50000"},
	}
	if err := validateBidAskOrder(bad, nil); err == nil {
		t.Error("expected error for non-descending bids")
	}
}

func TestValidateBidAskOrder_AsksAsc(t *testing.T) {
	good := []model.PriceLevel{
		{Price: "50100"}, {Price: "50200"},
	}
	if err := validateBidAskOrder(nil, good); err != nil {
		t.Errorf("good asks: %v", err)
	}
	bad := []model.PriceLevel{
		{Price: "50200"}, {Price: "50100"},
	}
	if err := validateBidAskOrder(nil, bad); err == nil {
		t.Error("expected error for non-ascending asks")
	}
}
