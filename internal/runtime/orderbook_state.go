// Package runtime — orderbook_state holds the in-memory order book assembled
// from a reference snapshot and applied deltas. It is owned by a MarketGroupWorker
// and is the single source of truth for generating local full-depth snapshots.
package runtime

import (
	"fmt"
	"sort"
	"time"

	"MarketDataBackend/internal/model"
)

// orderBookState maintains one full-depth order book for a single group. It
// starts empty (not ready) and stays that way until a complete reference
// snapshot arrives from the upstream adapter. Delta messages are buffered
// before the reference snapshot and applied once the snapshot's sequence can
// be matched. Sequence gaps discard the state — a new reference snapshot is
// required to recover.
type orderBookState struct {
	groupID   string
	inputID   string
	snapInput string // input_id of the orderbook_snapshot Kafka stream

	// bids: price → quantity, descending price order on export
	bids map[string]string
	// asks: price → quantity, ascending price order on export
	asks map[string]string

	// Sequence cursor tracking. lastUpdateID and sequence mirror the
	// orderbook_delta model fields and are set from the last applied
	// delta (or reference snapshot, for the initial values).
	lastUpdateID *int64
	sequence     *int64

	// Ready is true when a reference snapshot has been applied.
	ready bool

	// Buffered deltas collected before the reference snapshot arrives.
	// They are drained once the snapshot is applied and the first
	// contiguous delta is found.
	pending []model.OrderBookDelta
}

func newOrderBookState(groupID, snapInput string) *orderBookState {
	return &orderBookState{
		groupID:   groupID,
		snapInput: snapInput,
		bids:      make(map[string]string),
		asks:      make(map[string]string),
	}
}

// --- public surface used by the group worker -----------------------------

// applySnapshot initialises the order book from a complete reference snapshot.
// Any existing state is discarded. Returns an error if the snapshot's bids or
// asks are not correctly ordered (bids descending, asks ascending), but the
// state is still replaced — callers should treat an error as a signal to
// inspect, not a blocker.
func (s *orderBookState) applySnapshot(snap model.OrderBookSnapshot) error {
	s.bids = make(map[string]string, len(snap.Bids))
	s.asks = make(map[string]string, len(snap.Asks))
	s.lastUpdateID = nil
	s.sequence = nil

	for _, level := range snap.Bids {
		if level.Quantity == "0" || level.Quantity == "0.0" {
			continue
		}
		s.bids[level.Price] = level.Quantity
	}
	for _, level := range snap.Asks {
		if level.Quantity == "0" || level.Quantity == "0.0" {
			continue
		}
		s.asks[level.Price] = level.Quantity
	}

	s.sequence = snap.Sequence
	s.inputID = snap.InputID
	s.ready = true

	// Validate that the snapshot's ordering conforms to the contract.
	if err := validateBidAskOrder(snap.Bids, snap.Asks); err != nil {
		return fmt.Errorf("orderbook state: snapshot ordering: %w", err)
	}
	return nil
}

// applyDelta attempts to apply one validated delta to the book. It returns
// true when the delta was applied, false when it was skipped (buffered
// before a reference snapshot, or not contiguous). An error is returned only
// for a sequence gap, which the caller must treat as a fatal state reset.
func (s *orderBookState) applyDelta(delta model.OrderBookDelta) (applied bool, err error) {
	if !s.ready {
		// Buffer deltas until a reference snapshot arrives.
		s.pending = append(s.pending, delta)
		return false, nil
	}

	// Drain any buffered deltas first.
	if len(s.pending) > 0 {
		if err := s.drainPending(); err != nil {
			return false, err
		}
	}

	return s.applyOne(delta)
}

// ready reports whether the book has a reference snapshot.
func (s *orderBookState) isReady() bool { return s.ready }

// reset discards all state — bids, asks, sequence cursors, buffered deltas.
// The book returns to the "not ready" condition, waiting for a new reference
// snapshot.
func (s *orderBookState) reset() {
	s.bids = make(map[string]string)
	s.asks = make(map[string]string)
	s.lastUpdateID = nil
	s.sequence = nil
	s.ready = false
	s.pending = nil
}

// generateSnapshot creates a full-depth snapshot at the given time. Bids are
// sorted descending, asks ascending. Only non-zero levels are included.
// Returns an empty snapshot when not ready.
func (s *orderBookState) generateSnapshot(at time.Time) model.OrderBookSnapshot {
	snap := model.OrderBookSnapshot{
		GroupID:      s.groupID,
		InputID:      s.inputID,
		SnapshotTime: at,
		Sequence:     copyInt64Ptr(s.sequence),
		CreatedAt:    at,
	}
	if !s.ready {
		// Not ready: return an empty snapshot that the caller can skip.
		return snap
	}
	snap.Bids = s.exportBids()
	snap.Asks = s.exportAsks()
	return snap
}

// --- internal helpers ----------------------------------------------------

func (s *orderBookState) applyOne(delta model.OrderBookDelta) (bool, error) {
	// Sequence continuity check.
	if err := checkSequenceMatch(s.lastUpdateID, s.sequence, delta); err != nil {
		return false, fmt.Errorf("orderbook state: sequence gap: %w", err)
	}

	// Apply bids: price→quantity; quantity=0 removes the level.
	for _, level := range delta.Bids {
		if level.Quantity == "0" || level.Quantity == "0.0" {
			delete(s.bids, level.Price)
		} else {
			s.bids[level.Price] = level.Quantity
		}
	}
	// Apply asks: price→quantity; quantity=0 removes the level.
	for _, level := range delta.Asks {
		if level.Quantity == "0" || level.Quantity == "0.0" {
			delete(s.asks, level.Price)
		} else {
			s.asks[level.Price] = level.Quantity
		}
	}

	// Advance sequence cursors. lastUpdateID is set from the delta's
	// LastUpdateID (the most universal cursor); sequence is set from the
	// delta's Sequence when present.
	s.lastUpdateID = copyInt64Ptr(delta.LastUpdateID)
	if delta.Sequence != nil {
		s.sequence = copyInt64Ptr(delta.Sequence)
	}
	return true, nil
}

// drainPending replays buffered deltas, skipping any that are older than (or
// equal to) the reference snapshot's sequence, and looking for the first
// contiguous delta.  Once a delta is applied, subsequent buffered deltas must
// be contiguous.  If a gap is detected, drainPending returns an error.
func (s *orderBookState) drainPending() error {
	buf := s.pending
	s.pending = nil

	// Skip any deltas whose update-id range is not ahead of the
	// snapshot.  A delta is "ahead" when its last_update_id is strictly
	// greater than the current last_update_id, or when no last_update_id
	// is known but the first_update_id is ahead of the snapshot sequence.
	found := -1
	for i, d := range buf {
		if s.lastUpdateID != nil && d.LastUpdateID != nil &&
			*d.LastUpdateID <= *s.lastUpdateID {
			continue
		}
		if s.sequence != nil && d.Sequence != nil &&
			*d.Sequence <= *s.sequence {
			continue
		}
		// This delta is potentially ahead. Try to apply it.
		applied, err := s.applyOne(d)
		if err != nil {
			return err
		}
		if applied {
			found = i
			break
		}
	}
	if found < 0 {
		// No buffered delta was ahead of the snapshot — wait for newer
		// data from Kafka.
		return nil
	}

	// Apply remaining buffered deltas in order.
	for _, d := range buf[found+1:] {
		if _, err := s.applyOne(d); err != nil {
			return err
		}
	}
	return nil
}

// exportBids returns all non-zero bids sorted by price descending.
func (s *orderBookState) exportBids() []model.PriceLevel {
	return sortLevels(s.bids, func(a, b model.PriceLevel) bool {
		return compareNumeric(a.Price, b.Price) > 0
	})
}

// exportAsks returns all non-zero asks sorted by price ascending.
func (s *orderBookState) exportAsks() []model.PriceLevel {
	return sortLevels(s.asks, func(a, b model.PriceLevel) bool {
		return compareNumeric(a.Price, b.Price) < 0
	})
}

func sortLevels(m map[string]string, less func(a, b model.PriceLevel) bool) []model.PriceLevel {
	levels := make([]model.PriceLevel, 0, len(m))
	for price, qty := range m {
		levels = append(levels, model.PriceLevel{Price: price, Quantity: qty})
	}
	sort.Slice(levels, func(i, j int) bool {
		return less(levels[i], levels[j])
	})
	return levels
}

// --- sequence helpers ----------------------------------------------------

// checkSequenceMatch returns nil when delta can follow the current state.
// It uses last_update_id and/or sequence whichever is available, preferring
// sequence when both are present (as it's the exchange-neutral universal
// cursor). Returns an error on a gap so the caller can reset the state.
func checkSequenceMatch(
	currentLastUpdate *int64, currentSeq *int64, delta model.OrderBookDelta,
) error {
	// If the delta carries a sequence, continuity is:
	//   current_sequence + 1 == delta_sequence
	if delta.Sequence != nil && currentSeq != nil {
		expected := *currentSeq + 1
		if *delta.Sequence == expected {
			return nil
		}
		return fmt.Errorf(
			"expected sequence %d, got %d", expected, *delta.Sequence,
		)
	}
	if delta.Sequence != nil && currentSeq == nil {
		// First delta after a snapshot that had no sequence (unusual).
		// Accept it — we cannot verify continuity.
		return nil
	}

	// Use last_update_id for continuity:
	//   current.last_update_id + 1 == delta.first_update_id
	if currentLastUpdate != nil && delta.FirstUpdateID != nil {
		expected := *currentLastUpdate + 1
		if *delta.FirstUpdateID == expected {
			return nil
		}
		return fmt.Errorf(
			"expected first_update_id %d, got %d",
			expected, *delta.FirstUpdateID,
		)
	}
	if delta.FirstUpdateID != nil && currentLastUpdate == nil {
		// First delta after a snapshot that carried no update-id
		// (e.g. an exchange-level snapshot). Accept it.
		return nil
	}

	// prev_update_id could also be used:
	//   delta.prev_update_id == current.last_update_id
	if currentLastUpdate != nil && delta.PrevUpdateID != nil {
		if *delta.PrevUpdateID == *currentLastUpdate {
			return nil
		}
		return fmt.Errorf(
			"prev_update_id mismatch: expected %d, got %d",
			*currentLastUpdate, *delta.PrevUpdateID,
		)
	}

	// No sequence information on either side — accept (best-effort).
	return nil
}

func validateBidAskOrder(bids, asks []model.PriceLevel) error {
	for i := 1; i < len(bids); i++ {
		if compareNumeric(bids[i-1].Price, bids[i].Price) < 0 {
			return fmt.Errorf("bids[%d].price=%s < bids[%d].price=%s",
				i-1, bids[i-1].Price, i, bids[i].Price)
		}
	}
	for i := 1; i < len(asks); i++ {
		if compareNumeric(asks[i-1].Price, asks[i].Price) > 0 {
			return fmt.Errorf("asks[%d].price=%s > asks[%d].price=%s",
				i-1, asks[i-1].Price, i, asks[i].Price)
		}
	}
	return nil
}

func copyInt64Ptr(p *int64) *int64 {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}
