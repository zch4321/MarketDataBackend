// Package runtime — orderbook_state holds the in-memory order book assembled
// from a reference snapshot and applied deltas. It is owned by a MarketGroupWorker
// and is the single source of truth for generating local full-depth snapshots.
package runtime

import (
	"fmt"
	"math/big"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"MarketDataBackend/internal/model"
)

// orderBookState maintains one full-depth order book for a single group. It
// starts empty (not ready) and stays that way until a complete reference
// snapshot arrives from the upstream adapter. Delta messages are buffered
// before the reference snapshot and applied once the snapshot's sequence can
// be matched. Sequence gaps discard the state — a new reference snapshot is
// required to recover.
//
// Concurrency: snapshotGenerateLoop (reader), snapshotReceiveLoop (writer),
// and onDeltaApplied (writer) run in separate goroutines.  Readers acquire
// s.mu.RLock; writers acquire s.mu.Lock.  The ready flag uses atomic.Bool
// for the fast path used by snapshotGenerateLoop before acquiring the lock.
type orderBookState struct {
	mu        sync.RWMutex
	ready     atomic.Bool
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

	// Buffered deltas collected before the reference snapshot arrives.
	// They are drained once the snapshot is applied and the first
	// contiguous delta is found. A hard cap prevents unbounded memory
	// growth when the reference snapshot is missing for a long time.
	pending    []model.OrderBookDelta
	maxPending int
}

const defaultMaxPending = 10000

func newOrderBookState(groupID, snapInput string) *orderBookState {
	return &orderBookState{
		groupID:    groupID,
		snapInput:  snapInput,
		bids:       make(map[string]string),
		asks:       make(map[string]string),
		maxPending: defaultMaxPending,
	}
}

// --- public surface used by the group worker -----------------------------

// applySnapshot initialises the order book from a complete reference snapshot.
// Any existing state is discarded.  Validation runs before the state is
// replaced, so an ordering violation in the upstream snapshot does not
// overwrite a previously valid book.
func (s *orderBookState) applySnapshot(snap model.OrderBookSnapshot) error {
	// Validate before acquiring the lock — this is a pure function on the
	// snapshot data and does not touch shared state.
	if err := validateBidAskOrder(snap.Bids, snap.Asks); err != nil {
		return fmt.Errorf("orderbook state: snapshot ordering: %w", err)
	}

	bids := make(map[string]string, len(snap.Bids))
	asks := make(map[string]string, len(snap.Asks))

	for _, level := range snap.Bids {
		if level.Quantity == "0" || level.Quantity == "0.0" {
			continue
		}
		bids[level.Price] = level.Quantity
	}
	for _, level := range snap.Asks {
		if level.Quantity == "0" || level.Quantity == "0.0" {
			continue
		}
		asks[level.Price] = level.Quantity
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.bids = bids
	s.asks = asks
	s.lastUpdateID = nil
	s.sequence = snap.Sequence
	s.inputID = snap.InputID
	s.ready.Store(true)

	// Immediately drain any deltas buffered before the snapshot arrived.
	// This prevents the snapshot timer from generating a snapshot that
	// misses already-persisted delta updates (P1-3).
	if len(s.pending) > 0 {
		if err := s.drainPending(); err != nil {
			// Deltas are not contiguous with the snapshot — discard
			// the buffered batch and wait for the next delta from
			// Kafka to restart.
			s.pending = nil
			return fmt.Errorf("orderbook state: drain pending after snapshot: %w", err)
		}
	}
	return nil
}

// applyDelta attempts to apply one validated delta to the book. It returns
// true when the delta was applied, false when it was skipped (buffered
// before a reference snapshot, or not contiguous). An error is returned only
// for a sequence gap, which the caller must treat as a fatal state reset.
func (s *orderBookState) applyDelta(delta model.OrderBookDelta) (applied bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.ready.Load() {
		// Buffer deltas until a reference snapshot arrives.
		if len(s.pending) >= s.maxPending {
			return false, fmt.Errorf(
				"orderbook state: pending delta buffer full (%d); waiting for reference snapshot",
				s.maxPending)
		}
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

// isReady reports whether the book has a reference snapshot.
// It uses the atomic flag so snapshotGenerateLoop can poll without contention.
func (s *orderBookState) isReady() bool {
	return s.ready.Load()
}

// reset discards all state — bids, asks, sequence cursors, buffered deltas.
// The book returns to the "not ready" condition, waiting for a new reference
// snapshot.
func (s *orderBookState) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bids = make(map[string]string)
	s.asks = make(map[string]string)
	s.lastUpdateID = nil
	s.sequence = nil
	s.ready.Store(false)
	s.pending = nil
}

// generateSnapshot creates a full-depth snapshot at the given time. Bids are
// sorted descending, asks ascending. Only non-zero levels are included.
// Returns an empty snapshot when not ready.
// CreatedAt is left at its zero value — the database layer fills it via
// DEFAULT now() for consistency with the rest of the table.
func (s *orderBookState) generateSnapshot(at time.Time) model.OrderBookSnapshot {
	if !s.ready.Load() {
		return model.OrderBookSnapshot{
			GroupID:      s.groupID,
			InputID:      s.inputID,
			SnapshotTime: at,
		}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	snap := model.OrderBookSnapshot{
		GroupID:      s.groupID,
		InputID:      s.inputID,
		SnapshotTime: at,
		Sequence:     copyInt64Ptr(s.sequence),
		Bids:         s.exportBids(),
		Asks:         s.exportAsks(),
	}
	return snap
}

// --- internal helpers ----------------------------------------------------

func (s *orderBookState) applyOne(delta model.OrderBookDelta) (bool, error) {
	// Caller holds s.mu.
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
// Caller holds s.mu.
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

// --- helpers (no locking) -----------------------------------------------

// exportBids returns all non-zero bids sorted descending by price.
// Caller holds at least s.mu.RLock.
func (s *orderBookState) exportBids() []model.PriceLevel {
	return exportLevels(s.bids, sortDesc)
}

// exportAsks returns all non-zero asks sorted ascending by price.
// Caller holds at least s.mu.RLock.
func (s *orderBookState) exportAsks() []model.PriceLevel {
	return exportLevels(s.asks, sortAsc)
}

type sortDir bool

const sortDesc sortDir = false
const sortAsc sortDir = true

// levelWithPrice holds a PriceLevel together with a pre-parsed big.Float so
// that sorting (O(n log n)) parses each price exactly once instead of once
// per comparator invocation.
type levelWithPrice struct {
	level  model.PriceLevel
	parsed *big.Float
}

func exportLevels(m map[string]string, dir sortDir) []model.PriceLevel {
	levels := make([]levelWithPrice, 0, len(m))
	for price, qty := range m {
		if qty == "0" || qty == "0.0" {
			continue
		}
		pv, _, _ := big.ParseFloat(strings.TrimSpace(price), 10, 256, big.ToNearestEven)
		levels = append(levels, levelWithPrice{
			level:  model.PriceLevel{Price: price, Quantity: qty},
			parsed: pv,
		})
	}
	sort.Slice(levels, func(i, j int) bool {
		cmp := levels[i].parsed.Cmp(levels[j].parsed)
		if dir == sortDesc {
			return cmp > 0
		}
		return cmp < 0
	})
	out := make([]model.PriceLevel, len(levels))
	for i, lp := range levels {
		out[i] = lp.level
	}
	return out
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

// getSequenceAndLastUpdateID returns the current sequence and lastUpdateID
// from the order book state.  Both may be nil when no snapshot has been
// applied yet.  The caller must ensure the state is ready before relying
// on these values (e.g. by checking isReady).
func (s *orderBookState) getSequenceAndLastUpdateID() (*int64, *int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return copyInt64Ptr(s.sequence), copyInt64Ptr(s.lastUpdateID)
}

func copyInt64Ptr(p *int64) *int64 {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}
