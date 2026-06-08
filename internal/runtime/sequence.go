package runtime

import "MarketDataBackend/internal/model"

// checkDeltaContinuity validates a new delta against the last committed delta
// for its partition. For fact ingestion this is intentionally a monotonicity
// gate, not a strict contiguity gate: gaps are still useful facts and should be
// persisted, while stale/replayed records are acknowledged without advancing the
// cursor. Strict order-book reconstruction is handled separately by
// orderBookState.
func checkDeltaContinuity(
	previous *model.OrderBookDelta, current model.OrderBookDelta,
) (bool, error) {
	if previous == nil {
		return true, nil
	}
	if sameDeltaIdentity(*previous, current) {
		return false, nil
	}

	if prevTerminal, curTerminal := deltaTerminal(previous), deltaTerminal(&current); prevTerminal != nil && curTerminal != nil {
		return *curTerminal > *prevTerminal, nil
	}
	return true, nil
}

func deltaTerminal(delta *model.OrderBookDelta) *int64 {
	if delta.LastUpdateID != nil {
		return delta.LastUpdateID
	}
	return delta.Sequence
}

func sameDeltaIdentity(a, b model.OrderBookDelta) bool {
	if a.RawEventID != "" && b.RawEventID != "" && a.RawEventID == b.RawEventID {
		return true
	}
	return sameOptionalInt64(a.FirstUpdateID, b.FirstUpdateID) &&
		sameOptionalInt64(a.LastUpdateID, b.LastUpdateID) &&
		sameOptionalInt64(a.Sequence, b.Sequence) &&
		(a.FirstUpdateID != nil || a.LastUpdateID != nil || a.Sequence != nil)
}

func sameOptionalInt64(a, b *int64) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil || b == nil:
		return false
	default:
		return *a == *b
	}
}
