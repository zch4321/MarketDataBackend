package runtime

import (
	"fmt"

	"MarketDataBackend/internal/model"
)

// checkDeltaContinuity validates a new delta against the last committed delta
// for its partition. The bool reports whether the cursor should advance; an
// adjacent replay is valid but leaves the cursor unchanged.
func checkDeltaContinuity(
	previous *model.OrderBookDelta, current model.OrderBookDelta,
) (bool, error) {
	if previous == nil {
		return true, nil
	}
	if sameDeltaIdentity(*previous, current) {
		return false, nil
	}

	if current.PrevUpdateID != nil {
		if terminal := deltaTerminal(previous); terminal != nil &&
			*current.PrevUpdateID != *terminal {
			return false, fmt.Errorf(
				"orderbook delta sequence gap: prev_update_id=%d, want %d",
				*current.PrevUpdateID, *terminal,
			)
		}
		return true, nil
	}

	if previous.LastUpdateID != nil &&
		current.FirstUpdateID != nil && current.LastUpdateID != nil {
		next := *previous.LastUpdateID + 1
		if *current.FirstUpdateID > next || *current.LastUpdateID < next {
			return false, fmt.Errorf(
				"orderbook delta sequence gap: update range [%d,%d] does not cover %d",
				*current.FirstUpdateID, *current.LastUpdateID, next,
			)
		}
		return true, nil
	}

	if previous.Sequence != nil && current.Sequence != nil {
		next := *previous.Sequence + 1
		if *current.Sequence != next {
			return false, fmt.Errorf(
				"orderbook delta sequence gap: sequence=%d, want %d",
				*current.Sequence, next,
			)
		}
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
