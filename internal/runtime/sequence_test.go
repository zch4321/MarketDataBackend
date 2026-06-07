package runtime

import (
	"strings"
	"testing"

	"MarketDataBackend/internal/model"
)

func TestCheckDeltaContinuity(t *testing.T) {
	ptr := func(v int64) *int64 { return &v }
	cases := []struct {
		name     string
		previous model.OrderBookDelta
		current  model.OrderBookDelta
		advance  bool
		wantErr  bool
	}{
		{
			name:     "prev update id",
			previous: model.OrderBookDelta{LastUpdateID: ptr(100)},
			current:  model.OrderBookDelta{PrevUpdateID: ptr(100), LastUpdateID: ptr(101)},
			advance:  true,
		},
		{
			name:     "prev update gap",
			previous: model.OrderBookDelta{LastUpdateID: ptr(100)},
			current:  model.OrderBookDelta{PrevUpdateID: ptr(99), LastUpdateID: ptr(101)},
			wantErr:  true,
		},
		{
			name:     "range covers next",
			previous: model.OrderBookDelta{LastUpdateID: ptr(100)},
			current: model.OrderBookDelta{
				FirstUpdateID: ptr(99),
				LastUpdateID:  ptr(103),
			},
			advance: true,
		},
		{
			name:     "range gap",
			previous: model.OrderBookDelta{LastUpdateID: ptr(100)},
			current: model.OrderBookDelta{
				FirstUpdateID: ptr(102),
				LastUpdateID:  ptr(103),
			},
			wantErr: true,
		},
		{
			name:     "sequence increments",
			previous: model.OrderBookDelta{Sequence: ptr(10)},
			current:  model.OrderBookDelta{Sequence: ptr(11)},
			advance:  true,
		},
		{
			name:     "sequence gap",
			previous: model.OrderBookDelta{Sequence: ptr(10)},
			current:  model.OrderBookDelta{Sequence: ptr(12)},
			wantErr:  true,
		},
		{
			name:     "raw id replay",
			previous: model.OrderBookDelta{RawEventID: "evt-1", Sequence: ptr(10)},
			current:  model.OrderBookDelta{RawEventID: "evt-1", Sequence: ptr(99)},
			advance:  false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			advance, err := checkDeltaContinuity(&tc.previous, tc.current)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if err != nil && !strings.Contains(err.Error(), "sequence gap") {
				t.Errorf("gap error = %q", err)
			}
			if advance != tc.advance {
				t.Errorf("advance = %v, want %v", advance, tc.advance)
			}
		})
	}
}

func TestCheckDeltaContinuityAcceptsFirstDelta(t *testing.T) {
	advance, err := checkDeltaContinuity(nil, model.OrderBookDelta{})
	if err != nil || !advance {
		t.Fatalf("first delta = advance %v, err %v; want true, nil", advance, err)
	}
}
