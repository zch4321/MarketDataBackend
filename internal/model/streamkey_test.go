package model

import "testing"

func TestNewGroupID(t *testing.T) {
	if got := NewGroupID("binance", "spot", "BTCUSDT"); got != "binance:spot:BTCUSDT" {
		t.Errorf("NewGroupID = %q, want binance:spot:BTCUSDT", got)
	}
	// Surrounding whitespace is trimmed so ids stay canonical.
	if got := NewGroupID(" binance ", " spot ", " BTCUSDT "); got != "binance:spot:BTCUSDT" {
		t.Errorf("NewGroupID (trimmed) = %q, want binance:spot:BTCUSDT", got)
	}
}

func TestNewInputID(t *testing.T) {
	gid := NewGroupID("binance", "spot", "BTCUSDT")
	if got := NewInputID(gid, "kline_1m"); got != "binance:spot:BTCUSDT:kline_1m" {
		t.Errorf("NewInputID = %q, want binance:spot:BTCUSDT:kline_1m", got)
	}
}

func TestStreamKeyFor(t *testing.T) {
	cases := []struct {
		kind     string
		interval string
		want     string
		wantErr  bool
	}{
		{StreamKindTrade, "", "trade", false},
		{StreamKindOrderBookDelta, "", "orderbook_delta", false},
		{StreamKindOrderBookSnapshot, "", "orderbook_snapshot", false},
		{StreamKindKline, "1m", "kline_1m", false},
		{StreamKindKline, "1h", "kline_1h", false},
		{StreamKindKline, "", "", true},   // kline needs an interval
		{StreamKindTrade, "1m", "", true}, // trade must not carry an interval
		{"bogus", "", "", true},
	}
	for _, c := range cases {
		got, err := StreamKeyFor(c.kind, c.interval)
		if c.wantErr {
			if err == nil {
				t.Errorf("StreamKeyFor(%q,%q) = %q, want error", c.kind, c.interval, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("StreamKeyFor(%q,%q) unexpected error: %v", c.kind, c.interval, err)
			continue
		}
		if got != c.want {
			t.Errorf("StreamKeyFor(%q,%q) = %q, want %q", c.kind, c.interval, got, c.want)
		}
	}
}

func TestParseStreamKey(t *testing.T) {
	cases := []struct {
		streamKey    string
		wantKind     string
		wantInterval string
		wantErr      bool
	}{
		{"trade", StreamKindTrade, "", false},
		{"orderbook_delta", StreamKindOrderBookDelta, "", false},
		{"orderbook_snapshot", StreamKindOrderBookSnapshot, "", false},
		{"kline_1m", StreamKindKline, "1m", false},
		{"kline_15m", StreamKindKline, "15m", false},
		{"kline_", "", "", true},
		{"unknown", "", "", true},
	}
	for _, c := range cases {
		kind, interval, err := ParseStreamKey(c.streamKey)
		if c.wantErr {
			if err == nil {
				t.Errorf("ParseStreamKey(%q) = (%q,%q), want error", c.streamKey, kind, interval)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseStreamKey(%q) unexpected error: %v", c.streamKey, err)
			continue
		}
		if kind != c.wantKind || interval != c.wantInterval {
			t.Errorf("ParseStreamKey(%q) = (%q,%q), want (%q,%q)",
				c.streamKey, kind, interval, c.wantKind, c.wantInterval)
		}
	}
}

// TestStreamKeyRoundTrip verifies StreamKeyFor and ParseStreamKey are inverses.
func TestStreamKeyRoundTrip(t *testing.T) {
	cases := []struct{ kind, interval string }{
		{StreamKindTrade, ""},
		{StreamKindOrderBookDelta, ""},
		{StreamKindOrderBookSnapshot, ""},
		{StreamKindKline, "1m"},
		{StreamKindKline, "4h"},
	}
	for _, c := range cases {
		key, err := StreamKeyFor(c.kind, c.interval)
		if err != nil {
			t.Fatalf("StreamKeyFor(%q,%q): %v", c.kind, c.interval, err)
		}
		kind, interval, err := ParseStreamKey(key)
		if err != nil {
			t.Fatalf("ParseStreamKey(%q): %v", key, err)
		}
		if kind != c.kind || interval != c.interval {
			t.Errorf("round trip %q/%q -> %q -> %q/%q", c.kind, c.interval, key, kind, interval)
		}
	}
}
