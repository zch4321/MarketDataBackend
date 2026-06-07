package model

import "testing"

func TestIsValidDesiredStatus(t *testing.T) {
	valid := []string{DesiredStatusRunning, DesiredStatusPaused, DesiredStatusDisabled}
	for _, s := range valid {
		if !IsValidDesiredStatus(s) {
			t.Errorf("expected %q to be valid", s)
		}
	}
	if IsValidDesiredStatus("bogus") {
		t.Error("expected bogus to be invalid")
	}
	if IsValidDesiredStatus("") {
		t.Error("expected empty string to be invalid")
	}
}

func TestIsValidStreamKind(t *testing.T) {
	valid := []string{
		StreamKindTrade,
		StreamKindKline,
		StreamKindOrderBookDelta,
		StreamKindOrderBookSnapshot,
	}
	for _, s := range valid {
		if !IsValidStreamKind(s) {
			t.Errorf("expected %q to be valid", s)
		}
	}
	if IsValidStreamKind("kline_1m") {
		t.Error("stream_key kline_1m should not be a valid stream_kind")
	}
}
