package runtime

import (
	"strings"
	"testing"
	"time"

	"MarketDataBackend/internal/kafka"
	"MarketDataBackend/internal/model"
)

func TestBuildOrderBookDeltaDecodesObjectAndCompactLevels(t *testing.T) {
	in := model.GroupInput{
		GroupID:    "binance:spot:BTCUSDT",
		StreamKey:  model.StreamKindOrderBookDelta,
		StreamKind: model.StreamKindOrderBookDelta,
	}
	msg := kafka.Message{
		Topic:     "md.depth",
		Partition: 2,
		Offset:    12,
		Value: []byte(`{
			"event_time":"2026-06-07T10:00:00Z",
			"exchange_time":1780826400100,
			"local_receive_time":"2026-06-07T10:00:00.200Z",
			"raw_event_id":123,
			"first_update_id":"1001",
			"last_update_id":1002,
			"prev_update_id":1000,
			"sequence":"55",
			"bids":[{"price":"100.1","quantity":"2"},["100.0",0]],
			"asks":[["100.2","3"]]
		}`),
	}

	got, err := buildOrderBookDelta(in, msg)
	if err != nil {
		t.Fatalf("buildOrderBookDelta: %v", err)
	}
	if got.InputID != in.GroupID+":orderbook_delta" || got.RawEventID != "123" {
		t.Errorf("identity = %+v", got)
	}
	if got.FirstUpdateID == nil || *got.FirstUpdateID != 1001 ||
		got.LastUpdateID == nil || *got.LastUpdateID != 1002 ||
		got.PrevUpdateID == nil || *got.PrevUpdateID != 1000 ||
		got.Sequence == nil || *got.Sequence != 55 {
		t.Errorf("sequence fields = %+v", got)
	}
	if len(got.Bids) != 2 || got.Bids[1].Quantity != "0" ||
		len(got.Asks) != 1 || got.Asks[0].Price != "100.2" {
		t.Errorf("levels = bids=%+v asks=%+v", got.Bids, got.Asks)
	}
	if got.ExchangeTime == nil || got.LocalReceiveTime == nil ||
		got.KafkaTopic != msg.Topic || got.KafkaOffset != 12 {
		t.Errorf("timestamps/coordinates = %+v", got)
	}
	if want := time.UnixMilli(1780826400100).UTC(); !got.ExchangeTime.Equal(want) {
		t.Errorf("ExchangeTime = %s, want %s", got.ExchangeTime, want)
	}
}

func TestOrderBookDeltaValidationRejectsInvalidPayloads(t *testing.T) {
	in := model.GroupInput{GroupID: "g", StreamKey: "orderbook_delta"}
	valid := `{
		"event_time":"2026-06-07T10:00:00Z",
		"first_update_id":10,
		"last_update_id":11,
		"bids":[["100","1"]],
		"asks":[]
	}`
	cases := map[string]string{
		"missing event": strings.Replace(valid,
			`"event_time":"2026-06-07T10:00:00Z",`, "", 1),
		"backwards update range": strings.Replace(valid,
			`"first_update_id":10`, `"first_update_id":12`, 1),
		"empty changes": strings.Replace(valid,
			`"bids":[["100","1"]]`, `"bids":[]`, 1),
		"zero price":        strings.Replace(valid, `["100","1"]`, `["0","1"]`, 1),
		"negative quantity": strings.Replace(valid, `["100","1"]`, `["100","-1"]`, 1),
		"bad compact level": strings.Replace(valid, `["100","1"]`, `["100"]`, 1),
		"unknown level field": strings.Replace(valid,
			`["100","1"]`, `{"price":"100","quantity":"1","count":2}`, 1),
		"unknown field": strings.TrimSuffix(valid, "}") + `,"symbol":"BTCUSDT"}`,
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := buildOrderBookDelta(in, kafka.Message{Value: []byte(payload)}); err == nil {
				t.Fatal("buildOrderBookDelta succeeded, want error")
			}
		})
	}
}
