package runtime

import (
	"strings"
	"testing"
	"time"

	"MarketDataBackend/internal/kafka"
	"MarketDataBackend/internal/model"
)

func TestBuildTradeDecodesAndMapsKafkaCoordinates(t *testing.T) {
	in := model.GroupInput{
		GroupID:       "binance:spot:BTCUSDT",
		StreamKey:     "trade",
		SchemaVersion: 2,
	}
	msg := kafka.Message{
		Topic:     "md.trade",
		Partition: 3,
		Offset:    42,
		Value: []byte(`{
			"event_time": 1780826400123,
			"exchange_time": "2026-06-07T10:00:00.100Z",
			"local_receive_time": "1780826400456",
			"trade_id": 12345,
			"price": 105000.125,
			"quantity": "0.001",
			"side": "BID",
			"is_aggregated": true
		}`),
	}

	got, err := buildTrade(in, msg)
	if err != nil {
		t.Fatalf("buildTrade: %v", err)
	}
	if got.InputID != "binance:spot:BTCUSDT:trade" || got.GroupID != in.GroupID {
		t.Errorf("ids = %q/%q", got.GroupID, got.InputID)
	}
	if got.RawTradeID != "" {
		t.Errorf("RawTradeID = %q, want empty so Kafka offset is the fallback key", got.RawTradeID)
	}
	if got.TradeID != "12345" || got.Price != "105000.125" || got.Quantity != "0.001" {
		t.Errorf("decoded trade = %+v", got)
	}
	if got.Side != model.TradeSideBuy || !got.IsAggregated {
		t.Errorf("side/aggregate = %q/%v", got.Side, got.IsAggregated)
	}
	if got.KafkaTopic != msg.Topic || got.KafkaPartition != 3 || got.KafkaOffset != 42 {
		t.Errorf("Kafka coordinates = %q/%d/%d", got.KafkaTopic, got.KafkaPartition, got.KafkaOffset)
	}
	if got.SchemaVersion != 2 || got.ExchangeTime == nil || got.LocalReceiveTime == nil {
		t.Errorf("schema/timestamps = %+v", got)
	}
	if want := time.UnixMilli(1780826400123).UTC(); !got.EventTime.Equal(want) {
		t.Errorf("EventTime = %s, want %s", got.EventTime, want)
	}
}

func TestTradeValidationRejectsInvalidFields(t *testing.T) {
	valid := `{"event_time":"2026-06-07T10:00:00Z","price":"1","quantity":"2","side":"buy"}`
	cases := map[string]string{
		"missing event": `{"price":"1","quantity":"2","side":"buy"}`,
		"zero price":    `{"event_time":1,"price":"0","quantity":"2","side":"buy"}`,
		"negative qty":  `{"event_time":1,"price":"1","quantity":"-2","side":"buy"}`,
		"fraction":      `{"event_time":1,"price":"1/2","quantity":"2","side":"buy"}`,
		"bad side":      `{"event_time":1,"price":"1","quantity":"2","side":"hold"}`,
		"unknown field": strings.TrimSuffix(valid, "}") + `,"symbol":"BTCUSDT"}`,
		"trailing json": valid + `{}`,
		"object price":  `{"event_time":1,"price":{"n":1},"quantity":"2","side":"buy"}`,
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := buildTrade(model.GroupInput{GroupID: "g", StreamKey: "trade"}, kafka.Message{
				Value: []byte(payload),
			}); err == nil {
				t.Fatal("buildTrade succeeded, want error")
			}
		})
	}
}

func TestNormalizeSide(t *testing.T) {
	cases := map[string]string{
		"buy": model.TradeSideBuy,
		"B":   model.TradeSideBuy,
		"ask": model.TradeSideSell,
		" S ": model.TradeSideSell,
	}
	for input, want := range cases {
		got, err := normalizeSide(input)
		if err != nil || got != want {
			t.Errorf("normalizeSide(%q) = %q, %v; want %q", input, got, err, want)
		}
	}
}
