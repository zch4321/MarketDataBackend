package runtime

import (
	"strings"
	"testing"
	"time"

	"MarketDataBackend/internal/kafka"
	"MarketDataBackend/internal/model"
)

func TestBuildKlineDecodesAndValidatesInputInterval(t *testing.T) {
	in := model.GroupInput{
		GroupID:    "binance:spot:BTCUSDT",
		StreamKey:  "kline_1m",
		StreamKind: model.StreamKindKline,
		Interval:   "1m",
	}
	msg := kafka.Message{
		Topic:     "md.kline.1m",
		Partition: 1,
		Offset:    9,
		Value: []byte(`{
			"source":"exchange",
			"interval":"1m",
			"open_time":"2026-06-07T10:00:00Z",
			"close_time":1780826460000,
			"open":"100",
			"high":110.5,
			"low":"90",
			"close":"105.25",
			"volume":"12.5",
			"quote_volume":1250,
			"trade_count":"42",
			"is_closed":false,
			"revision":3
		}`),
	}

	got, err := buildKline(in, msg)
	if err != nil {
		t.Fatalf("buildKline: %v", err)
	}
	if got.InputID != in.GroupID+":kline_1m" ||
		got.Source != model.KlineSourceExchange || got.Interval != "1m" {
		t.Errorf("identity/source = %+v", got)
	}
	if got.Open != "100" || got.High != "110.5" || got.Close != "105.25" ||
		got.Volume != "12.5" || got.QuoteVolume != "1250" {
		t.Errorf("OHLCV = %+v", got)
	}
	if got.TradeCount != 42 || got.Revision != 3 || got.IsClosed {
		t.Errorf("count/revision/closed = %d/%d/%v", got.TradeCount, got.Revision, got.IsClosed)
	}
	if got.KafkaTopic != msg.Topic || got.KafkaPartition != 1 || got.KafkaOffset != 9 {
		t.Errorf("Kafka coordinates = %+v", got)
	}
	if want := time.Date(2026, 6, 7, 10, 1, 0, 0, time.UTC); !got.CloseTime.Equal(want) {
		t.Errorf("CloseTime = %s, want %s", got.CloseTime, want)
	}
}

func TestKlineValidationRejectsInvalidPayloads(t *testing.T) {
	in := model.GroupInput{
		GroupID:    "g",
		StreamKey:  "kline_1m",
		StreamKind: model.StreamKindKline,
		Interval:   "1m",
	}
	valid := `{
		"interval":"1m",
		"open_time":"2026-06-07T10:00:00Z",
		"close_time":"2026-06-07T10:01:00Z",
		"open":"100","high":"110","low":"90","close":"105",
		"volume":"1","trade_count":1,"revision":1,"is_closed":false
	}`
	cases := map[string]struct {
		input   model.GroupInput
		payload string
	}{
		"interval mismatch": {
			input: in, payload: strings.Replace(valid, `"interval":"1m"`, `"interval":"5m"`, 1),
		},
		"stream key mismatch": {
			input: func() model.GroupInput {
				bad := in
				bad.StreamKey = "kline_5m"
				return bad
			}(),
			payload: valid,
		},
		"computed source": {
			input: in, payload: strings.Replace(valid, "{", `{"source":"computed",`, 1),
		},
		"missing open time": {
			input: in, payload: strings.Replace(valid,
				`"open_time":"2026-06-07T10:00:00Z",`, "", 1),
		},
		"close before open": {
			input: in, payload: strings.Replace(valid,
				`"close_time":"2026-06-07T10:01:00Z"`,
				`"close_time":"2026-06-07T09:59:00Z"`, 1),
		},
		"negative volume": {
			input: in, payload: strings.Replace(valid, `"volume":"1"`, `"volume":"-1"`, 1),
		},
		"open outside range": {
			input: in, payload: strings.Replace(valid, `"open":"100"`, `"open":"120"`, 1),
		},
		"negative revision": {
			input: in, payload: strings.Replace(valid, `"revision":1`, `"revision":-1`, 1),
		},
		"unknown field": {
			input: in, payload: strings.TrimSuffix(valid, "}") + `,"symbol":"BTCUSDT"}`,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := buildKline(tc.input, kafka.Message{Value: []byte(tc.payload)}); err == nil {
				t.Fatal("buildKline succeeded, want error")
			}
		})
	}
}
