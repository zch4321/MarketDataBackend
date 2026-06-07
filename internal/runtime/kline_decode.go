package runtime

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"MarketDataBackend/internal/kafka"
	"MarketDataBackend/internal/model"
)

// klineMessage is the normalized exchange-candlestick payload. Decimal and
// integer fields accept either JSON strings or numbers to tolerate producer
// differences without sacrificing PostgreSQL numeric precision.
type klineMessage struct {
	Source      string     `json:"source"`
	Interval    string     `json:"interval"`
	OpenTime    flexTime   `json:"open_time"`
	CloseTime   flexTime   `json:"close_time"`
	Open        flexString `json:"open"`
	High        flexString `json:"high"`
	Low         flexString `json:"low"`
	Close       flexString `json:"close"`
	Volume      flexString `json:"volume"`
	QuoteVolume flexString `json:"quote_volume"`
	TradeCount  flexInt64  `json:"trade_count"`
	IsClosed    bool       `json:"is_closed"`
	Revision    flexInt64  `json:"revision"`
}

func decodeKline(data []byte) (klineMessage, error) {
	var km klineMessage
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&km); err != nil {
		return klineMessage{}, fmt.Errorf("decode kline: %w", err)
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return klineMessage{}, fmt.Errorf("decode kline: multiple JSON values")
		}
		return klineMessage{}, fmt.Errorf("decode kline: trailing data: %w", err)
	}
	return km, nil
}

func (km klineMessage) validate(in model.GroupInput) (string, error) {
	expectedKey, err := model.StreamKeyFor(model.StreamKindKline, in.Interval)
	if err != nil {
		return "", fmt.Errorf("kline: invalid input interval: %w", err)
	}
	if in.StreamKind != model.StreamKindKline || in.StreamKey != expectedKey {
		return "", fmt.Errorf(
			"kline: input stream_key %q does not match interval %q",
			in.StreamKey, in.Interval,
		)
	}
	if strings.TrimSpace(km.Interval) != in.Interval {
		return "", fmt.Errorf(
			"kline: message interval %q does not match input interval %q",
			km.Interval, in.Interval,
		)
	}

	source := strings.ToLower(strings.TrimSpace(km.Source))
	if source == "" {
		source = model.KlineSourceExchange
	}
	if source != model.KlineSourceExchange {
		return "", fmt.Errorf("kline: unsupported source %q", km.Source)
	}
	if !km.OpenTime.set {
		return "", fmt.Errorf("kline: missing open_time")
	}
	if !km.CloseTime.set {
		return "", fmt.Errorf("kline: missing close_time")
	}
	if !km.CloseTime.t.After(km.OpenTime.t) {
		return "", fmt.Errorf("kline: close_time must be after open_time")
	}

	prices := []struct {
		name  string
		value flexString
	}{
		{"open", km.Open},
		{"high", km.High},
		{"low", km.Low},
		{"close", km.Close},
	}
	for _, field := range prices {
		if !validPositiveNumeric(string(field.value)) {
			return "", fmt.Errorf("kline: invalid %s %q", field.name, string(field.value))
		}
	}
	if !validNonNegativeNumeric(string(km.Volume)) {
		return "", fmt.Errorf("kline: invalid volume %q", string(km.Volume))
	}
	if strings.TrimSpace(string(km.QuoteVolume)) != "" &&
		!validNonNegativeNumeric(string(km.QuoteVolume)) {
		return "", fmt.Errorf("kline: invalid quote_volume %q", string(km.QuoteVolume))
	}
	if km.TradeCount.value < 0 {
		return "", fmt.Errorf("kline: trade_count must be non-negative")
	}
	if km.Revision.value < 0 {
		return "", fmt.Errorf("kline: revision must be non-negative")
	}
	if compareNumeric(string(km.High), string(km.Low)) < 0 {
		return "", fmt.Errorf("kline: high must be greater than or equal to low")
	}
	for _, field := range []struct {
		name  string
		value flexString
	}{{"open", km.Open}, {"close", km.Close}} {
		if compareNumeric(string(field.value), string(km.Low)) < 0 ||
			compareNumeric(string(field.value), string(km.High)) > 0 {
			return "", fmt.Errorf("kline: %s must be within low/high", field.name)
		}
	}
	return source, nil
}

func (km klineMessage) toKline(
	in model.GroupInput, msg kafka.Message, source string,
) model.Kline {
	return model.Kline{
		GroupID:        in.GroupID,
		InputID:        inputID(in),
		Source:         source,
		Interval:       in.Interval,
		OpenTime:       km.OpenTime.t,
		CloseTime:      km.CloseTime.t,
		Open:           strings.TrimSpace(string(km.Open)),
		High:           strings.TrimSpace(string(km.High)),
		Low:            strings.TrimSpace(string(km.Low)),
		Close:          strings.TrimSpace(string(km.Close)),
		Volume:         strings.TrimSpace(string(km.Volume)),
		QuoteVolume:    strings.TrimSpace(string(km.QuoteVolume)),
		TradeCount:     km.TradeCount.value,
		IsClosed:       km.IsClosed,
		Revision:       km.Revision.value,
		KafkaTopic:     msg.Topic,
		KafkaPartition: msg.Partition,
		KafkaOffset:    msg.Offset,
	}
}

func buildKline(in model.GroupInput, msg kafka.Message) (model.Kline, error) {
	km, err := decodeKline(msg.Value)
	if err != nil {
		return model.Kline{}, err
	}
	source, err := km.validate(in)
	if err != nil {
		return model.Kline{}, err
	}
	return km.toKline(in, msg, source), nil
}
