package kafka

import (
	"context"
	"testing"
)

func TestReaderFactoryValidatesConfig(t *testing.T) {
	factory := NewReaderFactory()
	cases := []Config{
		{Topic: "trades", GroupID: "group"},
		{Brokers: []string{""}, Topic: "trades", GroupID: "group"},
		{Brokers: []string{"localhost:9092"}, GroupID: "group"},
		{Brokers: []string{"localhost:9092"}, Topic: "trades"},
	}
	for _, cfg := range cases {
		if _, err := factory.NewConsumer(context.Background(), cfg); err == nil {
			t.Fatalf("NewConsumer(%+v) succeeded, want validation error", cfg)
		}
	}
}

func TestReaderFactoryBuildsConsumerWithoutConnectingEagerly(t *testing.T) {
	factory := NewReaderFactory()
	consumer, err := factory.NewConsumer(context.Background(), Config{
		Brokers: []string{"127.0.0.1:1"},
		Topic:   "trades",
		GroupID: "runtime-test",
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	if err := consumer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
