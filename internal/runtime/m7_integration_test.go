package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"

	"MarketDataBackend/internal/kafka"
	"MarketDataBackend/internal/metadata"
	"MarketDataBackend/internal/model"
	"MarketDataBackend/internal/storage"
)

type routedBrokerFactory struct {
	mu sync.Mutex

	topics    map[string]chan kafka.Message
	configs   []kafka.Config
	commits   map[string][]kafka.Message
	consumers []*routedBrokerConsumer
}

type routedBrokerConsumer struct {
	broker *routedBrokerFactory
	topic  string
	closed chan struct{}
	once   sync.Once
}

func newRoutedBrokerFactory() *routedBrokerFactory {
	return &routedBrokerFactory{
		topics:  make(map[string]chan kafka.Message),
		commits: make(map[string][]kafka.Message),
	}
}

func (f *routedBrokerFactory) topic(name string) chan kafka.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.topics[name] == nil {
		f.topics[name] = make(chan kafka.Message, 32)
	}
	return f.topics[name]
}

func (f *routedBrokerFactory) NewConsumer(
	_ context.Context, cfg kafka.Config,
) (kafka.Consumer, error) {
	consumer := &routedBrokerConsumer{
		broker: f,
		topic:  cfg.Topic,
		closed: make(chan struct{}),
	}
	f.mu.Lock()
	if f.topics[cfg.Topic] == nil {
		f.topics[cfg.Topic] = make(chan kafka.Message, 32)
	}
	f.configs = append(f.configs, cfg)
	f.consumers = append(f.consumers, consumer)
	f.mu.Unlock()
	return consumer, nil
}

func (c *routedBrokerConsumer) Fetch(ctx context.Context) (kafka.Message, error) {
	topic := c.broker.topic(c.topic)
	select {
	case <-ctx.Done():
		return kafka.Message{}, ctx.Err()
	case <-c.closed:
		return kafka.Message{}, context.Canceled
	case msg := <-topic:
		return msg, nil
	}
}

func (c *routedBrokerConsumer) Commit(_ context.Context, msg kafka.Message) error {
	c.broker.mu.Lock()
	c.broker.commits[c.topic] = append(c.broker.commits[c.topic], msg)
	c.broker.mu.Unlock()
	return nil
}

func (c *routedBrokerConsumer) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

func (f *routedBrokerFactory) send(topic string, msg kafka.Message) {
	msg.Topic = topic
	f.topic(topic) <- msg
}

func (f *routedBrokerFactory) consumerCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.consumers)
}

func (f *routedBrokerFactory) commitCount(topic string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.commits[topic])
}

type m7Inputs struct {
	groupID    string
	tradeID    string
	klineID    string
	deltaID    string
	tradeTopic string
	klineTopic string
	deltaTopic string
}

func createM7Group(
	t *testing.T, store metadata.Store, suffix string,
) m7Inputs {
	t.Helper()
	topics := m7Inputs{
		tradeTopic: "md.trade." + suffix,
		klineTopic: "md.kline." + suffix,
		deltaTopic: "md.depth." + suffix,
	}
	group := model.MarketGroup{
		Exchange:   "test",
		MarketType: model.MarketTypeSpot,
		Symbol:     "M7" + suffix,
		Inputs: []model.GroupInput{
			{
				StreamKey: model.StreamKindTrade, StreamKind: model.StreamKindTrade,
				Enabled: true, KafkaTopic: topics.tradeTopic,
				KafkaGroupID: "m7-" + suffix + "-trade",
			},
			{
				StreamKey: "kline_1m", StreamKind: model.StreamKindKline, Interval: "1m",
				Enabled: true, KafkaTopic: topics.klineTopic,
				KafkaGroupID: "m7-" + suffix + "-kline",
			},
			{
				StreamKey:  model.StreamKindOrderBookDelta,
				StreamKind: model.StreamKindOrderBookDelta,
				Enabled:    true, KafkaTopic: topics.deltaTopic,
				KafkaGroupID: "m7-" + suffix + "-delta",
			},
		},
	}
	if err := store.CreateGroup(context.Background(), group); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	topics.groupID = model.NewGroupID(group.Exchange, group.MarketType, group.Symbol)
	topics.tradeID = model.NewInputID(topics.groupID, model.StreamKindTrade)
	topics.klineID = model.NewInputID(topics.groupID, "kline_1m")
	topics.deltaID = model.NewInputID(topics.groupID, model.StreamKindOrderBookDelta)
	return topics
}

func m7Trade(offset int64, rawID string) kafka.Message {
	msg := runtimeTradeMessage(offset, offset+1, rawID)
	msg.Value = []byte(fmt.Sprintf(`{
		"event_time":"2026-06-07T10:00:00Z",
		"raw_trade_id":%q,
		"price":"100.25","quantity":"0.5","side":"buy"
	}`, rawID))
	return msg
}

func m7Kline(offset, revision int64, closePrice string, closed bool) kafka.Message {
	return validKlineMessage(offset, closePrice, revision, closed)
}

func m7Delta(offset, sequence int64, rawID string) kafka.Message {
	msg := validDeltaMessage(offset, sequence)
	msg.Value = []byte(fmt.Sprintf(`{
		"event_time":"2026-06-07T10:00:00Z",
		"raw_event_id":%q,
		"sequence":%d,
		"bids":[["100.1","1"],["100.0","0"]],
		"asks":[{"price":"100.2","quantity":"2"}]
	}`, rawID, sequence))
	return msg
}

func TestM7PipelinesRunInParallelAndPauseIndependently(t *testing.T) {
	store, pool := newStore(t)
	broker := newRoutedBrokerFactory()
	inputs := createM7Group(t, store, "memory")

	cfg := fastNodeConfig("node-m7-memory")
	cfg.ReconcileInterval = 20 * time.Millisecond
	node := NewNode(store, cfg, discardLogger())
	node.EnableConsumption(
		storage.NewPostgresStorage(pool), broker, []string{"memory:9092"},
		20*time.Millisecond, 20*time.Millisecond,
	)
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- node.Run(runCtx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	eventually(t, 2*time.Second, func() bool {
		return broker.consumerCount() == 3
	}, "trade, kline and delta consumers should start")

	broker.send(inputs.tradeTopic, m7Trade(0, "trade-1"))
	broker.send(inputs.klineTopic, m7Kline(0, 1, "105", false))
	broker.send(inputs.deltaTopic, m7Delta(0, 100, "delta-100"))
	eventually(t, 3*time.Second, func() bool {
		return factCount(t, pool, "trades", inputs.tradeID) == 1 &&
			factCount(t, pool, "klines", inputs.klineID) == 1 &&
			factCount(t, pool, "orderbook_deltas", inputs.deltaID) == 1
	}, "all three facts should reach PostgreSQL")
	bids, asks := readRuntimeDeltaLevels(t, pool, inputs.deltaID, 0)
	if len(bids) != 2 || bids[0].Price != "100.1" || bids[1].Quantity != "0" ||
		len(asks) != 1 || asks[0].Price != "100.2" {
		t.Fatalf("stored delta levels = bids=%+v asks=%+v", bids, asks)
	}

	// An in-progress exchange candle updates in place and closes without
	// creating a second row.
	broker.send(inputs.klineTopic, m7Kline(1, 2, "108", true))
	eventually(t, 2*time.Second, func() bool {
		closePrice, closed := readRuntimeKline(t, pool, inputs.klineID)
		return closePrice == "108" && closed
	}, "closed kline should replace the open revision")

	if err := store.UpdateInputDesiredStatus(
		context.Background(), inputs.groupID,
		model.StreamKindTrade, model.DesiredStatusPaused,
	); err != nil {
		t.Fatalf("pause trade: %v", err)
	}
	eventually(t, 2*time.Second, func() bool {
		return streamStatus(t, store, inputs.groupID, model.StreamKindTrade).ActualStatus ==
			model.ActualStatusPaused
	}, "trade should pause independently")

	broker.send(inputs.tradeTopic, m7Trade(1, "trade-2"))
	broker.send(inputs.deltaTopic, m7Delta(1, 101, "delta-101"))
	eventually(t, 2*time.Second, func() bool {
		return factCount(t, pool, "orderbook_deltas", inputs.deltaID) == 2
	}, "delta should continue while trade is paused")
	time.Sleep(100 * time.Millisecond)
	if got := factCount(t, pool, "trades", inputs.tradeID); got != 1 {
		t.Fatalf("trades while paused = %d, want 1", got)
	}

	if err := store.UpdateInputDesiredStatus(
		context.Background(), inputs.groupID,
		model.StreamKindTrade, model.DesiredStatusRunning,
	); err != nil {
		t.Fatalf("resume trade: %v", err)
	}
	eventually(t, 3*time.Second, func() bool {
		return factCount(t, pool, "trades", inputs.tradeID) == 2
	}, "resumed trade should consume its pending record")

	// Both delta idempotency paths are acknowledged without adding facts.
	broker.send(inputs.deltaTopic, m7Delta(2, 102, "delta-101"))
	broker.send(inputs.deltaTopic, m7Delta(1, 101, "delta-offset-replay"))
	broker.send(inputs.deltaTopic, m7Delta(3, 102, "delta-102"))
	eventually(t, 2*time.Second, func() bool {
		return broker.commitCount(inputs.deltaTopic) == 5
	}, "delta replays should still be committed")
	if got := factCount(t, pool, "orderbook_deltas", inputs.deltaID); got != 3 {
		t.Fatalf("delta rows after replays = %d, want 3", got)
	}

	// A monotonic jump is still a valid fact and should be written/committed.
	broker.send(inputs.deltaTopic, m7Delta(4, 104, "delta-104"))
	eventually(t, 2*time.Second, func() bool {
		return broker.commitCount(inputs.deltaTopic) == 6 &&
			factCount(t, pool, "orderbook_deltas", inputs.deltaID) == 4
	}, "monotonic sequence jump should be written and committed")
	status := streamStatus(t, store, inputs.groupID, model.StreamKindOrderBookDelta)
	if status.ActualStatus != model.ActualStatusRunning || status.LastError != "" {
		t.Fatalf("delta status after monotonic jump = %+v, want running without error", status)
	}
}

func TestKafkaM7EndToEnd(t *testing.T) {
	rawBrokers := strings.TrimSpace(os.Getenv("TEST_KAFKA_BROKERS"))
	if rawBrokers == "" {
		t.Skip("set TEST_KAFKA_BROKERS to run Kafka integration tests")
	}
	brokers := strings.Split(rawBrokers, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}

	store, pool := newStore(t)
	suffix := fmt.Sprintf("e2e%d", time.Now().UnixNano())
	inputs := createM7Group(t, store, suffix)
	for _, topic := range []string{inputs.tradeTopic, inputs.klineTopic, inputs.deltaTopic} {
		createKafkaTopic(t, brokers, topic)
	}

	nodeCfg := fastNodeConfig("node-m7-kafka")
	nodeCfg.ReconcileInterval = 25 * time.Millisecond
	node := NewNode(store, nodeCfg, discardLogger())
	node.EnableConsumption(
		storage.NewPostgresStorage(pool), kafka.NewReaderFactory(), brokers,
		25*time.Millisecond, 25*time.Millisecond,
	)
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- node.Run(runCtx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	writers := make(map[string]*kafkago.Writer)
	for _, topic := range []string{inputs.tradeTopic, inputs.klineTopic, inputs.deltaTopic} {
		writers[topic] = &kafkago.Writer{
			Addr: kafkago.TCP(brokers...), Topic: topic,
			Balancer: &kafkago.LeastBytes{}, RequiredAcks: kafkago.RequireAll,
			AllowAutoTopicCreation: false,
		}
	}
	t.Cleanup(func() {
		for _, writer := range writers {
			_ = writer.Close()
		}
	})
	write := func(topic string, msg kafka.Message) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		var lastErr error
		for time.Now().Before(deadline) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			lastErr = writers[topic].WriteMessages(ctx, kafkago.Message{Value: msg.Value})
			cancel()
			if lastErr == nil {
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
		t.Fatalf("write Kafka topic %s: %v", topic, lastErr)
	}

	write(inputs.tradeTopic, m7Trade(0, "trade-1"))
	write(inputs.klineTopic, m7Kline(0, 1, "105", false))
	write(inputs.deltaTopic, m7Delta(0, 500, "delta-500"))
	eventually(t, 15*time.Second, func() bool {
		return factCount(t, pool, "trades", inputs.tradeID) == 1 &&
			factCount(t, pool, "klines", inputs.klineID) == 1 &&
			factCount(t, pool, "orderbook_deltas", inputs.deltaID) == 1
	}, "real Kafka should feed all three PostgreSQL fact tables")

	write(inputs.klineTopic, m7Kline(1, 2, "109", true))
	eventually(t, 10*time.Second, func() bool {
		closePrice, closed := readRuntimeKline(t, pool, inputs.klineID)
		return closePrice == "109" && closed
	}, "real Kafka kline should update and close in place")

	if err := store.UpdateInputDesiredStatus(
		context.Background(), inputs.groupID, "kline_1m", model.DesiredStatusPaused,
	); err != nil {
		t.Fatalf("pause kline: %v", err)
	}
	eventually(t, 5*time.Second, func() bool {
		return streamStatus(t, store, inputs.groupID, "kline_1m").ActualStatus ==
			model.ActualStatusPaused
	}, "real Kafka kline consumer should pause")

	write(inputs.tradeTopic, m7Trade(1, "trade-2"))
	write(inputs.deltaTopic, m7Delta(1, 501, "delta-501"))
	eventually(t, 10*time.Second, func() bool {
		return factCount(t, pool, "trades", inputs.tradeID) == 2 &&
			factCount(t, pool, "orderbook_deltas", inputs.deltaID) == 2
	}, "pausing kline should not stop trade or delta")
}

func factCount(
	t *testing.T, pool *pgxpool.Pool, table, inputID string,
) int {
	t.Helper()
	var count int
	query := "SELECT count(*) FROM " + table + " WHERE input_id = $1"
	if err := pool.QueryRow(context.Background(), query, inputID).Scan(&count); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return count
}

func readRuntimeKline(
	t *testing.T, pool *pgxpool.Pool, inputID string,
) (string, bool) {
	t.Helper()
	var closePrice string
	var closed bool
	if err := pool.QueryRow(
		context.Background(),
		"SELECT close, is_closed FROM klines WHERE input_id = $1",
		inputID,
	).Scan(&closePrice, &closed); err != nil {
		t.Fatalf("read runtime kline: %v", err)
	}
	return closePrice, closed
}

func readRuntimeDeltaLevels(
	t *testing.T, pool *pgxpool.Pool, inputID string, offset int64,
) ([]model.PriceLevel, []model.PriceLevel) {
	t.Helper()
	var rawBids, rawAsks []byte
	if err := pool.QueryRow(
		context.Background(),
		`SELECT bids, asks FROM orderbook_deltas
		  WHERE input_id = $1 AND kafka_offset = $2`,
		inputID, offset,
	).Scan(&rawBids, &rawAsks); err != nil {
		t.Fatalf("read runtime delta levels: %v", err)
	}
	var bids, asks []model.PriceLevel
	if err := json.Unmarshal(rawBids, &bids); err != nil {
		t.Fatalf("decode stored bids: %v", err)
	}
	if err := json.Unmarshal(rawAsks, &asks); err != nil {
		t.Fatalf("decode stored asks: %v", err)
	}
	return bids, asks
}

func streamStatus(
	t *testing.T, store metadata.Store, groupID, streamKey string,
) model.StreamRuntimeStatus {
	t.Helper()
	statuses, err := store.ListStreamRuntimeStatus(context.Background(), groupID)
	if err != nil {
		t.Fatalf("ListStreamRuntimeStatus: %v", err)
	}
	for _, status := range statuses {
		if status.StreamKey == streamKey {
			return status
		}
	}
	return model.StreamRuntimeStatus{}
}
