package runtime

import (
	"context"
	"errors"
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

type memoryBrokerFactory struct {
	messages chan kafka.Message

	mu        sync.Mutex
	configs   []kafka.Config
	commits   []kafka.Message
	consumers []*memoryBrokerConsumer
}

type memoryBrokerConsumer struct {
	broker *memoryBrokerFactory
	closed chan struct{}
	once   sync.Once
}

func newMemoryBrokerFactory() *memoryBrokerFactory {
	return &memoryBrokerFactory{messages: make(chan kafka.Message, 16)}
}

func (f *memoryBrokerFactory) NewConsumer(
	_ context.Context, cfg kafka.Config,
) (kafka.Consumer, error) {
	c := &memoryBrokerConsumer{broker: f, closed: make(chan struct{})}
	f.mu.Lock()
	f.configs = append(f.configs, cfg)
	f.consumers = append(f.consumers, c)
	f.mu.Unlock()
	return c, nil
}

func (c *memoryBrokerConsumer) Fetch(ctx context.Context) (kafka.Message, error) {
	select {
	case <-ctx.Done():
		return kafka.Message{}, ctx.Err()
	case <-c.closed:
		return kafka.Message{}, context.Canceled
	case msg := <-c.broker.messages:
		return msg, nil
	}
}

func (c *memoryBrokerConsumer) Commit(_ context.Context, msg kafka.Message) error {
	c.broker.mu.Lock()
	c.broker.commits = append(c.broker.commits, msg)
	c.broker.mu.Unlock()
	return nil
}

func (c *memoryBrokerConsumer) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

func (f *memoryBrokerFactory) commitCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.commits)
}

func (f *memoryBrokerFactory) consumerCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.consumers)
}

func createTradeGroup(t *testing.T, store metadata.Store, topic, consumerGroup string) (string, string) {
	t.Helper()
	g := model.MarketGroup{
		Exchange:   "binance",
		MarketType: model.MarketTypeSpot,
		Symbol:     "BTCUSDT",
		Inputs: []model.GroupInput{{
			StreamKey:    model.StreamKindTrade,
			StreamKind:   model.StreamKindTrade,
			Enabled:      true,
			KafkaTopic:   topic,
			KafkaGroupID: consumerGroup,
		}},
	}
	if err := store.CreateGroup(context.Background(), g); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	groupID := model.NewGroupID(g.Exchange, g.MarketType, g.Symbol)
	return groupID, model.NewInputID(groupID, model.StreamKindTrade)
}

func runtimeTradeMessage(offset, highWatermark int64, rawID string) kafka.Message {
	return kafka.Message{
		Topic:         "md.trade.integration",
		Partition:     0,
		Offset:        offset,
		HighWatermark: highWatermark,
		Value: []byte(`{
			"event_time":"2026-06-07T10:00:00Z",
			"raw_trade_id":"` + rawID + `",
			"price":"100.25",
			"quantity":"0.5",
			"side":"buy"
		}`),
	}
}

func TestTradePipelineWritesCommitsDeduplicatesAndResumes(t *testing.T) {
	store, pool := newStore(t)
	ctx := context.Background()
	broker := newMemoryBrokerFactory()
	groupID, inputID := createTradeGroup(t, store, "md.trade.integration", "runtime-integration")

	cfg := fastNodeConfig("node-m6")
	cfg.ReconcileInterval = 20 * time.Millisecond
	node := NewNode(store, cfg, discardLogger())
	node.EnableConsumption(
		storage.NewPostgresStorage(pool),
		broker,
		[]string{"memory:9092"},
		20*time.Millisecond,
		20*time.Millisecond,
	)
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- node.Run(runCtx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	eventually(t, 2*time.Second, func() bool {
		return broker.consumerCount() == 1
	}, "trade consumer should start after the group lease is acquired")

	broker.messages <- runtimeTradeMessage(0, 1, "raw-1")
	eventually(t, 3*time.Second, func() bool {
		return tradeCount(t, pool, inputID) == 1 && broker.commitCount() == 1
	}, "first trade should be written before its offset is committed")

	// Replay both idempotency paths: the exact Kafka offset, then the same raw
	// trade id at a different offset. Both are committed but neither adds a row.
	broker.messages <- runtimeTradeMessage(0, 1, "raw-replayed-offset")
	broker.messages <- runtimeTradeMessage(1, 2, "raw-1")
	eventually(t, 3*time.Second, func() bool {
		return broker.commitCount() == 3
	}, "idempotent replays should still advance the consumer")
	if got := tradeCount(t, pool, inputID); got != 1 {
		t.Fatalf("trade rows after replays = %d, want 1", got)
	}

	eventually(t, 2*time.Second, func() bool {
		st := tradeStatus(t, store, groupID)
		return st.ActualStatus == model.ActualStatusRunning &&
			st.CommittedOffset != nil && *st.CommittedOffset == 1 &&
			st.KafkaLag != nil && *st.KafkaLag == 0 &&
			st.LastEventTime != nil && st.LastProcessedTime != nil
	}, "runtime status should expose offset, lag and processing timestamps")

	if err := store.UpdateInputDesiredStatus(
		ctx, groupID, model.StreamKindTrade, model.DesiredStatusPaused,
	); err != nil {
		t.Fatalf("pause input: %v", err)
	}
	eventually(t, 2*time.Second, func() bool {
		return tradeStatus(t, store, groupID).ActualStatus == model.ActualStatusPaused
	}, "paused input should stop its consumer")

	broker.messages <- runtimeTradeMessage(2, 3, "raw-2")
	time.Sleep(150 * time.Millisecond)
	if got := broker.commitCount(); got != 3 {
		t.Fatalf("commits while paused = %d, want 3", got)
	}
	if got := tradeCount(t, pool, inputID); got != 1 {
		t.Fatalf("trade rows while paused = %d, want 1", got)
	}

	if err := store.UpdateInputDesiredStatus(
		ctx, groupID, model.StreamKindTrade, model.DesiredStatusRunning,
	); err != nil {
		t.Fatalf("resume input: %v", err)
	}
	eventually(t, 3*time.Second, func() bool {
		return broker.consumerCount() == 2 &&
			broker.commitCount() == 4 &&
			tradeCount(t, pool, inputID) == 2
	}, "resumed input should continue with the pending record")

	broker.mu.Lock()
	if broker.configs[0].GroupID != broker.configs[1].GroupID {
		t.Errorf("consumer group changed across resume: %q -> %q",
			broker.configs[0].GroupID, broker.configs[1].GroupID)
	}
	broker.mu.Unlock()
}

func tradeCount(t *testing.T, pool *pgxpool.Pool, inputID string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(
		context.Background(), "SELECT count(*) FROM trades WHERE input_id = $1", inputID,
	).Scan(&count); err != nil {
		t.Fatalf("count trades: %v", err)
	}
	return count
}

func tradeStatus(t *testing.T, store metadata.Store, groupID string) model.StreamRuntimeStatus {
	t.Helper()
	statuses, err := store.ListStreamRuntimeStatus(context.Background(), groupID)
	if err != nil {
		t.Fatalf("ListStreamRuntimeStatus: %v", err)
	}
	if len(statuses) != 1 {
		return model.StreamRuntimeStatus{}
	}
	return statuses[0]
}

func TestKafkaTradeEndToEnd(t *testing.T) {
	rawBrokers := strings.TrimSpace(os.Getenv("TEST_KAFKA_BROKERS"))
	if rawBrokers == "" {
		t.Skip("set TEST_KAFKA_BROKERS to run Kafka integration tests")
	}
	brokers := strings.Split(rawBrokers, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}

	store, pool := newStore(t)
	suffix := time.Now().UnixNano()
	topic := fmt.Sprintf("md.trade.e2e.%d", suffix)
	consumerGroup := fmt.Sprintf("md-runtime-e2e-%d", suffix)
	symbol := fmt.Sprintf("E2E%d", suffix)
	group := model.MarketGroup{
		Exchange:   "test",
		MarketType: model.MarketTypeSpot,
		Symbol:     symbol,
		Inputs: []model.GroupInput{{
			StreamKey:    model.StreamKindTrade,
			StreamKind:   model.StreamKindTrade,
			Enabled:      true,
			KafkaTopic:   topic,
			KafkaGroupID: consumerGroup,
		}},
	}
	if err := store.CreateGroup(context.Background(), group); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	groupID := model.NewGroupID(group.Exchange, group.MarketType, group.Symbol)
	inputID := model.NewInputID(groupID, model.StreamKindTrade)
	createKafkaTopic(t, brokers, topic)

	nodeCfg := fastNodeConfig("node-kafka-e2e")
	nodeCfg.ReconcileInterval = 25 * time.Millisecond
	node := NewNode(store, nodeCfg, discardLogger())
	node.EnableConsumption(
		storage.NewPostgresStorage(pool),
		kafka.NewReaderFactory(),
		brokers,
		25*time.Millisecond,
		25*time.Millisecond,
	)
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- node.Run(runCtx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	writer := &kafkago.Writer{
		Addr:                   kafkago.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafkago.LeastBytes{},
		RequiredAcks:           kafkago.RequireAll,
		AllowAutoTopicCreation: false,
	}
	t.Cleanup(func() { _ = writer.Close() })
	write := func(rawID, price string) {
		t.Helper()
		payload := fmt.Sprintf(
			`{"event_time":"2026-06-07T10:00:00Z","raw_trade_id":%q,`+
				`"price":%q,"quantity":"0.5","side":"buy"}`,
			rawID, price,
		)
		deadline := time.Now().Add(10 * time.Second)
		var lastErr error
		for time.Now().Before(deadline) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			lastErr = writer.WriteMessages(ctx, kafkago.Message{Value: []byte(payload)})
			cancel()
			if lastErr == nil {
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
		t.Fatalf("write Kafka message: %v", lastErr)
	}

	write("e2e-raw-1", "100.25")
	eventually(t, 15*time.Second, func() bool {
		return tradeCount(t, pool, inputID) == 1
	}, "Kafka trade should reach PostgreSQL")
	eventually(t, 5*time.Second, func() bool {
		st := tradeStatus(t, store, groupID)
		return st.CommittedOffset != nil && *st.CommittedOffset == 0 &&
			st.LastEventTime != nil && st.LastProcessedTime != nil
	}, "successful PostgreSQL write should be followed by offset commit and status report")

	if err := store.UpdateInputDesiredStatus(
		context.Background(), groupID, model.StreamKindTrade, model.DesiredStatusPaused,
	); err != nil {
		t.Fatalf("pause input: %v", err)
	}
	eventually(t, 5*time.Second, func() bool {
		return tradeStatus(t, store, groupID).ActualStatus == model.ActualStatusPaused
	}, "real Kafka consumer should stop when the input is paused")

	write("e2e-raw-2", "101.50")
	time.Sleep(500 * time.Millisecond)
	if got := tradeCount(t, pool, inputID); got != 1 {
		t.Fatalf("trade rows while paused = %d, want 1", got)
	}

	if err := store.UpdateInputDesiredStatus(
		context.Background(), groupID, model.StreamKindTrade, model.DesiredStatusRunning,
	); err != nil {
		t.Fatalf("resume input: %v", err)
	}
	eventually(t, 15*time.Second, func() bool {
		return tradeCount(t, pool, inputID) == 2
	}, "resumed consumer should continue from its committed offset")

	// A duplicate exchange trade id at the next Kafka offset is committed but
	// remains one PostgreSQL fact because the storage write is idempotent.
	write("e2e-raw-2", "101.50")
	eventually(t, 10*time.Second, func() bool {
		st := tradeStatus(t, store, groupID)
		return st.CommittedOffset != nil && *st.CommittedOffset >= 2
	}, "duplicate raw trade should still be acknowledged")
	if got := tradeCount(t, pool, inputID); got != 2 {
		t.Fatalf("trade rows after duplicate raw id = %d, want 2", got)
	}
}

func createKafkaTopic(t *testing.T, brokers []string, topic string) {
	t.Helper()
	client := &kafkago.Client{
		Addr:    kafkago.TCP(brokers...),
		Timeout: 10 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	response, err := client.CreateTopics(ctx, &kafkago.CreateTopicsRequest{
		Topics: []kafkago.TopicConfig{{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		}},
	})
	cancel()
	if err != nil {
		t.Fatalf("create Kafka topic: %v", err)
	}
	if topicErr := response.Errors[topic]; topicErr != nil &&
		!errors.Is(topicErr, kafkago.TopicAlreadyExists) {
		t.Fatalf("create Kafka topic %q: %v", topic, topicErr)
	}

	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		conn, err := kafkago.DialLeader(ctx, "tcp", brokers[0], topic, 0)
		cancel()
		if err == nil {
			_ = conn.Close()
			return
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("Kafka topic %q leader did not become ready: %v", topic, lastErr)
}
