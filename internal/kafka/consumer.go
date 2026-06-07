// Package kafka is a thin consumer wrapper used by the stream-runtime input
// workers. The runtime depends only on the Consumer/Factory interfaces so the
// consume/commit logic can be exercised with an in-memory fake (no broker) and
// a real broker-backed implementation can be swapped in behind the same seam.
//
// Offset handling is at-least-once: a message is committed only after its
// payload has been durably written. A process that dies between "write" and
// "commit" re-delivers the message, which the idempotent fact-table writes turn
// into a no-op.
package kafka

import (
	"context"
	"fmt"
	"strings"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// Message is a single consumed record from one topic partition.
type Message struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
	// Time is the record timestamp as reported by the broker (may be zero).
	Time time.Time
	// HighWatermark is the offset just past the last record currently in the
	// partition. It is used to derive consumer lag and may be zero when the
	// implementation does not report it.
	HighWatermark int64
}

// Consumer reads records from a single topic partition for one consumer group
// and commits offsets manually.
type Consumer interface {
	// Fetch returns the next record, blocking until one is available or ctx is
	// done (in which case it returns ctx.Err()).
	Fetch(ctx context.Context) (Message, error)
	// Commit synchronously records msg as processed so a later consumer in the
	// same group resumes at the following offset.
	Commit(ctx context.Context, msg Message) error
	// Close releases any underlying network resources.
	Close() error
}

// Config describes a single input's consumer: which topic/partition to read and
// which consumer group owns the committed offset.
type Config struct {
	Brokers   []string
	Topic     string
	GroupID   string
	Partition int
}

// Factory creates consumers. It is injected into the runtime so tests can
// provide an in-memory broker instead of a real one.
type Factory interface {
	NewConsumer(ctx context.Context, cfg Config) (Consumer, error)
}

// ReaderFactory creates kafka-go consumer-group readers. FetchMessage is used
// instead of ReadMessage so offsets are never committed automatically.
type ReaderFactory struct{}

// NewReaderFactory returns a broker-backed consumer factory.
func NewReaderFactory() *ReaderFactory {
	return &ReaderFactory{}
}

// NewConsumer creates a single-topic consumer-group reader. Partition
// assignment is managed by Kafka; M6 topics are expected to have one partition.
func (f *ReaderFactory) NewConsumer(_ context.Context, cfg Config) (Consumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka: at least one broker is required")
	}
	brokers := make([]string, len(cfg.Brokers))
	for i, broker := range cfg.Brokers {
		brokers[i] = strings.TrimSpace(broker)
		if brokers[i] == "" {
			return nil, fmt.Errorf("kafka: broker address cannot be empty")
		}
	}
	if strings.TrimSpace(cfg.Topic) == "" {
		return nil, fmt.Errorf("kafka: topic is required")
	}
	if strings.TrimSpace(cfg.GroupID) == "" {
		return nil, fmt.Errorf("kafka: consumer group is required")
	}

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:        brokers,
		Topic:          cfg.Topic,
		GroupID:        cfg.GroupID,
		MinBytes:       1,
		MaxBytes:       10 << 20,
		MaxWait:        time.Second,
		CommitInterval: 0,
		StartOffset:    kafkago.FirstOffset,
	})
	return &readerConsumer{reader: reader}, nil
}

type readerConsumer struct {
	reader *kafkago.Reader
}

func (c *readerConsumer) Fetch(ctx context.Context) (Message, error) {
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return Message{}, err
	}
	return Message{
		Topic:         msg.Topic,
		Partition:     msg.Partition,
		Offset:        msg.Offset,
		Key:           append([]byte(nil), msg.Key...),
		Value:         append([]byte(nil), msg.Value...),
		Time:          msg.Time,
		HighWatermark: msg.HighWaterMark,
	}, nil
}

func (c *readerConsumer) Commit(ctx context.Context, msg Message) error {
	return c.reader.CommitMessages(ctx, kafkago.Message{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Key:       msg.Key,
		Value:     msg.Value,
		Time:      msg.Time,
	})
}

func (c *readerConsumer) Close() error {
	return c.reader.Close()
}

var _ Factory = (*ReaderFactory)(nil)
var _ Consumer = (*readerConsumer)(nil)
