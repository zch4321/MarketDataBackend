package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"MarketDataBackend/internal/kafka"
	"MarketDataBackend/internal/model"
	"MarketDataBackend/internal/storage"
)

type scriptedConsumer struct {
	mu sync.Mutex

	messages   []kafka.Message
	commitErrs []error
	fetches    int
	commits    []kafka.Message
	closed     bool
}

func (c *scriptedConsumer) Fetch(ctx context.Context) (kafka.Message, error) {
	c.mu.Lock()
	c.fetches++
	if len(c.messages) > 0 {
		msg := c.messages[0]
		c.messages = c.messages[1:]
		c.mu.Unlock()
		return msg, nil
	}
	c.mu.Unlock()
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}

func (c *scriptedConsumer) Commit(_ context.Context, msg kafka.Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.commitErrs) > 0 {
		err := c.commitErrs[0]
		c.commitErrs = c.commitErrs[1:]
		if err != nil {
			return err
		}
	}
	c.commits = append(c.commits, msg)
	return nil
}

func (c *scriptedConsumer) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return nil
}

func (c *scriptedConsumer) counts() (fetches, commits int, closed bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.fetches, len(c.commits), c.closed
}

func (c *scriptedConsumer) committedMessages() []kafka.Message {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]kafka.Message(nil), c.commits...)
}

type scriptedFactWriter struct {
	mu sync.Mutex

	errs       []error
	defaultErr error
	calls      int
	trades     []model.Trade
	klines     []model.Kline
	deltas     []model.OrderBookDelta
	progress   map[string]model.StreamWriteProgress
}

func (w *scriptedFactWriter) nextError() error {
	w.calls++
	if len(w.errs) > 0 {
		err := w.errs[0]
		w.errs = w.errs[1:]
		if err != nil {
			return err
		}
	}
	return w.defaultErr
}

func (w *scriptedFactWriter) WriteFactBatch(
	_ context.Context, batch storage.FactBatch,
) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.nextError(); err != nil {
		return err
	}
	w.trades = append(w.trades, batch.Trades...)
	w.klines = append(w.klines, batch.Klines...)
	w.deltas = append(w.deltas, batch.OrderBookDeltas...)
	if w.progress == nil {
		w.progress = make(map[string]model.StreamWriteProgress)
	}
	w.progress[progressKey(batch.Progress.InputID, batch.Progress.KafkaPartition)] =
		batch.Progress
	return nil
}

func (w *scriptedFactWriter) LoadStreamWriteProgress(
	_ context.Context, inputID string, partition int,
) (model.StreamWriteProgress, bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.progress == nil {
		return model.StreamWriteProgress{}, false, nil
	}
	p, ok := w.progress[progressKey(inputID, partition)]
	return p, ok, nil
}

func (w *scriptedFactWriter) WriteOrderBookSnapshots(
	_ context.Context, _ []model.OrderBookSnapshot,
) error {
	return nil
}

func (w *scriptedFactWriter) WritePoisonRecord(
	_ context.Context, _ model.PoisonRecord,
) error {
	return nil
}

func progressKey(inputID string, partition int) string {
	return fmt.Sprintf("%s/%d", inputID, partition)
}

func (w *scriptedFactWriter) tradeSnapshot() (int, []model.Trade) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.calls, append([]model.Trade(nil), w.trades...)
}

func (w *scriptedFactWriter) factCounts() (trades, klines, deltas int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.trades), len(w.klines), len(w.deltas)
}

type memoryStatusReporter struct {
	mu       sync.Mutex
	statuses []model.StreamRuntimeStatus
}

func (r *memoryStatusReporter) ReportStreamRuntimeStatus(
	_ context.Context, st model.StreamRuntimeStatus,
) error {
	r.mu.Lock()
	r.statuses = append(r.statuses, st)
	r.mu.Unlock()
	return nil
}

func validTradeMessage(offset int64) kafka.Message {
	return kafka.Message{
		Topic:         "md.trade",
		Partition:     2,
		Offset:        offset,
		HighWatermark: 5,
		Value: []byte(`{
			"event_time":"2026-06-07T10:00:00Z",
			"raw_trade_id":"raw-1",
			"price":"100.5",
			"quantity":"0.25",
			"side":"sell"
		}`),
	}
}

func newTestInputWorker(
	consumer kafka.Consumer, writer factWriter, reporter statusReporter,
) *inputWorker {
	group := model.MarketGroup{GroupID: "binance:spot:BTCUSDT"}
	in := model.GroupInput{
		InputID:    group.GroupID + ":trade",
		GroupID:    group.GroupID,
		StreamKey:  "trade",
		StreamKind: model.StreamKindTrade,
	}
	return newInputWorker(
		group, in, consumer, writer, reporter, "node-a", 20*time.Millisecond,
		BatchConfig{Size: 1, FlushInterval: time.Second}, discardLogger(),
	)
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("condition not met: %s", msg)
}

func stopInputWorker(t *testing.T, cancel context.CancelFunc, iw *inputWorker) {
	t.Helper()
	cancel()
	select {
	case <-iw.done:
	case <-time.After(2 * time.Second):
		t.Fatal("input worker did not stop")
	}
}

func TestInputWorkerWritesThenCommitsAndReportsProgress(t *testing.T) {
	consumer := &scriptedConsumer{messages: []kafka.Message{validTradeMessage(2)}}
	writer := &scriptedFactWriter{}
	reporter := &memoryStatusReporter{}
	iw := newTestInputWorker(consumer, writer, reporter)
	ctx, cancel := context.WithCancel(context.Background())
	go iw.run(ctx)

	waitFor(t, time.Second, func() bool {
		_, commits, _ := consumer.counts()
		return commits == 1
	}, "trade should be committed")

	st := iw.snapshot(model.ActualStatusRunning, "")
	if st.KafkaPartition == nil || *st.KafkaPartition != 2 {
		t.Errorf("partition = %v, want 2", st.KafkaPartition)
	}
	if st.CommittedOffset == nil || *st.CommittedOffset != 2 {
		t.Errorf("committed offset = %v, want 2", st.CommittedOffset)
	}
	if st.HighWatermarkOffset == nil || *st.HighWatermarkOffset != 5 {
		t.Errorf("high watermark = %v, want 5", st.HighWatermarkOffset)
	}
	if st.KafkaLag == nil || *st.KafkaLag != 2 {
		t.Errorf("lag = %v, want 2", st.KafkaLag)
	}
	if st.LastEventTime == nil || st.LastProcessedTime == nil {
		t.Errorf("event/processed timestamps not reported: %+v", st)
	}
	calls, rows := writer.tradeSnapshot()
	if calls != 1 || len(rows) != 1 || rows[0].KafkaOffset != 2 {
		t.Errorf("writes = %d %+v", calls, rows)
	}

	stopInputWorker(t, cancel, iw)
	_, _, closed := consumer.counts()
	if !closed {
		t.Error("consumer was not closed")
	}
}

func TestInputWorkerRetriesSameRecordAfterWriteFailure(t *testing.T) {
	consumer := &scriptedConsumer{messages: []kafka.Message{validTradeMessage(0)}}
	writer := &scriptedFactWriter{errs: []error{errors.New("db unavailable"), nil}}
	iw := newTestInputWorker(consumer, writer, &memoryStatusReporter{})
	ctx, cancel := context.WithCancel(context.Background())
	go iw.run(ctx)

	waitFor(t, 2*time.Second, func() bool {
		calls, _ := writer.tradeSnapshot()
		_, commits, _ := consumer.counts()
		return calls >= 2 && commits == 1
	}, "same trade should be retried and then committed")

	fetches, _, _ := consumer.counts()
	if fetches != 2 {
		// The second Fetch is the worker blocking for the next message. A value
		// above two would mean it fetched past the failed record.
		t.Errorf("fetches = %d, want 2 after retry and next blocking fetch", fetches)
	}
	stopInputWorker(t, cancel, iw)
}

func TestInputWorkerRetriesCommitWithoutRewriting(t *testing.T) {
	consumer := &scriptedConsumer{
		messages:   []kafka.Message{validTradeMessage(0)},
		commitErrs: []error{errors.New("coordinator unavailable"), nil},
	}
	writer := &scriptedFactWriter{}
	iw := newTestInputWorker(consumer, writer, &memoryStatusReporter{})
	ctx, cancel := context.WithCancel(context.Background())
	go iw.run(ctx)

	waitFor(t, 2*time.Second, func() bool {
		_, commits, _ := consumer.counts()
		return commits == 1
	}, "commit should eventually succeed")

	calls, _ := writer.tradeSnapshot()
	if calls != 1 {
		t.Errorf("writes = %d, want 1 while retrying only commit", calls)
	}
	stopInputWorker(t, cancel, iw)
}

func TestInputWorkerDoesNotCommitPersistentWriteFailure(t *testing.T) {
	consumer := &scriptedConsumer{messages: []kafka.Message{validTradeMessage(0)}}
	writer := &scriptedFactWriter{defaultErr: errors.New("db down")}
	iw := newTestInputWorker(consumer, writer, &memoryStatusReporter{})
	ctx, cancel := context.WithCancel(context.Background())
	go iw.run(ctx)

	waitFor(t, time.Second, func() bool {
		calls, _ := writer.tradeSnapshot()
		return calls > 0
	}, "write should be attempted")
	stopInputWorker(t, cancel, iw)

	_, commits, _ := consumer.counts()
	if commits != 0 {
		t.Fatalf("commits = %d, want 0", commits)
	}
	st := iw.snapshot(model.ActualStatusError, "")
	if st.CommittedOffset != nil {
		t.Errorf("committed offset = %v, want nil", st.CommittedOffset)
	}
}

// TestInputWorkerDecodeErrorGoesToDeadLetter verifies that a decode error
// is persisted as a poison record and its offset committed (M10 dead-letter).
func TestInputWorkerDecodeErrorGoesToDeadLetter(t *testing.T) {
	msg := validTradeMessage(7)
	msg.Value = []byte(`{"event_time":1,"price":"bad","quantity":"1","side":"buy"}`)
	consumer := &scriptedConsumer{messages: []kafka.Message{msg}}
	writer := &scriptedFactWriter{}
	iw := newTestInputWorker(consumer, writer, &memoryStatusReporter{})
	ctx, cancel := context.WithCancel(context.Background())
	go iw.run(ctx)

	// The worker processes the poison, commits the offset, and continues.
	// Wait for the error state to appear.
	waitFor(t, time.Second, func() bool {
		return iw.StateForTest() == model.ActualStatusError
	}, "decode error should be visible")
	_ = cancel // stop the worker
	stopInputWorker(t, cancel, iw)

	_, commits, _ := consumer.counts()
	calls, _ := writer.tradeSnapshot()
	// The dead-letter path commits the poison offset, but never writes
	// a fact batch because decode failed.
	if commits != 1 || calls != 0 {
		t.Errorf("commits/writes = %d/%d, want 1/0", commits, calls)
	}
}

// StateForTest returns only the in-memory status token without overriding it.
func (iw *inputWorker) StateForTest() string {
	iw.mu.Lock()
	defer iw.mu.Unlock()
	return iw.status
}

type recordingFactory struct {
	mu        sync.Mutex
	configs   []kafka.Config
	consumers []*scriptedConsumer
}

func (f *recordingFactory) NewConsumer(_ context.Context, cfg kafka.Config) (kafka.Consumer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c := &scriptedConsumer{}
	f.configs = append(f.configs, cfg)
	f.consumers = append(f.consumers, c)
	return c, nil
}

func (f *recordingFactory) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.consumers)
}

func (f *recordingFactory) closedCount() int {
	f.mu.Lock()
	consumers := append([]*scriptedConsumer(nil), f.consumers...)
	f.mu.Unlock()
	closed := 0
	for _, consumer := range consumers {
		_, _, isClosed := consumer.counts()
		if isClosed {
			closed++
		}
	}
	return closed
}

func TestInputManagerPausesAndResumesWithSameConsumerGroup(t *testing.T) {
	groupID := "binance:spot:BTCUSDT"
	in := model.GroupInput{
		InputID:       groupID + ":trade",
		GroupID:       groupID,
		StreamKey:     "trade",
		StreamKind:    model.StreamKindTrade,
		Enabled:       true,
		KafkaTopic:    "md.trade",
		KafkaGroupID:  "stable-group",
		DesiredStatus: model.DesiredStatusRunning,
	}
	store := &fakeWorkerStore{renewOK: true, inputs: []model.GroupInput{in}}
	factory := &recordingFactory{}
	w := newWorker(
		model.MarketGroup{GroupID: groupID}, []model.GroupInput{in}, store,
		"node-a", time.Minute, time.Hour, discardLogger(),
	)
	w.enableConsumption(&scriptedFactWriter{}, factory, []string{"broker:9092"},
		20*time.Millisecond, 10*time.Millisecond)
	w.start(context.Background())
	waitState(t, w, WorkerRunning)
	waitFor(t, time.Second, func() bool { return factory.count() == 1 }, "consumer should start")

	store.mu.Lock()
	store.inputs[0].DesiredStatus = model.DesiredStatusPaused
	store.mu.Unlock()
	waitFor(t, time.Second, func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		if len(store.statuses) == 0 {
			return false
		}
		return store.statuses[len(store.statuses)-1].ActualStatus == model.ActualStatusPaused
	}, "paused status should be reported")

	store.mu.Lock()
	store.inputs[0].DesiredStatus = model.DesiredStatusRunning
	store.mu.Unlock()
	waitFor(t, time.Second, func() bool { return factory.count() == 2 }, "consumer should restart")

	factory.mu.Lock()
	if factory.configs[0].GroupID != "stable-group" || factory.configs[1].GroupID != "stable-group" {
		t.Errorf("consumer groups = %q/%q", factory.configs[0].GroupID, factory.configs[1].GroupID)
	}
	factory.mu.Unlock()
	if !w.Stop(context.Background()) {
		t.Fatal("worker did not stop")
	}
}

func validKlineMessage(offset int64, closePrice string, revision int64, closed bool) kafka.Message {
	return kafka.Message{
		Topic:         "md.kline.1m",
		Partition:     0,
		Offset:        offset,
		HighWatermark: offset + 1,
		Value: []byte(fmt.Sprintf(`{
			"interval":"1m",
			"open_time":"2026-06-07T10:00:00Z",
			"close_time":"2026-06-07T10:01:00Z",
			"open":"100","high":"120","low":"90","close":%q,
			"volume":"10","quote_volume":"1000","trade_count":5,
			"is_closed":%t,"revision":%d
		}`, closePrice, closed, revision)),
	}
}

func validDeltaMessage(offset, sequence int64) kafka.Message {
	return kafka.Message{
		Topic:         "md.depth",
		Partition:     0,
		Offset:        offset,
		HighWatermark: offset + 1,
		Value: []byte(fmt.Sprintf(`{
			"event_time":"2026-06-07T10:00:00Z",
			"raw_event_id":"evt-%d",
			"sequence":%d,
			"bids":[["100","1"]],
			"asks":[["101","2"]]
		}`, sequence, sequence)),
	}
}

func TestInputWorkerWritesKlineAndCommits(t *testing.T) {
	consumer := &scriptedConsumer{messages: []kafka.Message{
		validKlineMessage(0, "105", 1, false),
	}}
	writer := &scriptedFactWriter{}
	group := model.MarketGroup{GroupID: "binance:spot:BTCUSDT"}
	in := model.GroupInput{
		InputID:    group.GroupID + ":kline_1m",
		GroupID:    group.GroupID,
		StreamKey:  "kline_1m",
		StreamKind: model.StreamKindKline,
		Interval:   "1m",
	}
	iw := newInputWorker(
		group, in, consumer, writer, &memoryStatusReporter{},
		"node-a", 20*time.Millisecond,
		BatchConfig{Size: 1, FlushInterval: time.Second}, discardLogger(),
	)
	ctx, cancel := context.WithCancel(context.Background())
	go iw.run(ctx)

	waitFor(t, time.Second, func() bool {
		_, commits, _ := consumer.counts()
		_, klines, _ := writer.factCounts()
		return commits == 1 && klines == 1
	}, "kline should be written and committed")
	stopInputWorker(t, cancel, iw)
}

func TestInputWorkerMonotonicSequenceGapIsWrittenAndCommitted(t *testing.T) {
	consumer := &scriptedConsumer{messages: []kafka.Message{
		validDeltaMessage(0, 10),
		validDeltaMessage(1, 12),
	}}
	writer := &scriptedFactWriter{}
	group := model.MarketGroup{GroupID: "binance:spot:BTCUSDT"}
	in := model.GroupInput{
		InputID:    group.GroupID + ":orderbook_delta",
		GroupID:    group.GroupID,
		StreamKey:  model.StreamKindOrderBookDelta,
		StreamKind: model.StreamKindOrderBookDelta,
	}
	iw := newInputWorker(
		group, in, consumer, writer, &memoryStatusReporter{},
		"node-a", 20*time.Millisecond,
		BatchConfig{Size: 1, FlushInterval: time.Second}, discardLogger(),
	)
	ctx, cancel := context.WithCancel(context.Background())
	go iw.run(ctx)

	waitFor(t, time.Second, func() bool {
		st := iw.snapshot(model.ActualStatusRunning, "")
		return st.CommittedOffset != nil && *st.CommittedOffset == 1
	}, "monotonic sequence gap should be written and committed")
	stopInputWorker(t, cancel, iw)

	_, commits, _ := consumer.counts()
	_, _, deltas := writer.factCounts()
	if commits != 2 || deltas != 2 {
		t.Fatalf("commits/deltas = %d/%d, want 2/2", commits, deltas)
	}
	iw.mu.Lock()
	lastErr := iw.lastErr
	iw.mu.Unlock()
	if lastErr != "" {
		t.Errorf("last_error = %q, want empty", lastErr)
	}
}

func TestInputWorkerFlushesOneBatchAtConfiguredSize(t *testing.T) {
	consumer := &scriptedConsumer{messages: []kafka.Message{
		validTradeMessage(0), validTradeMessage(1), validTradeMessage(2),
	}}
	writer := &scriptedFactWriter{}
	group := model.MarketGroup{GroupID: "binance:spot:BTCUSDT"}
	in := model.GroupInput{
		InputID: group.GroupID + ":trade", GroupID: group.GroupID,
		StreamKey: "trade", StreamKind: model.StreamKindTrade,
	}
	iw := newInputWorker(
		group, in, consumer, writer, &memoryStatusReporter{}, "node-a",
		time.Second, BatchConfig{Size: 3, FlushInterval: time.Hour}, discardLogger(),
	)
	ctx, cancel := context.WithCancel(context.Background())
	go iw.run(ctx)

	waitFor(t, time.Second, func() bool {
		_, commits, _ := consumer.counts()
		return commits == 1
	}, "three records should flush as one batch")
	calls, trades := writer.tradeSnapshot()
	if calls != 1 || len(trades) != 3 {
		t.Fatalf("batch writes/trades = %d/%d, want 1/3", calls, len(trades))
	}
	committed := consumer.committedMessages()
	if len(committed) != 1 || committed[0].Offset != 2 {
		t.Fatalf("committed messages = %+v, want only offset 2", committed)
	}
	stopInputWorker(t, cancel, iw)
}

func TestInputWorkerFlushesBatchOnInterval(t *testing.T) {
	consumer := &scriptedConsumer{messages: []kafka.Message{validTradeMessage(7)}}
	writer := &scriptedFactWriter{}
	group := model.MarketGroup{GroupID: "binance:spot:BTCUSDT"}
	in := model.GroupInput{
		InputID: group.GroupID + ":trade", GroupID: group.GroupID,
		StreamKey: "trade", StreamKind: model.StreamKindTrade,
	}
	iw := newInputWorker(
		group, in, consumer, writer, &memoryStatusReporter{}, "node-a",
		time.Second, BatchConfig{Size: 10, FlushInterval: 25 * time.Millisecond},
		discardLogger(),
	)
	ctx, cancel := context.WithCancel(context.Background())
	go iw.run(ctx)

	waitFor(t, time.Second, func() bool {
		_, commits, _ := consumer.counts()
		return commits == 1
	}, "flush interval should commit a partial batch")
	calls, trades := writer.tradeSnapshot()
	if calls != 1 || len(trades) != 1 {
		t.Fatalf("interval batch writes/trades = %d/%d, want 1/1", calls, len(trades))
	}
	stopInputWorker(t, cancel, iw)
}

func TestInputWorkerGracefulStopFlushesButLeaseLossDiscards(t *testing.T) {
	newPendingWorker := func(writer *scriptedFactWriter) (
		*inputWorker, *scriptedConsumer, context.CancelFunc,
	) {
		consumer := &scriptedConsumer{messages: []kafka.Message{validTradeMessage(0)}}
		group := model.MarketGroup{GroupID: "binance:spot:BTCUSDT"}
		in := model.GroupInput{
			InputID: group.GroupID + ":trade", GroupID: group.GroupID,
			StreamKey: "trade", StreamKind: model.StreamKindTrade,
		}
		iw := newInputWorker(
			group, in, consumer, writer, &memoryStatusReporter{}, "node-a",
			time.Second, BatchConfig{Size: 10, FlushInterval: time.Hour}, discardLogger(),
		)
		ctx, cancel := context.WithCancel(context.Background())
		go iw.run(ctx)
		waitFor(t, time.Second, func() bool {
			fetches, _, _ := consumer.counts()
			return fetches >= 2
		}, "worker should hold one pending record")
		return iw, consumer, cancel
	}

	gracefulWriter := &scriptedFactWriter{}
	graceful, gracefulConsumer, gracefulCancel := newPendingWorker(gracefulWriter)
	graceful.flushOnStop.Store(true)
	stopInputWorker(t, gracefulCancel, graceful)
	if calls, rows := gracefulWriter.tradeSnapshot(); calls != 1 || len(rows) != 1 {
		t.Fatalf("graceful writes/trades = %d/%d, want 1/1", calls, len(rows))
	}
	if _, commits, _ := gracefulConsumer.counts(); commits != 1 {
		t.Fatalf("graceful commits = %d, want 1", commits)
	}

	lostWriter := &scriptedFactWriter{}
	lost, lostConsumer, lostCancel := newPendingWorker(lostWriter)
	stopInputWorker(t, lostCancel, lost)
	if calls, rows := lostWriter.tradeSnapshot(); calls != 0 || len(rows) != 0 {
		t.Fatalf("lease-loss writes/trades = %d/%d, want 0/0", calls, len(rows))
	}
	if _, commits, _ := lostConsumer.counts(); commits != 0 {
		t.Fatalf("lease-loss commits = %d, want 0", commits)
	}
}

func TestInputWorkerDurableReplayCommitsWithoutRewritingFact(t *testing.T) {
	group := model.MarketGroup{GroupID: "binance:spot:BTCUSDT"}
	in := model.GroupInput{
		InputID: group.GroupID + ":trade", GroupID: group.GroupID,
		StreamKey: "trade", StreamKind: model.StreamKindTrade,
	}
	writer := &scriptedFactWriter{
		progress: map[string]model.StreamWriteProgress{
			progressKey(in.InputID, 2): {
				InputID: in.InputID, KafkaPartition: 2, DurableOffset: 9,
			},
		},
	}
	consumer := &scriptedConsumer{messages: []kafka.Message{validTradeMessage(9)}}
	iw := newInputWorker(
		group, in, consumer, writer, &memoryStatusReporter{}, "node-a",
		time.Second, BatchConfig{Size: 1, FlushInterval: time.Hour}, discardLogger(),
	)
	ctx, cancel := context.WithCancel(context.Background())
	go iw.run(ctx)

	waitFor(t, time.Second, func() bool {
		_, commits, _ := consumer.counts()
		return commits == 1
	}, "durable replay should still advance Kafka")
	calls, trades := writer.tradeSnapshot()
	if calls != 1 || len(trades) != 0 {
		t.Fatalf("replay writes/trades = %d/%d, want progress-only 1/0", calls, len(trades))
	}
	stopInputWorker(t, cancel, iw)
}

func TestInputWorkerMonotonicSequenceGapFlushesFullBatch(t *testing.T) {
	consumer := &scriptedConsumer{messages: []kafka.Message{
		validDeltaMessage(0, 10),
		validDeltaMessage(1, 11),
		validDeltaMessage(2, 13),
	}}
	writer := &scriptedFactWriter{}
	group := model.MarketGroup{GroupID: "binance:spot:BTCUSDT"}
	in := model.GroupInput{
		InputID: group.GroupID + ":orderbook_delta", GroupID: group.GroupID,
		StreamKey:  model.StreamKindOrderBookDelta,
		StreamKind: model.StreamKindOrderBookDelta,
	}
	iw := newInputWorker(
		group, in, consumer, writer, &memoryStatusReporter{}, "node-a",
		time.Second, BatchConfig{Size: 3, FlushInterval: time.Hour}, discardLogger(),
	)
	ctx, cancel := context.WithCancel(context.Background())
	go iw.run(ctx)

	waitFor(t, time.Second, func() bool {
		_, commits, _ := consumer.counts()
		return commits == 1
	}, "monotonic sequence gap should flush the full batch")
	_, commits, _ := consumer.counts()
	_, _, deltas := writer.factCounts()
	calls, _ := writer.tradeSnapshot()
	if calls != 1 || commits != 1 || deltas != 3 {
		t.Fatalf("batch writes/commits/deltas = %d/%d/%d, want 1/1/3",
			calls, commits, deltas)
	}
	committed := consumer.committedMessages()
	if len(committed) != 1 || committed[0].Offset != 2 {
		t.Fatalf("batch committed = %+v, want offset 2", committed)
	}
	stopInputWorker(t, cancel, iw)
}

func TestInputManagerRunsThreeKindsAndPausesOneIndependently(t *testing.T) {
	groupID := "binance:spot:BTCUSDT"
	inputs := []model.GroupInput{
		{
			InputID: groupID + ":trade", GroupID: groupID,
			StreamKey: "trade", StreamKind: model.StreamKindTrade,
			Enabled: true, KafkaTopic: "md.trade", DesiredStatus: model.DesiredStatusRunning,
		},
		{
			InputID: groupID + ":kline_1m", GroupID: groupID,
			StreamKey: "kline_1m", StreamKind: model.StreamKindKline, Interval: "1m",
			Enabled: true, KafkaTopic: "md.kline", DesiredStatus: model.DesiredStatusRunning,
		},
		{
			InputID: groupID + ":orderbook_delta", GroupID: groupID,
			StreamKey: "orderbook_delta", StreamKind: model.StreamKindOrderBookDelta,
			Enabled: true, KafkaTopic: "md.depth", DesiredStatus: model.DesiredStatusRunning,
		},
	}
	store := &fakeWorkerStore{renewOK: true, inputs: inputs}
	factory := &recordingFactory{}
	w := newWorker(
		model.MarketGroup{GroupID: groupID}, inputs, store,
		"node-a", time.Minute, time.Hour, discardLogger(),
	)
	w.enableConsumption(
		&scriptedFactWriter{}, factory, []string{"broker:9092"},
		20*time.Millisecond, 10*time.Millisecond,
	)
	w.start(context.Background())
	waitState(t, w, WorkerRunning)
	waitFor(t, time.Second, func() bool { return factory.count() == 3 },
		"all three consumers should start")

	store.mu.Lock()
	store.inputs[1].DesiredStatus = model.DesiredStatusPaused
	store.mu.Unlock()
	waitFor(t, time.Second, func() bool {
		return factory.closedCount() == 1
	}, "pausing kline should close only its consumer")

	if !w.Stop(context.Background()) {
		t.Fatal("worker did not stop")
	}
}
