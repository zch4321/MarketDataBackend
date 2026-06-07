package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"MarketDataBackend/internal/kafka"
	"MarketDataBackend/internal/model"
	"MarketDataBackend/internal/storage"
)

// writeBackoff is how long an input worker waits before retrying after a write
// or commit failure, so a struggling dependency is not hammered in a tight loop.
const writeBackoff = 200 * time.Millisecond

// statusReporter is the subset of the metadata store an input worker needs to
// publish its observed runtime status.
type statusReporter interface {
	ReportStreamRuntimeStatus(ctx context.Context, status model.StreamRuntimeStatus) error
}

type factWriter interface {
	WriteFactBatch(ctx context.Context, batch storage.FactBatch) error
	LoadStreamWriteProgress(
		ctx context.Context, inputID string, partition int,
	) (model.StreamWriteProgress, bool, error)
	WriteOrderBookSnapshots(ctx context.Context, rows []model.OrderBookSnapshot) error
}

// BatchConfig controls one stream kind's per-input batch.
type BatchConfig struct {
	Size          int
	FlushInterval time.Duration
}

func (c BatchConfig) withDefaults() BatchConfig {
	if c.Size <= 0 {
		c.Size = 1
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = 100 * time.Millisecond
	}
	return c
}

// ConsumptionConfig groups input status/reconcile cadence and per-kind batch
// settings for a runtime node.
type ConsumptionConfig struct {
	StatusReportEvery   time.Duration
	InputReconcileEvery time.Duration
	TradeBatch          BatchConfig
	KlineBatch          BatchConfig
	OrderBookDeltaBatch BatchConfig
	SnapshotInterval    time.Duration // M8: interval between local full-depth snapshots
}

func (c ConsumptionConfig) batchFor(kind string) BatchConfig {
	switch kind {
	case model.StreamKindTrade:
		return c.TradeBatch.withDefaults()
	case model.StreamKindKline:
		return c.KlineBatch.withDefaults()
	case model.StreamKindOrderBookDelta:
		return c.OrderBookDeltaBatch.withDefaults()
	default:
		return BatchConfig{}.withDefaults()
	}
}

type pendingRecord struct {
	msg       kafka.Message
	eventTime time.Time
	trade     *model.Trade
	kline     *model.Kline
	delta     *model.OrderBookDelta
}

// inputWorker consumes one trade, kline, or orderbook-delta input stream. It
// decodes and validates each record, writes the fact, and commits the offset
// only after a successful write (at-least-once).
type inputWorker struct {
	group       model.MarketGroup
	input       model.GroupInput
	consumer    kafka.Consumer
	storage     factWriter
	reporter    statusReporter
	nodeID      string
	reportEvery time.Duration
	batch       BatchConfig
	logger      *slog.Logger
	flushOnStop atomic.Bool

	// onDeltaApplied is called for each successfully persisted delta (M8).
	// It must be brief and non-blocking.
	onDeltaApplied func(delta model.OrderBookDelta)

	mu        sync.Mutex
	status    string
	lastErr   string
	partition int
	committed *int64
	highWM    *int64
	lastEvent *time.Time
	lastProc  *time.Time

	lastDeltaByPartition map[int]model.OrderBookDelta
	loadedProgress       map[int]bool
	durableByPartition   map[int]int64
	done                 chan struct{}
}

func newInputWorker(group model.MarketGroup, in model.GroupInput, consumer kafka.Consumer,
	st factWriter, reporter statusReporter, nodeID string,
	reportEvery time.Duration, batch BatchConfig, logger *slog.Logger) *inputWorker {
	if reportEvery <= 0 {
		reportEvery = time.Second
	}
	return &inputWorker{
		group:                group,
		input:                in,
		consumer:             consumer,
		storage:              st,
		reporter:             reporter,
		nodeID:               nodeID,
		reportEvery:          reportEvery,
		batch:                batch.withDefaults(),
		logger:               logger,
		status:               model.ActualStatusRunning,
		partition:            0,
		lastDeltaByPartition: make(map[int]model.OrderBookDelta),
		loadedProgress:       make(map[int]bool),
		durableByPartition:   make(map[int]int64),
		done:                 make(chan struct{}),
	}
}

// run drives the consume loop plus a periodic status reporter until ctx is
// canceled (graceful stop or lease loss). It always flushes a final status and
// closes the consumer on exit.
func (iw *inputWorker) run(ctx context.Context) {
	defer close(iw.done)

	iw.flush(ctx)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		iw.reportLoop(ctx)
	}()

	iw.consume(ctx)
	wg.Wait()

	// The run context is canceled on exit, so flush the final snapshot with a
	// fresh context. The status (running) is left as-is; the caller decides the
	// terminal status (paused on input stop, stopped/error on group stop).
	fctx, cancel := context.WithTimeout(context.Background(), reportTimeout)
	iw.flush(fctx)
	cancel()

	if err := iw.consumer.Close(); err != nil {
		iw.logger.Warn("close consumer failed", "input_id", inputID(iw.input), "err", err)
	}
}

// consume builds one contiguous per-partition batch, flushes it by size or
// interval, then commits only the highest message after the database confirms
// that facts and stream_write_progress are durable.
func (iw *inputWorker) consume(ctx context.Context) {
	var (
		pending      []pendingRecord
		pendingSince time.Time
	)
	defer func() {
		if !iw.flushOnStop.Load() || len(pending) == 0 {
			return
		}
		flushCtx, cancel := context.WithTimeout(context.Background(), reportTimeout)
		defer cancel()
		_ = iw.flushBatch(flushCtx, pending)
	}()
	for {
		msg, err := iw.fetch(ctx, pendingSince)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, context.DeadlineExceeded) && len(pending) > 0 {
				if iw.flushBatch(ctx, pending) {
					pending = nil
					pendingSince = time.Time{}
				}
				continue
			}
			iw.setStatus(model.ActualStatusError, "fetch: "+err.Error())
			iw.logger.Warn("fetch failed", "input_id", inputID(iw.input), "err", err)
			if !sleepCtx(ctx, writeBackoff) {
				return
			}
			continue
		}

		if len(pending) > 0 && pending[0].msg.Partition != msg.Partition {
			if !iw.flushBatch(ctx, pending) {
				return
			}
			pending = nil
			pendingSince = time.Time{}
		}

		if !iw.ensureProgress(ctx, msg.Partition) {
			return
		}

		record, stage, err := iw.prepare(msg)
		if err != nil {
			if len(pending) > 0 {
				if !iw.flushBatch(ctx, pending) {
					return
				}
				pending = nil
				pendingSince = time.Time{}
			}
			iw.setStatus(model.ActualStatusError, stage+": "+err.Error())
			iw.logger.Warn(stage+" failed; record left uncommitted",
				"input_id", inputID(iw.input), "stream_kind", iw.input.StreamKind,
				"offset", msg.Offset, "err", err)
			// M7 still has no dead-letter path. Preserve at-least-once by
			// keeping poison records and sequence gaps uncommitted.
			<-ctx.Done()
			return
		}
		if len(pending) == 0 {
			pendingSince = time.Now()
		}
		pending = append(pending, record)
		if len(pending) >= iw.batch.Size {
			if !iw.flushBatch(ctx, pending) {
				return
			}
			pending = nil
			pendingSince = time.Time{}
		}
	}
}

func (iw *inputWorker) fetch(
	ctx context.Context, pendingSince time.Time,
) (kafka.Message, error) {
	if pendingSince.IsZero() {
		return iw.consumer.Fetch(ctx)
	}
	remaining := iw.batch.FlushInterval - time.Since(pendingSince)
	if remaining <= 0 {
		return kafka.Message{}, context.DeadlineExceeded
	}
	fetchCtx, cancel := context.WithTimeout(ctx, remaining)
	defer cancel()
	return iw.consumer.Fetch(fetchCtx)
}

func (iw *inputWorker) ensureProgress(ctx context.Context, partition int) bool {
	if iw.loadedProgress[partition] {
		return true
	}
	for {
		progress, ok, err := iw.storage.LoadStreamWriteProgress(
			ctx, inputID(iw.input), partition,
		)
		if err == nil {
			iw.loadedProgress[partition] = true
			if ok {
				iw.durableByPartition[partition] = progress.DurableOffset
				iw.mu.Lock()
				off := progress.DurableOffset
				iw.partition = partition
				iw.committed = &off
				iw.mu.Unlock()
				if iw.input.StreamKind == model.StreamKindOrderBookDelta {
					iw.lastDeltaByPartition[partition] = model.OrderBookDelta{
						RawEventID:   progress.LastRawEventID,
						LastUpdateID: progress.LastUpdateID,
						Sequence:     progress.LastSequence,
					}
				}
			} else {
				iw.durableByPartition[partition] = -1
			}
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		iw.setStatus(model.ActualStatusError, "load progress: "+err.Error())
		iw.logger.Warn("load stream write progress failed; retrying",
			"input_id", inputID(iw.input), "partition", partition, "err", err)
		if !sleepCtx(ctx, writeBackoff) {
			return false
		}
	}
}

func (iw *inputWorker) prepare(msg kafka.Message) (pendingRecord, string, error) {
	if msg.Offset <= iw.durableByPartition[msg.Partition] {
		return pendingRecord{msg: msg}, "", nil
	}
	switch iw.input.StreamKind {
	case model.StreamKindTrade:
		trade, err := buildTrade(iw.input, msg)
		if err != nil {
			return pendingRecord{}, "decode", err
		}
		tradeCopy := trade
		return pendingRecord{
			msg:       msg,
			eventTime: trade.EventTime,
			trade:     &tradeCopy,
		}, "", nil

	case model.StreamKindKline:
		kline, err := buildKline(iw.input, msg)
		if err != nil {
			return pendingRecord{}, "decode", err
		}
		klineCopy := kline
		return pendingRecord{
			msg:       msg,
			eventTime: kline.CloseTime,
			kline:     &klineCopy,
		}, "", nil

	case model.StreamKindOrderBookDelta:
		delta, err := buildOrderBookDelta(iw.input, msg)
		if err != nil {
			return pendingRecord{}, "decode", err
		}
		previous := iw.lastDeltaByPartition[msg.Partition]
		var previousPtr *model.OrderBookDelta
		if _, ok := iw.lastDeltaByPartition[msg.Partition]; ok {
			previousPtr = &previous
		}
		advance, err := checkDeltaContinuity(previousPtr, delta)
		if err != nil {
			return pendingRecord{}, "sequence", err
		}
		delta.RawPayload = append([]byte(nil), msg.Value...)
		var deltaPtr *model.OrderBookDelta
		if advance {
			iw.lastDeltaByPartition[msg.Partition] = delta
			deltaCopy := delta
			deltaPtr = &deltaCopy
		}
		return pendingRecord{
			msg:       msg,
			eventTime: delta.EventTime,
			delta:     deltaPtr,
		}, "", nil

	default:
		return pendingRecord{}, "decode",
			fmt.Errorf("unsupported stream_kind %q", iw.input.StreamKind)
	}
}

func (iw *inputWorker) flushBatch(ctx context.Context, records []pendingRecord) bool {
	if len(records) == 0 {
		return true
	}
	last := records[len(records)-1]
	batch := storage.FactBatch{
		Progress: model.StreamWriteProgress{
			InputID:        inputID(iw.input),
			KafkaPartition: last.msg.Partition,
			DurableOffset:  last.msg.Offset,
		},
	}
	for _, record := range records {
		if record.trade != nil {
			batch.Trades = append(batch.Trades, *record.trade)
		}
		if record.kline != nil {
			batch.Klines = append(batch.Klines, *record.kline)
		}
		if record.delta != nil {
			batch.OrderBookDeltas = append(batch.OrderBookDeltas, *record.delta)
		}
	}
	if iw.input.StreamKind == model.StreamKindOrderBookDelta {
		cursor := iw.lastDeltaByPartition[last.msg.Partition]
		batch.Progress.LastRawEventID = cursor.RawEventID
		batch.Progress.LastUpdateID = cursor.LastUpdateID
		batch.Progress.LastSequence = cursor.Sequence
	}

	for {
		if err := iw.storage.WriteFactBatch(ctx, batch); err != nil {
			if ctx.Err() != nil {
				return false
			}
			iw.setStatus(model.ActualStatusError, "write: "+err.Error())
			iw.logger.Warn("batch write failed; retrying without committing",
				"input_id", inputID(iw.input), "records", len(records),
				"offset", last.msg.Offset, "err", err)
			if !sleepCtx(ctx, writeBackoff) {
				return false
			}
			continue
		}
		break
	}

	for {
		if err := iw.consumer.Commit(ctx, last.msg); err != nil {
			if ctx.Err() != nil {
				return false
			}
			iw.setStatus(model.ActualStatusError, "commit: "+err.Error())
			iw.logger.Warn("batch commit failed; retrying highest offset",
				"input_id", inputID(iw.input), "offset", last.msg.Offset, "err", err)
			if !sleepCtx(ctx, writeBackoff) {
				return false
			}
			continue
		}
		break
	}

	iw.durableByPartition[last.msg.Partition] = last.msg.Offset
	var eventTime *time.Time
	for i := len(records) - 1; i >= 0; i-- {
		if !records[i].eventTime.IsZero() {
			value := records[i].eventTime
			eventTime = &value
			break
		}
	}
	// M8: notify orderBookState about each persisted delta.
	if iw.onDeltaApplied != nil && iw.input.StreamKind == model.StreamKindOrderBookDelta {
		for _, record := range records {
			if record.delta != nil {
				iw.onDeltaApplied(*record.delta)
			}
		}
	}
	iw.advance(last.msg, eventTime)
	iw.setStatus(model.ActualStatusRunning, "")
	return true
}

// reportLoop periodically flushes the current status snapshot.
func (iw *inputWorker) reportLoop(ctx context.Context) {
	ticker := time.NewTicker(iw.reportEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			iw.flush(ctx)
		}
	}
}

// advance records progress after a record is committed: the committed offset and
// high watermark always move; the event time moves only for a real trade (nil
// for a skipped poison record).
func (iw *inputWorker) advance(msg kafka.Message, eventTime *time.Time) {
	iw.mu.Lock()
	defer iw.mu.Unlock()
	iw.partition = msg.Partition
	off := msg.Offset
	iw.committed = &off
	if msg.HighWatermark > 0 {
		hw := msg.HighWatermark
		iw.highWM = &hw
	}
	if eventTime != nil {
		iw.lastEvent = eventTime
	}
	now := time.Now().UTC()
	iw.lastProc = &now
}

func (iw *inputWorker) setStatus(status, lastErr string) {
	iw.mu.Lock()
	iw.status = status
	iw.lastErr = lastErr
	iw.mu.Unlock()
}

// flush publishes the current snapshot using the worker's stored status.
func (iw *inputWorker) flush(ctx context.Context) {
	iw.mu.Lock()
	st := iw.snapshotLocked(iw.status, iw.lastErr)
	iw.mu.Unlock()
	if err := iw.reporter.ReportStreamRuntimeStatus(ctx, st); err != nil {
		iw.logger.Warn("report status failed", "input_id", st.InputID, "err", err)
	}
}

// snapshot builds a status with an explicit status/last_error (used by the
// manager to report a paused input while keeping its offset counters).
func (iw *inputWorker) snapshot(status, lastErr string) model.StreamRuntimeStatus {
	iw.mu.Lock()
	defer iw.mu.Unlock()
	return iw.snapshotLocked(status, lastErr)
}

func (iw *inputWorker) snapshotLocked(status, lastErr string) model.StreamRuntimeStatus {
	part := iw.partition
	return model.StreamRuntimeStatus{
		InputID:             inputID(iw.input),
		GroupID:             iw.group.GroupID,
		StreamKey:           iw.input.StreamKey,
		NodeID:              iw.nodeID,
		ActualStatus:        status,
		KafkaPartition:      &part,
		KafkaLag:            lagLocked(iw.committed, iw.highWM),
		CommittedOffset:     iw.committed,
		HighWatermarkOffset: iw.highWM,
		LastEventTime:       iw.lastEvent,
		LastProcessedTime:   iw.lastProc,
		LastError:           lastErr,
	}
}

// lagLocked derives consumer lag from the last committed offset and the high
// watermark: lag = highWatermark - committed - 1, clamped at zero. It is nil
// when either value is unknown.
func lagLocked(committed, highWM *int64) *int64 {
	if committed == nil || highWM == nil {
		return nil
	}
	lag := *highWM - *committed - 1
	if lag < 0 {
		lag = 0
	}
	return &lag
}

// sleepCtx waits for d or until ctx is done. It returns false if ctx was
// canceled (so the caller should stop).
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// inputManager runs under a group Worker. It periodically reconciles the
// group's supported fact inputs against the desired state from the control
// plane, starting one inputWorker per runnable stream.
type inputManager struct {
	w      *Worker
	cancel context.CancelFunc
	done   chan struct{}

	mu        sync.Mutex
	active    map[string]*managedInput
	stopFlush bool
}

type managedInput struct {
	in     model.GroupInput
	iw     *inputWorker         // non-nil for fact streams
	sw     *snapshotInputWorker // non-nil for orderbook_snapshot
	cancel context.CancelFunc
	done   <-chan struct{}
}

func newInputManager(w *Worker) *inputManager {
	return &inputManager{w: w, active: make(map[string]*managedInput)}
}

// start launches the manager loop. The owning Worker stops it explicitly after
// leaseLoop resolves so graceful shutdown and lease loss can use different
// batch-drain policies.
func (m *inputManager) start() {
	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	m.done = make(chan struct{})
	go m.loop(ctx)
}

// stop cancels the manager (which stops all input workers) and waits for it to
// drain. It is safe to call once.
func (m *inputManager) stop(flush bool) {
	m.mu.Lock()
	m.stopFlush = flush
	m.mu.Unlock()
	if m.cancel != nil {
		m.cancel()
	}
	if m.done != nil {
		<-m.done
	}
}

func (m *inputManager) loop(ctx context.Context) {
	defer close(m.done)
	ticker := time.NewTicker(m.w.inputReconcileEvery)
	defer ticker.Stop()
	m.reconcile(ctx) // run once immediately so consumption starts promptly
	for {
		select {
		case <-ctx.Done():
			m.mu.Lock()
			flush := m.stopFlush
			m.mu.Unlock()
			m.stopAll(flush)
			return
		case <-ticker.C:
			m.reconcile(ctx)
		}
	}
}

// reconcile starts workers for newly-runnable supported inputs and stops
// workers for inputs that are no longer runnable.
func (m *inputManager) reconcile(ctx context.Context) {
	inputs, err := m.w.store.ListGroupInputs(ctx, m.w.group.GroupID)
	if err != nil {
		m.w.logger.Warn("list inputs failed", "group_id", m.w.group.GroupID, "err", err)
		return
	}
	desired := make(map[string]model.GroupInput)
	inactive := make(map[string]model.GroupInput)
	for _, in := range inputs {
		if !isConsumableStreamKind(in.StreamKind) {
			continue
		}
		if in.DesiredStatus == model.DesiredStatusRunning && in.Enabled {
			desired[inputID(in)] = in
		} else {
			inactive[inputID(in)] = in
		}
	}

	m.mu.Lock()
	var toStop []string
	for id := range m.active {
		if _, ok := desired[id]; !ok {
			toStop = append(toStop, id)
		}
	}
	m.mu.Unlock()

	sort.Strings(toStop)
	for _, id := range toStop {
		m.stopInput(id, true, true)
		delete(inactive, id)
	}
	for _, in := range inactive {
		m.reportInactive(in)
	}

	for id, in := range desired {
		m.mu.Lock()
		_, running := m.active[id]
		m.mu.Unlock()
		if running {
			continue
		}
		m.startInput(ctx, in)
	}
}

// startInput creates a consumer and the appropriate worker for one supported
// input. Fact streams (trade/kline/delta) get an inputWorker with batch writes;
// orderbook_snapshot gets a lightweight snapshotInputWorker.
func (m *inputManager) startInput(parent context.Context, in model.GroupInput) {
	if in.KafkaCluster != "" && in.KafkaCluster != "default" {
		m.reportStartError(in, "unsupported kafka cluster "+in.KafkaCluster)
		return
	}
	cfg := kafka.Config{
		Brokers:   m.w.brokers,
		Topic:     in.KafkaTopic,
		GroupID:   consumerGroupID(in),
		Partition: 0,
	}
	consumer, err := m.w.consumers.NewConsumer(parent, cfg)
	if err != nil {
		m.w.logger.Warn("create consumer failed", "input_id", inputID(in), "err", err)
		m.reportStartError(in, "consumer: "+err.Error())
		return
	}

	ctx, cancel := context.WithCancel(parent)
	var mi = managedInput{in: in, cancel: cancel}

	if in.StreamKind == model.StreamKindOrderBookSnapshot {
		sw := newSnapshotInputWorker(m.w.group, in, consumer,
			m.w.snapshotChan, m.w.nodeID, m.w.logger)
		go sw.run(ctx)
		mi.sw = sw
		mi.done = sw.done
	} else {
		iw := newInputWorker(m.w.group, in, consumer, m.w.storage, m.w.store,
			m.w.nodeID, m.w.statusReportEvery,
			m.w.consumptionConfig.batchFor(in.StreamKind), m.w.logger)
		// M8: hook delta application into the orderBookState.
		if in.StreamKind == model.StreamKindOrderBookDelta && m.w.orderBook != nil {
			iw.onDeltaApplied = m.w.onDeltaApplied
		}
		go iw.run(ctx)
		mi.iw = iw
		mi.done = iw.done
	}

	m.mu.Lock()
	m.active[inputID(in)] = &mi
	m.mu.Unlock()
	m.w.logger.Info("input worker started",
		"group_id", m.w.group.GroupID, "input_id", inputID(in),
		"stream_kind", in.StreamKind, "topic", in.KafkaTopic)
}

func isConsumableStreamKind(kind string) bool {
	switch kind {
	case model.StreamKindTrade, model.StreamKindKline, model.StreamKindOrderBookDelta,
		model.StreamKindOrderBookSnapshot:
		return true
	default:
		return false
	}
}

func (m *inputManager) reportStartError(in model.GroupInput, lastErr string) {
	ctx, cancel := context.WithTimeout(context.Background(), reportTimeout)
	defer cancel()
	st := model.StreamRuntimeStatus{
		InputID:      inputID(in),
		GroupID:      in.GroupID,
		StreamKey:    in.StreamKey,
		NodeID:       m.w.nodeID,
		ActualStatus: model.ActualStatusError,
		LastError:    lastErr,
	}
	if err := m.w.store.ReportStreamRuntimeStatus(ctx, st); err != nil {
		m.w.logger.Warn("report input start error failed", "input_id", inputID(in), "err", err)
	}
}

func (m *inputManager) reportInactive(in model.GroupInput) {
	ctx, cancel := context.WithTimeout(context.Background(), reportTimeout)
	defer cancel()
	st := model.StreamRuntimeStatus{
		InputID:      inputID(in),
		GroupID:      in.GroupID,
		StreamKey:    in.StreamKey,
		NodeID:       m.w.nodeID,
		ActualStatus: model.ActualStatusPaused,
	}
	if err := m.w.store.ReportStreamRuntimeStatus(ctx, st); err != nil {
		m.w.logger.Warn("report paused input failed", "input_id", inputID(in), "err", err)
	}
}

// stopInput cancels one input worker and waits for it to drain. When reportPause
// is set (the input became paused), a final paused status is published with the
// worker's last offset counters preserved.
func (m *inputManager) stopInput(id string, reportPause, flush bool) {
	m.mu.Lock()
	mi := m.active[id]
	delete(m.active, id)
	m.mu.Unlock()
	if mi == nil {
		return
	}
	if mi.iw != nil {
		mi.iw.flushOnStop.Store(flush)
	}
	mi.cancel()
	<-mi.done
	if reportPause {
		ctx, cancel := context.WithTimeout(context.Background(), reportTimeout)
		defer cancel()
		var st model.StreamRuntimeStatus
		if mi.iw != nil {
			st = mi.iw.snapshot(model.ActualStatusPaused, "")
		} else if mi.sw != nil {
			st = mi.sw.snapshot()
			st.ActualStatus = model.ActualStatusPaused
		}
		if err := m.w.store.ReportStreamRuntimeStatus(ctx, st); err != nil {
			m.w.logger.Warn("report paused status failed", "input_id", id, "err", err)
		}
	}
	m.w.logger.Info("input worker stopped", "group_id", m.w.group.GroupID, "input_id", id)
}

// stopAll stops every running input worker without overriding their status; the
// owning group worker publishes the terminal stopped/error status afterwards.
func (m *inputManager) stopAll(flush bool) {
	m.mu.Lock()
	ids := make([]string, 0, len(m.active))
	for id := range m.active {
		ids = append(ids, id)
	}
	m.mu.Unlock()
	for _, id := range ids {
		m.stopInput(id, false, flush)
	}
}
