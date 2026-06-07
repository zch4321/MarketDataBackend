package runtime

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"MarketDataBackend/internal/kafka"
	"MarketDataBackend/internal/model"
	"MarketDataBackend/internal/observability"
)

// WorkerState is the lifecycle state of a MarketGroupWorker.
type WorkerState int32

const (
	// WorkerStarting is the brief initial state before the lease loop runs.
	WorkerStarting WorkerState = iota
	// WorkerRunning means the worker holds the lease and is renewing it.
	WorkerRunning
	// WorkerStopping means a graceful stop was requested and is in progress.
	WorkerStopping
	// WorkerStopped is the terminal state after a graceful stop.
	WorkerStopped
	// WorkerError is the terminal state after losing the lease or a renew error.
	WorkerError
)

// String renders the state as the lower-case token used in logs and status.
func (s WorkerState) String() string {
	switch s {
	case WorkerStarting:
		return "starting"
	case WorkerRunning:
		return "running"
	case WorkerStopping:
		return "stopping"
	case WorkerStopped:
		return "stopped"
	case WorkerError:
		return "error"
	default:
		return "unknown"
	}
}

// terminal reports whether the state is one the worker never leaves.
func (s WorkerState) terminal() bool {
	return s == WorkerStopped || s == WorkerError
}

// workerStore is the subset of metadata.Store a Worker needs: renewing its
// group lease, reporting per-input runtime status, and (when consuming) listing
// the group's inputs so the input manager can react to per-input pause/resume.
type workerStore interface {
	RenewGroupLease(ctx context.Context, groupID, nodeID string, ttl time.Duration) (bool, error)
	ReportStreamRuntimeStatus(ctx context.Context, status model.StreamRuntimeStatus) error
	ListGroupInputs(ctx context.Context, groupID string) ([]model.GroupInput, error)
}

// ticker abstracts *time.Ticker so tests can drive lease renewals
// deterministically instead of waiting on the wall clock.
type ticker interface {
	C() <-chan time.Time
	Stop()
}

type realTicker struct{ t *time.Ticker }

func (r realTicker) C() <-chan time.Time { return r.t.C }
func (r realTicker) Stop()               { r.t.Stop() }

func newRealTicker(d time.Duration) ticker { return realTicker{t: time.NewTicker(d)} }

// reportTimeout bounds the fresh context used for the terminal status report,
// which must not reuse the (possibly canceled) run context.
const reportTimeout = 5 * time.Second

// Worker owns one MarketGroup on this node. It holds the group lease, renews it
// every renewEvery, and reports per-input runtime status. A lost lease (renew
// returns false) or a renew error moves the worker to a terminal state so the
// node manager can release and re-home the group.
//
// When consumption is enabled (storage and consumers are set), the worker also
// runs an inputManager that spins up one inputWorker per runnable trade, kline,
// or orderbook-delta input, and one snapshotInputWorker for the
// orderbook_snapshot input stream. With consumption disabled it behaves as in
// M5: lease-only, reporting status but not consuming.
type Worker struct {
	group      model.MarketGroup
	inputs     []model.GroupInput
	store      workerStore
	nodeID     string
	ttl        time.Duration
	renewEvery time.Duration
	logger     *slog.Logger
	newTicker  func(time.Duration) ticker

	// Consumption dependencies; nil unless enableConsumption was called.
	storage             factWriter
	consumers           kafka.Factory
	brokers             []string
	statusReportEvery   time.Duration
	inputReconcileEvery time.Duration
	consumptionConfig   ConsumptionConfig

	// M8 orderbook state and snapshot generation.
	orderBook        *orderBookState
	snapshotChan     chan snapshotResult
	snapshotInterval time.Duration
	stopSnapshot     chan struct{} // closed by finish() to stop the snapshot timer

	mu     sync.Mutex
	state  WorkerState
	cancel context.CancelFunc
	done   chan struct{}
}

// enableConsumption turns on Kafka consumption for this worker. It must be
// called before start. reportEvery bounds how often each input flushes its
// status; reconcileEvery bounds how often the input manager re-reads the
// group's inputs to react to per-input pause/resume.
func (w *Worker) enableConsumption(st factWriter, consumers kafka.Factory,
	brokers []string, reportEvery, reconcileEvery time.Duration) {
	w.enableConsumptionWithConfig(st, consumers, brokers, ConsumptionConfig{
		StatusReportEvery:   reportEvery,
		InputReconcileEvery: reconcileEvery,
	})
}

func (w *Worker) enableConsumptionWithConfig(
	st factWriter, consumers kafka.Factory, brokers []string, cfg ConsumptionConfig,
) {
	reportEvery := cfg.StatusReportEvery
	reconcileEvery := cfg.InputReconcileEvery
	if reportEvery <= 0 {
		reportEvery = time.Second
	}
	if reconcileEvery <= 0 {
		reconcileEvery = 5 * time.Second
	}
	snapInterval := cfg.SnapshotInterval
	if snapInterval <= 0 {
		snapInterval = time.Minute // M8 default, configurable
	}
	w.storage = st
	w.consumers = consumers
	w.brokers = brokers
	w.statusReportEvery = reportEvery
	w.inputReconcileEvery = reconcileEvery
	w.snapshotInterval = snapInterval
	cfg.StatusReportEvery = reportEvery
	cfg.InputReconcileEvery = reconcileEvery
	cfg.SnapshotInterval = snapInterval

	// Init orderbook state. snapInput is derived later when the snapshot input
	// worker is created — until then the state is dormant.
	snapInputID := ""
	for _, in := range w.inputs {
		if in.StreamKind == model.StreamKindOrderBookSnapshot {
			snapInputID = inputID(in)
			break
		}
	}
	w.orderBook = newOrderBookState(w.group.GroupID, snapInputID)
	w.snapshotChan = make(chan snapshotResult, 8)
	w.stopSnapshot = make(chan struct{})
	w.consumptionConfig = cfg
}

// consumptionEnabled reports whether the worker should run input consumers.
func (w *Worker) consumptionEnabled() bool {
	return w.storage != nil && w.consumers != nil
}

// newWorker constructs a worker in the Starting state. It does not run until
// start is called.
func newWorker(group model.MarketGroup, inputs []model.GroupInput, store workerStore,
	nodeID string, ttl, renewEvery time.Duration, logger *slog.Logger) *Worker {
	return &Worker{
		group:      group,
		inputs:     inputs,
		store:      store,
		nodeID:     nodeID,
		ttl:        ttl,
		renewEvery: renewEvery,
		logger:     logger,
		newTicker:  newRealTicker,
		state:      WorkerStarting,
		done:       make(chan struct{}),
	}
}

// start launches the worker's run loop in a new goroutine and returns
// immediately. The loop lives until parent is canceled, Stop is called, or the
// lease is lost.
func (w *Worker) start(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	w.mu.Lock()
	w.cancel = cancel
	w.mu.Unlock()
	go w.run(ctx)
}

func (w *Worker) run(ctx context.Context) {
	defer close(w.done)

	w.setState(WorkerStarting)
	w.reportAll(ctx, model.ActualStatusStarting, "")
	w.logger.Info("worker starting", "group_id", w.group.GroupID, "node_id", w.nodeID)

	w.setState(WorkerRunning)
	if w.consumptionEnabled() {
		w.reportUnsupportedInputs(ctx)
	} else {
		w.reportAll(ctx, model.ActualStatusRunning, "")
	}
	w.logger.Info("worker running", "group_id", w.group.GroupID, "node_id", w.nodeID)

	var im *inputManager
	if w.consumptionEnabled() {
		im = newInputManager(w)
		im.start()
		// M8: snapshot generation and reference-snapshot ingest.
		if w.orderBook != nil {
			go w.snapshotGenerateLoop(ctx)
			go w.snapshotReceiveLoop(ctx)
		}
	}

	state, status, lastErr := w.leaseLoop(ctx)

	// Stop input consumption before publishing the terminal status so the group
	// worker's reportAll is the last writer for every input.
	if im != nil {
		im.stop(state == WorkerStopped)
	}
	w.finish(state, status, lastErr)

	switch {
	case state == WorkerStopped:
		w.logger.Info("worker stopped", "group_id", w.group.GroupID, "node_id", w.nodeID)
	case lastErr == "lease lost":
		w.logger.Warn("worker lease lost", "group_id", w.group.GroupID, "node_id", w.nodeID)
	default:
		w.logger.Error("worker lease renew failed",
			"group_id", w.group.GroupID, "node_id", w.nodeID, "err", lastErr)
	}
}

// reportUnsupportedInputs leaves orderbook snapshots pending until M8. Status
// for the three M7 fact streams is owned by inputManager.
func (w *Worker) reportUnsupportedInputs(ctx context.Context) {
	for _, in := range w.inputs {
		if isConsumableStreamKind(in.StreamKind) {
			continue
		}
		st := model.StreamRuntimeStatus{
			InputID:      inputID(in),
			GroupID:      w.group.GroupID,
			StreamKey:    in.StreamKey,
			NodeID:       w.nodeID,
			ActualStatus: model.ActualStatusPending,
		}
		if err := w.store.ReportStreamRuntimeStatus(ctx, st); err != nil {
			w.logger.Warn("report pending stream status failed",
				"group_id", w.group.GroupID, "input_id", inputID(in), "err", err)
		}
	}
}

// leaseLoop renews the group lease until ctx is canceled (graceful stop) or the
// lease is lost/renewal errors. It returns the terminal worker state, the
// observed status to report and any last_error string.
func (w *Worker) leaseLoop(ctx context.Context) (WorkerState, string, string) {
	tk := w.newTicker(w.renewEvery)
	defer tk.Stop()

	for {
		select {
		case <-ctx.Done():
			// Graceful stop: the node manager canceled us (shutdown or the
			// group is no longer runnable).
			return WorkerStopped, model.ActualStatusStopped, ""
		case <-tk.C():
			ok, err := w.store.RenewGroupLease(ctx, w.group.GroupID, w.nodeID, w.ttl)
			switch {
			case err != nil:
				if ctx.Err() != nil {
					// Cancellation raced the renew; treat as a graceful stop.
					return WorkerStopped, model.ActualStatusStopped, ""
				}
				observability.IncLeaseFailures()
				return WorkerError, model.ActualStatusError, "renew lease: " + err.Error()
			case !ok:
				observability.IncLeaseFailures()
				// Lost the lease to another node (or it expired). Stop so the
				// group is not processed by two nodes at once.
				return WorkerError, model.ActualStatusError, "lease lost"
			}
		}
	}
}

// Stop requests a graceful stop and waits for the run loop to finish (bounded by
// ctx). It reports whether the worker fully stopped before the deadline.
func (w *Worker) Stop(ctx context.Context) bool {
	w.mu.Lock()
	cancel := w.cancel
	// Only surface the transient "stopping" state if the worker is still active;
	// never clobber a terminal state set by the run loop.
	if w.state == WorkerStarting || w.state == WorkerRunning {
		w.state = WorkerStopping
	}
	w.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	select {
	case <-w.done:
		return true
	case <-ctx.Done():
		return false
	}
}

// State returns the worker's current lifecycle state.
func (w *Worker) State() WorkerState {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.state
}

// finish sets the terminal state and reports the final per-input status using a
// fresh context, because the run context may already be canceled.
func (w *Worker) finish(state WorkerState, status, lastErr string) {
	w.setState(state)
	// Stop the snapshot generation loop before the final status report.
	if w.stopSnapshot != nil {
		select {
		case <-w.stopSnapshot:
		default:
			close(w.stopSnapshot)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), reportTimeout)
	defer cancel()
	w.reportAll(ctx, status, lastErr)
}

// reportAll upserts the observed runtime status for every input of the group.
// Reporting failures are logged but never abort the worker; status is best
// effort observability, not correctness.
func (w *Worker) reportAll(ctx context.Context, status, lastErr string) {
	for _, in := range w.inputs {
		st := model.StreamRuntimeStatus{
			InputID:      in.InputID,
			GroupID:      w.group.GroupID,
			StreamKey:    in.StreamKey,
			NodeID:       w.nodeID,
			ActualStatus: status,
			LastError:    lastErr,
		}
		if err := w.store.ReportStreamRuntimeStatus(ctx, st); err != nil {
			w.logger.Warn("report stream status failed",
				"group_id", w.group.GroupID, "input_id", in.InputID, "err", err)
		}
	}
}

func (w *Worker) setState(s WorkerState) {
	w.mu.Lock()
	w.state = s
	w.mu.Unlock()
}

// --- M8 snapshot generation and reference ingest -------------------------

// snapshotGenerateLoop periodically calls orderBookState.generateSnapshot and
// persists the result via WriteOrderBookSnapshots. It stops when stopSnapshot
// is closed (finish() is called) or ctx is done.
func (w *Worker) snapshotGenerateLoop(ctx context.Context) {
	ticker := time.NewTicker(w.snapshotInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stopSnapshot:
			return
		case <-ticker.C:
			if !w.orderBook.isReady() {
				continue
			}
			snap := w.orderBook.generateSnapshot(time.Now().UTC())
			if len(snap.Bids) == 0 && len(snap.Asks) == 0 {
				continue
			}
			if err := w.storage.WriteOrderBookSnapshots(ctx, []model.OrderBookSnapshot{snap}); err != nil {
				w.logger.Warn("snapshot write failed",
					"group_id", w.group.GroupID, "err", err)
				continue
			}
			w.logger.Debug("snapshot written",
				"group_id", w.group.GroupID, "input_id", snap.InputID,
				"bids", len(snap.Bids), "asks", len(snap.Asks))
		}
	}
}

// snapshotReceiveLoop reads reference snapshots from the snapshot channel
// (fed by the snapshotInputWorker) and applies them to the orderBookState.
// A sequence reset on a new reference snapshot is expected and idempotent.
func (w *Worker) snapshotReceiveLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stopSnapshot:
			return
		case result, ok := <-w.snapshotChan:
			if !ok {
				return
			}
			// Apply the reference snapshot — orders are validated first.
			// If validation fails the previous state is preserved.
			if err := w.orderBook.applySnapshot(result.snapshot); err != nil {
				w.logger.Warn("reference snapshot apply error — offset NOT committed",
					"group_id", w.group.GroupID, "err", err)
				// Do NOT close result.done: the snapshotInputWorker will
				// NOT commit this offset, so the message can be retried
				// on restart (or a fixed adapter).
				continue
			}
			w.logger.Debug("reference snapshot applied",
				"group_id", w.group.GroupID, "sequence", ptrVal(result.snapshot.Sequence),
				"bids", len(result.snapshot.Bids), "asks", len(result.snapshot.Asks))

			// Acknowledge application so the input worker can commit the
			// Kafka offset safely.
			if result.done != nil {
				close(result.done)
			}
		}
	}
}

// onDeltaApplied is the callback installed on every delta inputWorker. It is
// called after the delta batch commits in the database but before the Kafka
// offset is committed. A sequence gap resets the order book and waits for a
// new reference snapshot.
func (w *Worker) onDeltaApplied(delta model.OrderBookDelta) {
	if w.orderBook == nil {
		return
	}
	applied, err := w.orderBook.applyDelta(delta)
	if err != nil {
		w.logger.Error("orderbook delta apply failed — resetting state",
			"group_id", w.group.GroupID, "err", err)
		w.orderBook.reset()
		return
	}
	if applied {
		return
	}
	// Delta was buffered (book not yet ready) — this is normal.
}

func ptrVal(p *int64) int64 {
	if p == nil {
		return -1
	}
	return *p
}
