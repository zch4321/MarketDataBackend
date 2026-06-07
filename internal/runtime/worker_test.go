package runtime

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"MarketDataBackend/internal/model"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeWorkerStore is a controllable workerStore for state-machine tests.
type fakeWorkerStore struct {
	mu       sync.Mutex
	renewOK  bool
	renewErr error
	statuses []model.StreamRuntimeStatus
	inputs   []model.GroupInput
}

func (f *fakeWorkerStore) RenewGroupLease(_ context.Context, _, _ string, _ time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.renewOK, f.renewErr
}

func (f *fakeWorkerStore) ReportStreamRuntimeStatus(_ context.Context, st model.StreamRuntimeStatus) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statuses = append(f.statuses, st)
	return nil
}

func (f *fakeWorkerStore) ListGroupInputs(_ context.Context, _ string) ([]model.GroupInput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]model.GroupInput(nil), f.inputs...), nil
}

func (f *fakeWorkerStore) snapshot() []model.StreamRuntimeStatus {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]model.StreamRuntimeStatus(nil), f.statuses...)
}

func (f *fakeWorkerStore) lastStatus() (model.StreamRuntimeStatus, bool) {
	all := f.snapshot()
	if len(all) == 0 {
		return model.StreamRuntimeStatus{}, false
	}
	return all[len(all)-1], true
}

// manualTicker lets a test fire lease-renewal ticks deterministically.
type manualTicker struct{ ch chan time.Time }

func newManualTicker() *manualTicker { return &manualTicker{ch: make(chan time.Time, 1)} }

func (m *manualTicker) C() <-chan time.Time { return m.ch }
func (m *manualTicker) Stop()               {}
func (m *manualTicker) tick()               { m.ch <- time.Now() }

func newTestWorker(store workerStore, tk ticker) *Worker {
	g := model.MarketGroup{GroupID: "binance:spot:BTCUSDT", Weight: 1}
	inputs := []model.GroupInput{
		{InputID: "binance:spot:BTCUSDT:trade", GroupID: g.GroupID, StreamKey: "trade"},
	}
	w := newWorker(g, inputs, store, "node-a", 30*time.Second, time.Hour, discardLogger())
	w.newTicker = func(time.Duration) ticker { return tk }
	return w
}

func waitState(t *testing.T, w *Worker, want WorkerState) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if w.State() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("worker state = %v, want %v", w.State(), want)
}

func TestWorkerReachesRunningAndReports(t *testing.T) {
	store := &fakeWorkerStore{renewOK: true}
	w := newTestWorker(store, newManualTicker())
	w.start(context.Background())
	defer w.Stop(context.Background())

	waitState(t, w, WorkerRunning)

	got := store.snapshot()
	if len(got) < 2 {
		t.Fatalf("got %d status reports, want >= 2", len(got))
	}
	if got[0].ActualStatus != model.ActualStatusStarting {
		t.Errorf("first status = %q, want starting", got[0].ActualStatus)
	}
	if got[1].ActualStatus != model.ActualStatusRunning {
		t.Errorf("second status = %q, want running", got[1].ActualStatus)
	}
}

func TestWorkerGracefulStop(t *testing.T) {
	store := &fakeWorkerStore{renewOK: true}
	w := newTestWorker(store, newManualTicker())
	w.start(context.Background())
	waitState(t, w, WorkerRunning)

	if !w.Stop(context.Background()) {
		t.Fatal("worker did not stop")
	}

	if w.State() != WorkerStopped {
		t.Fatalf("state = %v, want stopped", w.State())
	}
	last, ok := store.lastStatus()
	if !ok || last.ActualStatus != model.ActualStatusStopped {
		t.Errorf("last status = %q, want stopped", last.ActualStatus)
	}
}

func TestWorkerLeaseLostMovesToError(t *testing.T) {
	store := &fakeWorkerStore{renewOK: false}
	tk := newManualTicker()
	w := newTestWorker(store, tk)
	w.start(context.Background())
	waitState(t, w, WorkerRunning)

	tk.tick() // renew returns (false, nil) -> lease lost
	waitState(t, w, WorkerError)

	last, _ := store.lastStatus()
	if last.ActualStatus != model.ActualStatusError {
		t.Errorf("last status = %q, want error", last.ActualStatus)
	}
	if last.LastError == "" {
		t.Error("expected last_error to be set when the lease is lost")
	}
}

func TestWorkerRenewErrorMovesToError(t *testing.T) {
	store := &fakeWorkerStore{renewErr: errors.New("db down")}
	tk := newManualTicker()
	w := newTestWorker(store, tk)
	w.start(context.Background())
	waitState(t, w, WorkerRunning)

	tk.tick() // renew returns an error -> error state
	waitState(t, w, WorkerError)

	last, _ := store.lastStatus()
	if last.ActualStatus != model.ActualStatusError {
		t.Errorf("last status = %q, want error", last.ActualStatus)
	}
}

func TestWorkerSuccessfulRenewStaysRunning(t *testing.T) {
	store := &fakeWorkerStore{renewOK: true}
	tk := newManualTicker()
	w := newTestWorker(store, tk)
	w.start(context.Background())
	defer w.Stop(context.Background())
	waitState(t, w, WorkerRunning)

	tk.tick()
	time.Sleep(10 * time.Millisecond)

	if got := w.State(); got != WorkerRunning {
		t.Fatalf("state = %v, want running after successful renew", got)
	}
}

func TestWorkerLeaseLossStopsAllM7Inputs(t *testing.T) {
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
	store := &fakeWorkerStore{renewOK: false, inputs: inputs}
	factory := &recordingFactory{}
	tk := newManualTicker()
	w := newWorker(
		model.MarketGroup{GroupID: groupID}, inputs, store,
		"node-a", time.Minute, time.Hour, discardLogger(),
	)
	w.newTicker = func(time.Duration) ticker { return tk }
	w.enableConsumption(
		&scriptedFactWriter{}, factory, []string{"broker:9092"},
		20*time.Millisecond, 10*time.Millisecond,
	)
	w.start(context.Background())
	waitState(t, w, WorkerRunning)
	waitFor(t, time.Second, func() bool { return factory.count() == 3 },
		"all M7 input consumers should start")

	tk.tick()
	waitState(t, w, WorkerError)
	waitFor(t, time.Second, func() bool { return factory.closedCount() == 3 },
		"lease loss should close every input consumer")
}

func TestWorkerStoppingStateObservable(t *testing.T) {
	// White-box: drive Stop directly so the transient "stopping" state is
	// deterministically observable before the run loop would close done.
	w := newWorker(model.MarketGroup{GroupID: "g"}, nil, &fakeWorkerStore{renewOK: true},
		"node-a", time.Second, time.Hour, discardLogger())
	w.state = WorkerRunning
	w.cancel = func() {}

	stopped := make(chan struct{})
	go func() {
		_ = w.Stop(context.Background())
		close(stopped)
	}()

	waitState(t, w, WorkerStopping)
	close(w.done) // release Stop's wait on done
	<-stopped
}

func TestWorkerStopReportsTimeout(t *testing.T) {
	w := newWorker(model.MarketGroup{GroupID: "g"}, nil, &fakeWorkerStore{renewOK: true},
		"node-a", time.Second, time.Hour, discardLogger())
	w.state = WorkerRunning
	w.cancel = func() {}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if w.Stop(ctx) {
		t.Fatal("Stop returned true before the worker completed")
	}
	close(w.done)
}

func TestWorkerStateString(t *testing.T) {
	cases := map[WorkerState]string{
		WorkerStarting:  "starting",
		WorkerRunning:   "running",
		WorkerStopping:  "stopping",
		WorkerStopped:   "stopped",
		WorkerError:     "error",
		WorkerState(99): "unknown",
	}
	for s, want := range cases {
		if got := s.String(); got != want {
			t.Errorf("WorkerState(%d).String() = %q, want %q", s, got, want)
		}
	}
}
