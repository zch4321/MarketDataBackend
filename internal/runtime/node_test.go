package runtime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"MarketDataBackend/internal/model"
	"MarketDataBackend/internal/storage"
)

type fakeNodeStore struct {
	mu sync.Mutex

	runnable      []model.MarketGroup
	blocked       map[string]bool
	inputErr      map[string]error
	acquireCalls  []string
	releaseCalls  []string
	releaseOnDone bool
}

func (f *fakeNodeStore) RegisterRuntimeNode(context.Context, model.RuntimeNode) error {
	return nil
}

func (f *fakeNodeStore) HeartbeatRuntimeNode(context.Context, string, model.RuntimeCapacity) error {
	return nil
}

func (f *fakeNodeStore) UpdateRuntimeNodeStatus(context.Context, string, string) error {
	return nil
}

func (f *fakeNodeStore) ListRunnableGroups(context.Context) ([]model.MarketGroup, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]model.MarketGroup(nil), f.runnable...), nil
}

func (f *fakeNodeStore) TryAcquireGroupLease(_ context.Context, groupID, _ string, _ time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acquireCalls = append(f.acquireCalls, groupID)
	return !f.blocked[groupID], nil
}

func (f *fakeNodeStore) RenewGroupLease(context.Context, string, string, time.Duration) (bool, error) {
	return true, nil
}

func (f *fakeNodeStore) ReleaseGroupLease(ctx context.Context, groupID, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.releaseCalls = append(f.releaseCalls, groupID)
	if ctx.Err() != nil {
		f.releaseOnDone = true
	}
	return nil
}

func (f *fakeNodeStore) ListGroupInputs(_ context.Context, groupID string) ([]model.GroupInput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.inputErr[groupID]; err != nil {
		return nil, err
	}
	return []model.GroupInput{{
		InputID:   groupID + ":trade",
		GroupID:   groupID,
		StreamKey: "trade",
	}}, nil
}

func (f *fakeNodeStore) ReportStreamRuntimeStatus(context.Context, model.StreamRuntimeStatus) error {
	return nil
}

func TestNodeReconcileContinuesAfterLeaseContention(t *testing.T) {
	store := &fakeNodeStore{
		runnable: []model.MarketGroup{
			{GroupID: "a-owned-elsewhere", Weight: 1},
			{GroupID: "b-available", Weight: 1},
		},
		blocked:  map[string]bool{"a-owned-elsewhere": true},
		inputErr: map[string]error{},
	}
	node := NewNode(store, NodeConfig{
		NodeID:        "node-a",
		MaxGroups:     1,
		MaxWeight:     10,
		LeaseTTL:      time.Minute,
		RenewInterval: 30 * time.Second,
	}, discardLogger())
	t.Cleanup(node.shutdown)

	node.reconcileOnce(context.Background())

	if got := node.runningGroupIDs(); !contains(got, "b-available") || len(got) != 1 {
		t.Fatalf("running groups = %v, want only b-available", got)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.acquireCalls) != 2 {
		t.Fatalf("acquire calls = %v, want both candidates attempted", store.acquireCalls)
	}
}

func TestNodeReconcileRespectsMaxWeight(t *testing.T) {
	store := &fakeNodeStore{
		runnable: []model.MarketGroup{
			{GroupID: "a-too-heavy", Weight: 3},
			{GroupID: "b-fits", Weight: 2},
			{GroupID: "c-no-room", Weight: 1},
		},
		blocked:  map[string]bool{},
		inputErr: map[string]error{},
	}
	node := NewNode(store, NodeConfig{
		NodeID:        "node-a",
		MaxGroups:     3,
		MaxWeight:     2,
		LeaseTTL:      time.Minute,
		RenewInterval: 30 * time.Second,
	}, discardLogger())
	t.Cleanup(node.shutdown)

	node.reconcileOnce(context.Background())

	got := node.runningGroupIDs()
	if len(got) != 1 || got[0] != "b-fits" {
		t.Fatalf("running groups = %v, want only b-fits", got)
	}
	if capacity := node.capacity(); capacity.CurrentWeight != 2 {
		t.Fatalf("current weight = %d, want 2", capacity.CurrentWeight)
	}
}

func TestNodeReleasesAcquiredLeaseWithFreshContext(t *testing.T) {
	store := &fakeNodeStore{
		blocked:  map[string]bool{},
		inputErr: map[string]error{"group-a": errors.New("read failed")},
	}
	node := NewNode(store, NodeConfig{
		NodeID:        "node-a",
		LeaseTTL:      time.Minute,
		RenewInterval: 30 * time.Second,
	}, discardLogger())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if node.tryStartWorker(ctx, model.MarketGroup{GroupID: "group-a", Weight: 1}) {
		t.Fatal("worker should not start when inputs cannot be loaded")
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.releaseCalls) != 1 || store.releaseCalls[0] != "group-a" {
		t.Fatalf("release calls = %v, want group-a", store.releaseCalls)
	}
	if store.releaseOnDone {
		t.Fatal("lease release reused the canceled reconcile context")
	}
}

func TestNodeRunRequiresIdentityAndStore(t *testing.T) {
	if err := NewNode(&fakeNodeStore{}, NodeConfig{}, discardLogger()).Run(context.Background()); err == nil {
		t.Fatal("expected empty node id to be rejected")
	}
	if err := NewNode(nil, NodeConfig{NodeID: "node-a"}, discardLogger()).Run(context.Background()); err == nil {
		t.Fatal("expected nil metadata store to be rejected")
	}
}

func TestNodeRunValidatesConsumptionDependencies(t *testing.T) {
	factory := &recordingFactory{}

	node := NewNode(&fakeNodeStore{}, NodeConfig{NodeID: "node-a"}, discardLogger())
	node.EnableConsumption(nil, factory, []string{"broker:9092"}, time.Second, time.Second)
	if err := node.Run(context.Background()); err == nil {
		t.Fatal("expected storage/factory mismatch to be rejected")
	}

	node = NewNode(&fakeNodeStore{}, NodeConfig{NodeID: "node-a"}, discardLogger())
	node.EnableConsumption(
		storage.NewPostgresStorage(nil), factory, nil, time.Second, time.Second,
	)
	if err := node.Run(context.Background()); err == nil {
		t.Fatal("expected missing brokers to be rejected")
	}
}

func TestNodeDoesNotReleaseLeaseWhenWorkerStopTimesOut(t *testing.T) {
	store := &fakeNodeStore{blocked: map[string]bool{}, inputErr: map[string]error{}}
	node := NewNode(store, NodeConfig{
		NodeID:          "node-a",
		ShutdownTimeout: time.Millisecond,
	}, discardLogger())
	worker := newWorker(model.MarketGroup{GroupID: "group-a"}, nil, store,
		"node-a", time.Minute, 30*time.Second, discardLogger())
	worker.state = WorkerRunning
	worker.cancel = func() {}
	node.workers["group-a"] = worker

	node.stopWorker("group-a")

	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.releaseCalls) != 0 {
		t.Fatalf("release calls = %v, want none while worker is still active", store.releaseCalls)
	}
	close(worker.done)
}
