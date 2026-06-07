package runtime

import (
	"context"
	"testing"
	"time"

	"MarketDataBackend/internal/model"
)

// TestNodeAcquiresAndReleasesGroup verifies a single runtime registers, acquires
// the lease for a runnable group, reports it running, and releases the lease on
// graceful shutdown.
func TestNodeAcquiresAndReleasesGroup(t *testing.T) {
	store, pool := newStore(t)
	ctx := context.Background()
	gid := createRunnableGroup(t, store, "BTCUSDT")

	node, stop := runNode(t, store, fastNodeConfig("node-a"))

	eventually(t, 3*time.Second, func() bool {
		l, _ := store.GetGroupLease(ctx, gid)
		return l != nil && l.NodeID == "node-a"
	}, "node-a should acquire the lease")

	eventually(t, 2*time.Second, func() bool {
		return contains(node.runningGroupIDs(), gid)
	}, "node-a should run the group")

	eventually(t, 2*time.Second, func() bool {
		return groupActualStatus(t, store, gid) == model.ActualStatusRunning
	}, "stream status should be running")

	if !nodeRegistered(t, pool, "node-a") {
		t.Error("node-a should be registered in runtime_nodes")
	}

	stop()

	l, err := store.GetGroupLease(ctx, gid)
	if err != nil {
		t.Fatalf("GetGroupLease: %v", err)
	}
	if l != nil {
		t.Errorf("lease should be released after shutdown, got owner %q", l.NodeID)
	}
}

// TestSingleOwnerAcrossTwoNodes verifies that when two runtimes compete for the
// same group, exactly one owns and runs it.
func TestSingleOwnerAcrossTwoNodes(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	gid := createRunnableGroup(t, store, "BTCUSDT")

	nodeA, _ := runNode(t, store, fastNodeConfig("node-a"))
	nodeB, _ := runNode(t, store, fastNodeConfig("node-b"))

	eventually(t, 4*time.Second, func() bool {
		l, _ := store.GetGroupLease(ctx, gid)
		running := len(nodeA.runningGroupIDs()) + len(nodeB.runningGroupIDs())
		return l != nil && running == 1
	}, "exactly one node should own and run the group")

	l, _ := store.GetGroupLease(ctx, gid)
	if l == nil {
		t.Fatal("expected a lease owner")
	}
	ownerRunsIt := (l.NodeID == "node-a" && contains(nodeA.runningGroupIDs(), gid)) ||
		(l.NodeID == "node-b" && contains(nodeB.runningGroupIDs(), gid))
	if !ownerRunsIt {
		t.Errorf("lease owner %q is not the node running the group", l.NodeID)
	}
}

// TestTwoNodesFillDistinctCapacity is the distributed scheduling regression
// test: when both nodes initially contend for the first sorted group, the loser
// must continue to the next candidate instead of leaving it unowned.
func TestTwoNodesFillDistinctCapacity(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	first := createRunnableGroup(t, store, "BTCUSDT")
	second := createRunnableGroup(t, store, "ETHUSDT")

	cfgA := fastNodeConfig("node-a")
	cfgA.MaxGroups = 1
	cfgB := fastNodeConfig("node-b")
	cfgB.MaxGroups = 1
	nodeA, _ := runNode(t, store, cfgA)
	nodeB, _ := runNode(t, store, cfgB)

	eventually(t, 5*time.Second, func() bool {
		l1, _ := store.GetGroupLease(ctx, first)
		l2, _ := store.GetGroupLease(ctx, second)
		return l1 != nil && l2 != nil && l1.NodeID != l2.NodeID &&
			len(nodeA.runningGroupIDs()) == 1 && len(nodeB.runningGroupIDs()) == 1
	}, "two capacity-one nodes should own distinct groups")
}

// TestOwnerRenewsAndHeartbeats verifies the owning node keeps its lease fresh
// (version + expiry advance) and updates its heartbeat.
func TestOwnerRenewsAndHeartbeats(t *testing.T) {
	store, pool := newStore(t)
	ctx := context.Background()
	gid := createRunnableGroup(t, store, "BTCUSDT")

	runNode(t, store, fastNodeConfig("node-a"))

	eventually(t, 3*time.Second, func() bool {
		l, _ := store.GetGroupLease(ctx, gid)
		return l != nil && l.NodeID == "node-a"
	}, "node-a should acquire the lease")

	l1, _ := store.GetGroupLease(ctx, gid)
	hb1 := nodeHeartbeat(t, pool, "node-a")

	eventually(t, 3*time.Second, func() bool {
		l2, _ := store.GetGroupLease(ctx, gid)
		return l2 != nil && l2.Version > l1.Version && l2.LeaseExpiresAt.After(l1.LeaseExpiresAt)
	}, "lease version and expiry should advance via renew")

	eventually(t, 3*time.Second, func() bool {
		return nodeHeartbeat(t, pool, "node-a").After(hb1)
	}, "node heartbeat should advance")
}

// TestExpiredLeaseTakeover verifies that a stalled owner (which stops renewing)
// loses its lease after expiry and another runtime takes over.
func TestExpiredLeaseTakeover(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	gid := createRunnableGroup(t, store, "BTCUSDT")

	// Simulate a dead owner: it holds a short lease and never renews it.
	ok, err := store.TryAcquireGroupLease(ctx, gid, "node-dead", 800*time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("seed dead-owner lease: ok=%v err=%v", ok, err)
	}

	nodeB, _ := runNode(t, store, fastNodeConfig("node-b"))

	// While the seeded lease is valid node-b must not own it.
	eventually(t, 300*time.Millisecond, func() bool {
		l, _ := store.GetGroupLease(ctx, gid)
		return l != nil && l.NodeID == "node-dead"
	}, "seeded lease should initially block node-b")

	// After expiry node-b takes over and runs the group.
	eventually(t, 5*time.Second, func() bool {
		l, _ := store.GetGroupLease(ctx, gid)
		return l != nil && l.NodeID == "node-b" && contains(nodeB.runningGroupIDs(), gid)
	}, "node-b should take over the expired lease")
}

// TestPausedGroupStopsWorker verifies that pausing a group makes the owning node
// stop the worker and release the lease.
func TestPausedGroupStopsWorker(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	gid := createRunnableGroup(t, store, "BTCUSDT")

	node, _ := runNode(t, store, fastNodeConfig("node-a"))

	eventually(t, 3*time.Second, func() bool {
		return contains(node.runningGroupIDs(), gid)
	}, "node-a should run the group")

	if err := store.UpdateGroupDesiredStatus(ctx, gid, model.DesiredStatusPaused); err != nil {
		t.Fatalf("pause group: %v", err)
	}

	eventually(t, 3*time.Second, func() bool {
		l, _ := store.GetGroupLease(ctx, gid)
		return !contains(node.runningGroupIDs(), gid) && l == nil
	}, "paused group should stop the worker and release the lease")

	eventually(t, 2*time.Second, func() bool {
		return groupActualStatus(t, store, gid) == model.ActualStatusStopped
	}, "stream status should be stopped after pause")

	if err := store.UpdateGroupDesiredStatus(ctx, gid, model.DesiredStatusRunning); err != nil {
		t.Fatalf("resume group: %v", err)
	}
	eventually(t, 3*time.Second, func() bool {
		l, _ := store.GetGroupLease(ctx, gid)
		return contains(node.runningGroupIDs(), gid) && l != nil && l.NodeID == "node-a"
	}, "resumed group should be reacquired")
	eventually(t, 2*time.Second, func() bool {
		return groupActualStatus(t, store, gid) == model.ActualStatusRunning
	}, "stream status should return to running after resume")
}
