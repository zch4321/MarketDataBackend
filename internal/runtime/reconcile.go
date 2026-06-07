// Package runtime implements the md-stream-runtime scheduling loop: a node
// registers itself, heartbeats, acquires leases for runnable market groups and
// runs one Worker per owned group. In M5 workers do not consume Kafka yet; they
// only hold the lease (renewing it) and report per-input runtime status.
package runtime

import "sort"

// reconcilePlan is the output of planReconcile: the market groups a single node
// should begin owning and the groups it should stop owning this round.
type reconcilePlan struct {
	toStart    []string
	toStop     []string
	startSlots int
}

// planReconcile decides, for one runtime node, which market groups to start and
// which to stop.
//
//   - desired is the set of currently runnable group ids (desired_status =
//     running) reported by the control plane.
//   - running is the set of group ids the node already owns a worker for.
//   - maxGroups caps how many groups the node may own at once; a value <= 0
//     means unlimited.
//
// A group the node runs but that is no longer desired is stopped. A desired
// group the node does not run is a start candidate. All candidates are returned
// in deterministic order, together with the number of free worker slots. The
// caller consumes a slot only after a lease acquisition succeeds. This matters
// in a multi-node deployment: a node that loses the lease race for the first
// candidate must keep trying later candidates instead of leaving them idle.
//
// The function is pure so the start/stop decision can be unit tested without
// any I/O.
func planReconcile(desired, running []string, maxGroups int) reconcilePlan {
	desiredSet := make(map[string]struct{}, len(desired))
	for _, id := range desired {
		desiredSet[id] = struct{}{}
	}
	runningSet := make(map[string]struct{}, len(running))
	for _, id := range running {
		runningSet[id] = struct{}{}
	}

	var plan reconcilePlan

	// Stop every group we run that is no longer desired.
	for _, id := range running {
		if _, ok := desiredSet[id]; !ok {
			plan.toStop = append(plan.toStop, id)
		}
	}
	// kept = groups we run that remain desired (they keep their slot).
	kept := len(runningSet) - len(plan.toStop)

	// Start candidates = desired groups we are not already running.
	var candidates []string
	for id := range desiredSet {
		if _, ok := runningSet[id]; !ok {
			candidates = append(candidates, id)
		}
	}

	sort.Strings(candidates)
	sort.Strings(plan.toStop)

	available := len(candidates)
	if maxGroups > 0 {
		available = maxGroups - kept
		if available < 0 {
			available = 0
		}
	}
	if available > len(candidates) {
		available = len(candidates)
	}
	plan.toStart = candidates
	plan.startSlots = available
	return plan
}
