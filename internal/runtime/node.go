package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"MarketDataBackend/internal/kafka"
	"MarketDataBackend/internal/model"
	"MarketDataBackend/internal/storage"
)

// defaultShutdownTimeout bounds how long the node waits for a single worker to
// stop and for its lease to be released when no explicit timeout is configured.
const defaultShutdownTimeout = 10 * time.Second

// NodeConfig configures a runtime Node's identity, capacity and loop cadence.
type NodeConfig struct {
	NodeID            string
	Hostname          string
	PodName           string
	MaxGroups         int
	MaxWeight         int
	LeaseTTL          time.Duration
	RenewInterval     time.Duration
	ReconcileInterval time.Duration
	HeartbeatInterval time.Duration
	ShutdownTimeout   time.Duration
}

// withDefaults fills any unset cadence/capacity fields with safe values so a
// caller only has to provide NodeID and LeaseTTL.
func (c NodeConfig) withDefaults() NodeConfig {
	if c.MaxGroups <= 0 {
		c.MaxGroups = 10
	}
	if c.MaxWeight <= 0 {
		c.MaxWeight = 100
	}
	if c.LeaseTTL <= 0 {
		c.LeaseTTL = 30 * time.Second
	}
	if c.RenewInterval <= 0 || c.RenewInterval >= c.LeaseTTL {
		c.RenewInterval = c.LeaseTTL / 3
	}
	if c.ReconcileInterval <= 0 {
		c.ReconcileInterval = 5 * time.Second
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 10 * time.Second
	}
	if c.ShutdownTimeout <= 0 {
		c.ShutdownTimeout = defaultShutdownTimeout
	}
	return c
}

// nodeStore is the metadata surface used by the M5 scheduler. Keeping the
// dependency narrow makes the scheduling loop easy to test without PostgreSQL.
type nodeStore interface {
	RegisterRuntimeNode(ctx context.Context, node model.RuntimeNode) error
	HeartbeatRuntimeNode(ctx context.Context, nodeID string, capacity model.RuntimeCapacity) error
	ListRunnableGroups(ctx context.Context) ([]model.MarketGroup, error)
	TryAcquireGroupLease(ctx context.Context, groupID, nodeID string, ttl time.Duration) (bool, error)
	RenewGroupLease(ctx context.Context, groupID, nodeID string, ttl time.Duration) (bool, error)
	ReleaseGroupLease(ctx context.Context, groupID, nodeID string) error
	ListGroupInputs(ctx context.Context, groupID string) ([]model.GroupInput, error)
	ReportStreamRuntimeStatus(ctx context.Context, status model.StreamRuntimeStatus) error
}

// Node is a single md-stream-runtime process. It registers itself, heartbeats,
// and runs a reconcile loop that acquires leases for runnable market groups and
// runs one Worker per owned group. Lease renewal (per worker) plus lease expiry
// (in the store) guarantee that a group is processed by at most one node.
type Node struct {
	store  nodeStore
	cfg    NodeConfig
	logger *slog.Logger

	storage             storage.MarketDataStorage
	consumers           kafka.Factory
	brokers             []string
	statusReportEvery   time.Duration
	inputReconcileEvery time.Duration
	consumptionConfig   ConsumptionConfig

	mu      sync.Mutex
	workers map[string]*Worker
}

// NewNode builds a Node bound to a metadata store. Unset config fields are
// defaulted; Run validates the required store and node identity.
func NewNode(store nodeStore, cfg NodeConfig, logger *slog.Logger) *Node {
	if logger == nil {
		logger = slog.Default()
	}
	return &Node{
		store:   store,
		cfg:     cfg.withDefaults(),
		logger:  logger,
		workers: make(map[string]*Worker),
	}
}

// EnableConsumption wires the M6/M7 market-data pipelines. It must be called
// before Run.
// A node without these dependencies retains the M5 lease-only behaviour used
// by scheduler tests.
func (n *Node) EnableConsumption(st storage.MarketDataStorage, consumers kafka.Factory,
	brokers []string, statusReportEvery, inputReconcileEvery time.Duration) {
	n.EnableConsumptionWithConfig(st, consumers, brokers, ConsumptionConfig{
		StatusReportEvery:   statusReportEvery,
		InputReconcileEvery: inputReconcileEvery,
	})
}

// EnableConsumptionWithConfig wires market-data storage, Kafka and M7.5 batch
// settings. It must be called before Run.
func (n *Node) EnableConsumptionWithConfig(
	st storage.MarketDataStorage, consumers kafka.Factory,
	brokers []string, cfg ConsumptionConfig,
) {
	n.storage = st
	n.consumers = consumers
	n.brokers = append([]string(nil), brokers...)
	n.statusReportEvery = cfg.StatusReportEvery
	n.inputReconcileEvery = cfg.InputReconcileEvery
	n.consumptionConfig = cfg
}

// Run registers the node, then drives the heartbeat and reconcile loops until
// ctx is canceled. On return every owned worker is stopped and its lease is
// released so another node can take over promptly.
func (n *Node) Run(ctx context.Context) error {
	if n.store == nil {
		return fmt.Errorf("runtime: metadata store is required")
	}
	if n.cfg.NodeID == "" {
		return fmt.Errorf("runtime: node id is required")
	}
	if (n.storage == nil) != (n.consumers == nil) {
		return fmt.Errorf("runtime: storage and kafka consumer factory must be configured together")
	}
	if n.storage != nil && len(n.brokers) == 0 {
		return fmt.Errorf("runtime: kafka brokers are required when consumption is enabled")
	}
	if err := n.register(ctx); err != nil {
		return err
	}
	n.logger.Info("runtime node registered",
		"node_id", n.cfg.NodeID, "max_groups", n.cfg.MaxGroups)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		n.heartbeatLoop(ctx)
	}()

	n.reconcileLoop(ctx) // blocks until ctx is canceled
	wg.Wait()

	n.shutdown()
	return nil
}

func (n *Node) register(ctx context.Context) error {
	node := model.RuntimeNode{
		NodeID:        n.cfg.NodeID,
		Hostname:      n.cfg.Hostname,
		PodName:       n.cfg.PodName,
		Status:        model.NodeStatusAlive,
		MaxGroups:     n.cfg.MaxGroups,
		CurrentGroups: 0,
		MaxWeight:     n.cfg.MaxWeight,
		CurrentWeight: 0,
	}
	if err := n.store.RegisterRuntimeNode(ctx, node); err != nil {
		return fmt.Errorf("runtime: register node: %w", err)
	}
	return nil
}

func (n *Node) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(n.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := n.store.HeartbeatRuntimeNode(ctx, n.cfg.NodeID, n.capacity()); err != nil {
				n.logger.Warn("heartbeat failed", "node_id", n.cfg.NodeID, "err", err)
			}
		}
	}
}

func (n *Node) reconcileLoop(ctx context.Context) {
	ticker := time.NewTicker(n.cfg.ReconcileInterval)
	defer ticker.Stop()
	// Reconcile once immediately so the node does not idle for a full interval
	// before picking up work.
	n.reconcileOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.reconcileOnce(ctx)
		}
	}
}

// reconcileOnce performs a single scheduling pass: drop self-exited workers,
// stop groups that are no longer runnable, and acquire/start newly runnable
// groups up to capacity.
func (n *Node) reconcileOnce(ctx context.Context) {
	n.pruneDeadWorkers()

	groups, err := n.store.ListRunnableGroups(ctx)
	if err != nil {
		n.logger.Warn("list runnable groups failed", "err", err)
		return
	}
	desired := make([]string, 0, len(groups))
	groupByID := make(map[string]model.MarketGroup, len(groups))
	for _, g := range groups {
		desired = append(desired, g.GroupID)
		groupByID[g.GroupID] = g
	}

	plan := planReconcile(desired, n.runningGroupIDs(), n.cfg.MaxGroups)

	for _, gid := range plan.toStop {
		n.stopWorker(gid)
	}
	started := 0
	currentWeight := n.capacity().CurrentWeight
	for _, gid := range plan.toStart {
		if started >= plan.startSlots {
			break
		}
		g := groupByID[gid]
		weight := groupWeight(g)
		if n.cfg.MaxWeight > 0 && currentWeight+weight > n.cfg.MaxWeight {
			continue
		}
		if n.tryStartWorker(ctx, g) {
			started++
			currentWeight += weight
		}
	}
}

// tryStartWorker acquires the lease for a group and, on success, starts a worker
// bound to the node lifetime context. If the lease is held elsewhere it is a
// no-op; if inputs cannot be loaded the freshly acquired lease is released.
func (n *Node) tryStartWorker(ctx context.Context, g model.MarketGroup) bool {
	ok, err := n.store.TryAcquireGroupLease(ctx, g.GroupID, n.cfg.NodeID, n.cfg.LeaseTTL)
	if err != nil {
		n.logger.Warn("acquire lease failed", "group_id", g.GroupID, "err", err)
		return false
	}
	if !ok {
		// Another node owns a still-valid lease; try again next round.
		return false
	}

	inputs, err := n.store.ListGroupInputs(ctx, g.GroupID)
	if err != nil {
		n.logger.Warn("list inputs failed; releasing lease", "group_id", g.GroupID, "err", err)
		n.releaseLease(g.GroupID, "release lease after input load failure")
		return false
	}

	w := newWorker(g, inputs, n.store, n.cfg.NodeID, n.cfg.LeaseTTL, n.cfg.RenewInterval, n.logger)
	if n.storage != nil {
		w.enableConsumptionWithConfig(
			n.storage, n.consumers, n.brokers, n.consumptionConfig,
		)
	}
	n.mu.Lock()
	n.workers[g.GroupID] = w
	n.mu.Unlock()
	w.start(ctx)
	n.logger.Info("worker started", "group_id", g.GroupID, "node_id", n.cfg.NodeID)
	return true
}

// stopWorker gracefully stops a worker and releases its lease. It uses a fresh
// context so it still works while the node is shutting down (node ctx canceled).
func (n *Node) stopWorker(gid string) {
	n.mu.Lock()
	w := n.workers[gid]
	delete(n.workers, gid)
	n.mu.Unlock()
	if w == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.ShutdownTimeout)
	stopped := w.Stop(ctx)
	cancel()
	if !stopped {
		// Releasing before the worker is fully stopped could allow another node
		// to start the group while this worker is still draining. Leave the
		// lease in place and let its TTL provide the safety boundary.
		n.logger.Warn("worker stop timed out; lease left to expire",
			"group_id", gid, "node_id", n.cfg.NodeID)
		return
	}
	n.releaseLease(gid, "release lease")
	n.logger.Info("worker stopped and lease released", "group_id", gid, "node_id", n.cfg.NodeID)
}

// pruneDeadWorkers drops workers that exited on their own (lease lost or renew
// error) and releases their lease defensively. ReleaseGroupLease is node-id
// checked, so it is a safe no-op when another node already owns the group.
func (n *Node) pruneDeadWorkers() {
	n.mu.Lock()
	var dead []string
	for gid, w := range n.workers {
		if w.State().terminal() {
			dead = append(dead, gid)
		}
	}
	for _, gid := range dead {
		delete(n.workers, gid)
	}
	n.mu.Unlock()

	for _, gid := range dead {
		ctx, cancel := context.WithTimeout(context.Background(), n.cfg.ShutdownTimeout)
		if err := n.store.ReleaseGroupLease(ctx, gid, n.cfg.NodeID); err != nil {
			n.logger.Warn("release lease for dead worker failed", "group_id", gid, "err", err)
		}
		cancel()
		n.logger.Info("worker pruned", "group_id", gid, "node_id", n.cfg.NodeID)
	}
}

// shutdown stops every remaining worker and releases its lease.
func (n *Node) shutdown() {
	for _, gid := range n.runningGroupIDs() {
		n.stopWorker(gid)
	}
	n.logger.Info("runtime node stopped", "node_id", n.cfg.NodeID)
}

func (n *Node) runningGroupIDs() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	ids := make([]string, 0, len(n.workers))
	for gid := range n.workers {
		ids = append(ids, gid)
	}
	return ids
}

func (n *Node) capacity() model.RuntimeCapacity {
	n.mu.Lock()
	defer n.mu.Unlock()
	weight := 0
	for _, w := range n.workers {
		weight += groupWeight(w.group)
	}
	return model.RuntimeCapacity{
		CurrentGroups: len(n.workers),
		CurrentWeight: weight,
	}
}

func (n *Node) releaseLease(groupID, action string) {
	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.ShutdownTimeout)
	defer cancel()
	if err := n.store.ReleaseGroupLease(ctx, groupID, n.cfg.NodeID); err != nil {
		n.logger.Warn(action+" failed", "group_id", groupID, "err", err)
	}
}

func groupWeight(g model.MarketGroup) int {
	if g.Weight <= 0 {
		return 1
	}
	return g.Weight
}
