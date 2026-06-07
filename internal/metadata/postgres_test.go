package metadata

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"MarketDataBackend/internal/model"
)

func sampleGroup(symbol string) model.MarketGroup {
	return model.MarketGroup{
		Exchange:   "binance",
		MarketType: "spot",
		Symbol:     symbol,
		BaseAsset:  "BTC",
		QuoteAsset: "USDT",
		// stream_kind / interval are intentionally omitted so CreateGroup must
		// derive them from stream_key.
		Inputs: []model.GroupInput{
			{StreamKey: "trade", KafkaTopic: "md.trade", KafkaGroupID: "rt", Enabled: true},
			{StreamKey: "kline_1m", KafkaTopic: "md.kline", KafkaGroupID: "rt", Enabled: true},
		},
	}
}

func TestCreateGroupAndListInputs(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()

	g := sampleGroup("BTCUSDT")
	if err := store.CreateGroup(ctx, g); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}

	gid := model.NewGroupID("binance", "spot", "BTCUSDT")
	inputs, err := store.ListGroupInputs(ctx, gid)
	if err != nil {
		t.Fatalf("ListGroupInputs: %v", err)
	}
	if len(inputs) != 2 {
		t.Fatalf("got %d inputs, want 2", len(inputs))
	}
	// Ordered by stream_key: kline_1m before trade.
	if inputs[0].StreamKey != "kline_1m" || inputs[0].StreamKind != model.StreamKindKline || inputs[0].Interval != "1m" {
		t.Errorf("kline input derived wrong: %+v", inputs[0])
	}
	if inputs[1].StreamKey != "trade" || inputs[1].StreamKind != model.StreamKindTrade || inputs[1].Interval != "" {
		t.Errorf("trade input derived wrong: %+v", inputs[1])
	}
	if inputs[0].InputID != model.NewInputID(gid, "kline_1m") {
		t.Errorf("input_id = %q, want derived id", inputs[0].InputID)
	}
	if inputs[1].DesiredStatus != model.DesiredStatusRunning || inputs[1].KafkaCluster != "default" || inputs[1].SchemaVersion != 1 {
		t.Errorf("input defaults wrong: %+v", inputs[1])
	}
}

func TestCreateGroupDuplicateConflict(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()

	if err := store.CreateGroup(ctx, sampleGroup("ETHUSDT")); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	// Same exchange/market_type/symbol -> ErrConflict.
	err := store.CreateGroup(ctx, sampleGroup("ETHUSDT"))
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate CreateGroup err = %v, want ErrConflict", err)
	}
}

func TestUpdateGroupDesiredStatus(t *testing.T) {
	store, pool := newStore(t)
	ctx := context.Background()

	if err := store.CreateGroup(ctx, sampleGroup("BTCUSDT")); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	gid := model.NewGroupID("binance", "spot", "BTCUSDT")

	before := readUpdatedAt(t, pool, gid)
	time.Sleep(2 * time.Millisecond) // ensure a later transaction timestamp

	if err := store.UpdateGroupDesiredStatus(ctx, gid, model.DesiredStatusPaused); err != nil {
		t.Fatalf("UpdateGroupDesiredStatus: %v", err)
	}

	var status string
	if err := pool.QueryRow(ctx, "SELECT desired_status FROM market_groups WHERE group_id = $1", gid).Scan(&status); err != nil {
		t.Fatalf("read status: %v", err)
	}
	if status != model.DesiredStatusPaused {
		t.Errorf("desired_status = %q, want paused", status)
	}
	if after := readUpdatedAt(t, pool, gid); !after.After(before) {
		t.Errorf("updated_at not advanced: before=%v after=%v", before, after)
	}

	// Unknown group -> ErrNotFound.
	if err := store.UpdateGroupDesiredStatus(ctx, "nope", model.DesiredStatusRunning); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown group err = %v, want ErrNotFound", err)
	}
	// Invalid status -> validation error (and the row stays paused).
	if err := store.UpdateGroupDesiredStatus(ctx, gid, "bogus"); err == nil {
		t.Error("invalid status: want error, got nil")
	}
}

func TestListRunnableGroups(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()

	if err := store.CreateGroup(ctx, sampleGroup("BTCUSDT")); err != nil {
		t.Fatalf("CreateGroup BTC: %v", err)
	}
	if err := store.CreateGroup(ctx, sampleGroup("ETHUSDT")); err != nil {
		t.Fatalf("CreateGroup ETH: %v", err)
	}
	ethID := model.NewGroupID("binance", "spot", "ETHUSDT")
	if err := store.UpdateGroupDesiredStatus(ctx, ethID, model.DesiredStatusPaused); err != nil {
		t.Fatalf("pause ETH: %v", err)
	}

	groups, err := store.ListRunnableGroups(ctx)
	if err != nil {
		t.Fatalf("ListRunnableGroups: %v", err)
	}
	if len(groups) != 1 {
		t.Fatalf("got %d runnable groups, want 1", len(groups))
	}
	if groups[0].Symbol != "BTCUSDT" || groups[0].DesiredStatus != model.DesiredStatusRunning {
		t.Errorf("unexpected runnable group: %+v", groups[0])
	}
	if groups[0].BaseAsset != "BTC" || groups[0].QuoteAsset != "USDT" {
		t.Errorf("nullable assets not round-tripped: %+v", groups[0])
	}
}

func TestRuntimeNodeRegisterAndHeartbeat(t *testing.T) {
	store, pool := newStore(t)
	ctx := context.Background()

	node := model.RuntimeNode{
		NodeID:    "node-1",
		Hostname:  "host-a",
		MaxGroups: 10,
		MaxWeight: 100,
	}
	if err := store.RegisterRuntimeNode(ctx, node); err != nil {
		t.Fatalf("RegisterRuntimeNode: %v", err)
	}

	// Re-register with new values: upsert updates fields, status defaults alive.
	node.Hostname = "host-b"
	node.MaxGroups = 20
	if err := store.RegisterRuntimeNode(ctx, node); err != nil {
		t.Fatalf("re-RegisterRuntimeNode: %v", err)
	}
	var (
		hostname  string
		maxGroups int
		status    string
	)
	if err := pool.QueryRow(ctx,
		"SELECT hostname, max_groups, status FROM runtime_nodes WHERE node_id = $1", "node-1").
		Scan(&hostname, &maxGroups, &status); err != nil {
		t.Fatalf("read node: %v", err)
	}
	if hostname != "host-b" || maxGroups != 20 || status != model.NodeStatusAlive {
		t.Errorf("upsert mismatch: host=%s max=%d status=%s", hostname, maxGroups, status)
	}

	// Heartbeat updates capacity and advances last_heartbeat_at.
	before := readNodeHeartbeat(t, pool, "node-1")
	time.Sleep(2 * time.Millisecond)
	if err := store.HeartbeatRuntimeNode(ctx, "node-1", model.RuntimeCapacity{CurrentGroups: 3, CurrentWeight: 30}); err != nil {
		t.Fatalf("HeartbeatRuntimeNode: %v", err)
	}
	var curGroups, curWeight int
	if err := pool.QueryRow(ctx,
		"SELECT current_groups, current_weight FROM runtime_nodes WHERE node_id = $1", "node-1").
		Scan(&curGroups, &curWeight); err != nil {
		t.Fatalf("read capacity: %v", err)
	}
	if curGroups != 3 || curWeight != 30 {
		t.Errorf("capacity = (%d,%d), want (3,30)", curGroups, curWeight)
	}
	if after := readNodeHeartbeat(t, pool, "node-1"); !after.After(before) {
		t.Errorf("heartbeat not advanced: before=%v after=%v", before, after)
	}

	// Heartbeat for an unknown node -> ErrNotFound.
	if err := store.HeartbeatRuntimeNode(ctx, "ghost", model.RuntimeCapacity{}); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown node heartbeat err = %v, want ErrNotFound", err)
	}
}

func TestLeaseLifecycle(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()

	if err := store.CreateGroup(ctx, sampleGroup("BTCUSDT")); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	gid := model.NewGroupID("binance", "spot", "BTCUSDT")
	const ttl = 30 * time.Second

	// node-a acquires.
	ok, err := store.TryAcquireGroupLease(ctx, gid, "node-a", ttl)
	if err != nil || !ok {
		t.Fatalf("node-a acquire: ok=%v err=%v", ok, err)
	}
	// node-b fails while the lease is valid.
	if ok, err := store.TryAcquireGroupLease(ctx, gid, "node-b", ttl); err != nil || ok {
		t.Fatalf("node-b acquire: ok=%v err=%v, want false", ok, err)
	}
	// owner renews.
	if ok, err := store.RenewGroupLease(ctx, gid, "node-a", ttl); err != nil || !ok {
		t.Fatalf("node-a renew: ok=%v err=%v", ok, err)
	}
	// non-owner renew fails.
	if ok, err := store.RenewGroupLease(ctx, gid, "node-b", ttl); err != nil || ok {
		t.Fatalf("node-b renew: ok=%v err=%v, want false", ok, err)
	}
	// non-owner release is a no-op (lease still held by node-a).
	if err := store.ReleaseGroupLease(ctx, gid, "node-b"); err != nil {
		t.Fatalf("node-b release: %v", err)
	}
	if ok, err := store.TryAcquireGroupLease(ctx, gid, "node-b", ttl); err != nil || ok {
		t.Fatalf("node-b acquire after bogus release: ok=%v err=%v, want false", ok, err)
	}
	// owner release frees the lease.
	if err := store.ReleaseGroupLease(ctx, gid, "node-a"); err != nil {
		t.Fatalf("node-a release: %v", err)
	}
	if ok, err := store.TryAcquireGroupLease(ctx, gid, "node-b", ttl); err != nil || !ok {
		t.Fatalf("node-b acquire after release: ok=%v err=%v, want true", ok, err)
	}
}

func TestLeaseExpiryTakeover(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()

	if err := store.CreateGroup(ctx, sampleGroup("BTCUSDT")); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	gid := model.NewGroupID("binance", "spot", "BTCUSDT")

	// node-a holds a very short lease.
	if ok, err := store.TryAcquireGroupLease(ctx, gid, "node-a", 100*time.Millisecond); err != nil || !ok {
		t.Fatalf("node-a acquire: ok=%v err=%v", ok, err)
	}
	// node-b cannot take over while it is valid.
	if ok, _ := store.TryAcquireGroupLease(ctx, gid, "node-b", time.Second); ok {
		t.Fatal("node-b acquired a still-valid lease")
	}
	// After expiry node-b takes over.
	time.Sleep(200 * time.Millisecond)
	if ok, err := store.TryAcquireGroupLease(ctx, gid, "node-b", 30*time.Second); err != nil || !ok {
		t.Fatalf("node-b takeover after expiry: ok=%v err=%v", ok, err)
	}
	// node-a can no longer renew the lost lease.
	if ok, err := store.RenewGroupLease(ctx, gid, "node-a", 30*time.Second); err != nil || ok {
		t.Fatalf("node-a renew after takeover: ok=%v err=%v, want false", ok, err)
	}
}

func TestLeaseConcurrentAcquire(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()

	if err := store.CreateGroup(ctx, sampleGroup("BTCUSDT")); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	gid := model.NewGroupID("binance", "spot", "BTCUSDT")

	const nodes = 12
	var (
		wg      sync.WaitGroup
		winners atomic.Int32
		start   = make(chan struct{})
	)
	for i := 0; i < nodes; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			<-start
			ok, err := store.TryAcquireGroupLease(ctx, gid, nodeName(n), 30*time.Second)
			if err != nil {
				t.Errorf("node %d acquire: %v", n, err)
				return
			}
			if ok {
				winners.Add(1)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if got := winners.Load(); got != 1 {
		t.Fatalf("concurrent acquire winners = %d, want exactly 1", got)
	}
}

func TestReportStreamRuntimeStatus(t *testing.T) {
	store, pool := newStore(t)
	ctx := context.Background()

	if err := store.CreateGroup(ctx, sampleGroup("BTCUSDT")); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	gid := model.NewGroupID("binance", "spot", "BTCUSDT")
	inputID := model.NewInputID(gid, "trade")

	lag1 := int64(42)
	off1 := int64(100)
	st := model.StreamRuntimeStatus{
		InputID:         inputID,
		GroupID:         gid,
		StreamKey:       "trade",
		NodeID:          "node-a",
		ActualStatus:    model.ActualStatusRunning,
		KafkaLag:        &lag1,
		CommittedOffset: &off1,
		LastError:       "",
	}
	if err := store.ReportStreamRuntimeStatus(ctx, st); err != nil {
		t.Fatalf("first ReportStreamRuntimeStatus: %v", err)
	}
	if got := countStatus(t, pool, inputID); got != 1 {
		t.Fatalf("status rows = %d, want 1", got)
	}

	// Second report upserts the same row and overrides lag/offset/last_error.
	lag2 := int64(7)
	off2 := int64(250)
	st.KafkaLag = &lag2
	st.CommittedOffset = &off2
	st.ActualStatus = model.ActualStatusError
	st.LastError = "decode failed"
	if err := store.ReportStreamRuntimeStatus(ctx, st); err != nil {
		t.Fatalf("second ReportStreamRuntimeStatus: %v", err)
	}
	if got := countStatus(t, pool, inputID); got != 1 {
		t.Fatalf("status rows after upsert = %d, want 1", got)
	}

	var (
		gotStatus string
		gotLag    int64
		gotOff    int64
		gotErr    string
	)
	if err := pool.QueryRow(ctx,
		`SELECT actual_status, kafka_lag, committed_offset, last_error
		   FROM stream_runtime_status WHERE input_id = $1`, inputID).
		Scan(&gotStatus, &gotLag, &gotOff, &gotErr); err != nil {
		t.Fatalf("read status: %v", err)
	}
	if gotStatus != model.ActualStatusError || gotLag != 7 || gotOff != 250 || gotErr != "decode failed" {
		t.Errorf("status not overwritten: status=%s lag=%d off=%d err=%q", gotStatus, gotLag, gotOff, gotErr)
	}
}

func TestListGroupsAndGetGroup(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()

	if err := store.CreateGroup(ctx, sampleGroup("BTCUSDT")); err != nil {
		t.Fatalf("CreateGroup BTC: %v", err)
	}
	if err := store.CreateGroup(ctx, sampleGroup("ETHUSDT")); err != nil {
		t.Fatalf("CreateGroup ETH: %v", err)
	}
	ethID := model.NewGroupID("binance", "spot", "ETHUSDT")
	if err := store.UpdateGroupDesiredStatus(ctx, ethID, model.DesiredStatusPaused); err != nil {
		t.Fatalf("pause ETH: %v", err)
	}

	// ListGroups returns all groups regardless of desired_status.
	groups, err := store.ListGroups(ctx)
	if err != nil {
		t.Fatalf("ListGroups: %v", err)
	}
	if len(groups) != 2 {
		t.Fatalf("ListGroups = %d, want 2", len(groups))
	}

	// GetGroup returns a single group, including nullable assets.
	g, err := store.GetGroup(ctx, ethID)
	if err != nil {
		t.Fatalf("GetGroup: %v", err)
	}
	if g.Symbol != "ETHUSDT" || g.DesiredStatus != model.DesiredStatusPaused {
		t.Errorf("GetGroup mismatch: %+v", g)
	}
	if g.BaseAsset != "BTC" || g.QuoteAsset != "USDT" {
		t.Errorf("nullable assets not round-tripped: %+v", g)
	}

	// Unknown group -> ErrNotFound.
	if _, err := store.GetGroup(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("GetGroup unknown err = %v, want ErrNotFound", err)
	}
}

func TestAddInputs(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()

	if err := store.CreateGroup(ctx, sampleGroup("BTCUSDT")); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	gid := model.NewGroupID("binance", "spot", "BTCUSDT")

	// Add a new input derived from stream_kind/interval.
	err := store.AddInputs(ctx, gid, []model.GroupInput{
		{StreamKind: model.StreamKindOrderBookDelta, KafkaTopic: "md.depth", KafkaGroupID: "rt", Enabled: true},
	})
	if err != nil {
		t.Fatalf("AddInputs: %v", err)
	}
	inputs, err := store.ListGroupInputs(ctx, gid)
	if err != nil {
		t.Fatalf("ListGroupInputs: %v", err)
	}
	if len(inputs) != 3 {
		t.Fatalf("inputs = %d, want 3", len(inputs))
	}

	// Duplicate stream_key -> ErrConflict.
	if err := store.AddInputs(ctx, gid, []model.GroupInput{
		{StreamKey: "trade", KafkaTopic: "md.trade", KafkaGroupID: "rt"},
	}); !errors.Is(err, ErrConflict) {
		t.Errorf("duplicate input err = %v, want ErrConflict", err)
	}

	// Unknown group -> ErrNotFound (foreign-key violation).
	if err := store.AddInputs(ctx, "nope", []model.GroupInput{
		{StreamKey: "trade", KafkaTopic: "md.trade", KafkaGroupID: "rt"},
	}); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown group err = %v, want ErrNotFound", err)
	}
}

func TestUpdateInputDesiredStatus(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()

	if err := store.CreateGroup(ctx, sampleGroup("BTCUSDT")); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	gid := model.NewGroupID("binance", "spot", "BTCUSDT")

	if err := store.UpdateInputDesiredStatus(ctx, gid, "trade", model.DesiredStatusPaused); err != nil {
		t.Fatalf("UpdateInputDesiredStatus: %v", err)
	}
	inputs, err := store.ListGroupInputs(ctx, gid)
	if err != nil {
		t.Fatalf("ListGroupInputs: %v", err)
	}
	for _, in := range inputs {
		want := model.DesiredStatusRunning
		if in.StreamKey == "trade" {
			want = model.DesiredStatusPaused
		}
		if in.DesiredStatus != want {
			t.Errorf("input %s desired_status = %s, want %s", in.StreamKey, in.DesiredStatus, want)
		}
	}

	// Unknown input -> ErrNotFound.
	if err := store.UpdateInputDesiredStatus(ctx, gid, "nope", model.DesiredStatusRunning); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown input err = %v, want ErrNotFound", err)
	}
	// Invalid status -> validation error.
	if err := store.UpdateInputDesiredStatus(ctx, gid, "trade", "bogus"); err == nil {
		t.Error("invalid status: want error, got nil")
	}
}

func TestGetGroupLeaseAndListStatus(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()

	if err := store.CreateGroup(ctx, sampleGroup("BTCUSDT")); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	gid := model.NewGroupID("binance", "spot", "BTCUSDT")

	// No lease yet -> (nil, nil).
	if lease, err := store.GetGroupLease(ctx, gid); err != nil || lease != nil {
		t.Fatalf("GetGroupLease (none): lease=%v err=%v, want nil/nil", lease, err)
	}

	if ok, err := store.TryAcquireGroupLease(ctx, gid, "node-a", 30*time.Second); err != nil || !ok {
		t.Fatalf("acquire: ok=%v err=%v", ok, err)
	}
	lease, err := store.GetGroupLease(ctx, gid)
	if err != nil {
		t.Fatalf("GetGroupLease: %v", err)
	}
	if lease == nil || lease.NodeID != "node-a" {
		t.Fatalf("lease = %+v, want owner node-a", lease)
	}

	// Report status for one input and read it back filtered by group.
	lag := int64(5)
	if err := store.ReportStreamRuntimeStatus(ctx, model.StreamRuntimeStatus{
		InputID:      model.NewInputID(gid, "trade"),
		GroupID:      gid,
		StreamKey:    "trade",
		NodeID:       "node-a",
		ActualStatus: model.ActualStatusRunning,
		KafkaLag:     &lag,
	}); err != nil {
		t.Fatalf("ReportStreamRuntimeStatus: %v", err)
	}
	statuses, err := store.ListStreamRuntimeStatus(ctx, gid)
	if err != nil {
		t.Fatalf("ListStreamRuntimeStatus: %v", err)
	}
	if len(statuses) != 1 || statuses[0].StreamKey != "trade" || statuses[0].NodeID != "node-a" {
		t.Fatalf("statuses = %+v, want one trade status owned by node-a", statuses)
	}
	if statuses[0].KafkaLag == nil || *statuses[0].KafkaLag != 5 {
		t.Errorf("kafka_lag not round-tripped: %+v", statuses[0])
	}
}

func TestRenewExpiredLeaseFails(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()

	if err := store.CreateGroup(ctx, sampleGroup("BTCUSDT")); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	gid := model.NewGroupID("binance", "spot", "BTCUSDT")

	// Acquire a very short lease, let it expire, then the owner's renew fails.
	if ok, err := store.TryAcquireGroupLease(ctx, gid, "node-a", 100*time.Millisecond); err != nil || !ok {
		t.Fatalf("acquire: ok=%v err=%v", ok, err)
	}
	time.Sleep(200 * time.Millisecond)
	if ok, err := store.RenewGroupLease(ctx, gid, "node-a", 30*time.Second); err != nil || ok {
		t.Fatalf("renew after expiry: ok=%v err=%v, want false", ok, err)
	}
	// And another node may take over the expired lease.
	if ok, err := store.TryAcquireGroupLease(ctx, gid, "node-b", 30*time.Second); err != nil || !ok {
		t.Fatalf("node-b takeover: ok=%v err=%v, want true", ok, err)
	}
}

func TestAcquireLeaseUnknownGroupNotFound(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	if _, err := store.TryAcquireGroupLease(ctx, "missing", "node-a", 30*time.Second); !errors.Is(err, ErrNotFound) {
		t.Fatalf("acquire unknown group err = %v, want ErrNotFound", err)
	}
}

func TestLeaseTTLGuard(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	if _, err := store.TryAcquireGroupLease(ctx, "g", "n", 0); err == nil {
		t.Error("acquire with ttl=0: want error, got nil")
	}
	if _, err := store.RenewGroupLease(ctx, "g", "n", -time.Second); err == nil {
		t.Error("renew with negative ttl: want error, got nil")
	}
}

func TestCreateGroupInvalidMarketType(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	g := sampleGroup("BTCUSDT")
	g.MarketType = "bogus"
	if err := store.CreateGroup(ctx, g); err == nil {
		t.Fatal("CreateGroup invalid market_type: want error, got nil")
	}
}

func TestCreateGroupInputConsistencyValidation(t *testing.T) {
	cases := []struct {
		name  string
		input model.GroupInput
	}{
		{"kline missing interval", model.GroupInput{StreamKind: model.StreamKindKline, KafkaTopic: "t"}},
		{"trade with interval", model.GroupInput{StreamKind: model.StreamKindTrade, Interval: "1m", KafkaTopic: "t"}},
		{"inconsistent key/kind", model.GroupInput{StreamKey: "trade", StreamKind: model.StreamKindKline, Interval: "1m", KafkaTopic: "t"}},
		{"missing kafka_topic", model.GroupInput{StreamKey: "trade"}},
		{"unknown stream_kind", model.GroupInput{StreamKind: "weird", KafkaTopic: "t"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store, _ := newStore(t)
			ctx := context.Background()
			g := model.MarketGroup{
				Exchange: "binance", MarketType: model.MarketTypeSpot, Symbol: "BTCUSDT",
				Inputs: []model.GroupInput{tc.input},
			}
			if err := store.CreateGroup(ctx, g); err == nil {
				t.Fatalf("CreateGroup with %s: want error, got nil", tc.name)
			}
		})
	}
}

func TestCreateGroupDerivesDefaultKafkaGroupID(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	g := model.MarketGroup{
		Exchange: "binance", MarketType: model.MarketTypeSpot, Symbol: "BTCUSDT",
		// kafka_group_id intentionally omitted -> derived default.
		Inputs: []model.GroupInput{{StreamKey: "trade", KafkaTopic: "md.trade"}},
	}
	if err := store.CreateGroup(ctx, g); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	gid := model.NewGroupID("binance", "spot", "BTCUSDT")
	inputs, err := store.ListGroupInputs(ctx, gid)
	if err != nil {
		t.Fatalf("ListGroupInputs: %v", err)
	}
	if len(inputs) != 1 {
		t.Fatalf("inputs = %d, want 1", len(inputs))
	}
	want := model.DefaultKafkaGroupID(gid, "trade")
	if inputs[0].KafkaGroupID != want {
		t.Errorf("kafka_group_id = %q, want %q", inputs[0].KafkaGroupID, want)
	}
}

// --- small read helpers --------------------------------------------------

func nodeName(n int) string { return "node-" + string(rune('a'+n)) }

func readUpdatedAt(t *testing.T, pool *pgxpool.Pool, groupID string) time.Time {
	t.Helper()
	var ts time.Time
	if err := pool.QueryRow(context.Background(),
		"SELECT updated_at FROM market_groups WHERE group_id = $1", groupID).Scan(&ts); err != nil {
		t.Fatalf("read updated_at: %v", err)
	}
	return ts
}

func readNodeHeartbeat(t *testing.T, pool *pgxpool.Pool, nodeID string) time.Time {
	t.Helper()
	var ts time.Time
	if err := pool.QueryRow(context.Background(),
		"SELECT last_heartbeat_at FROM runtime_nodes WHERE node_id = $1", nodeID).Scan(&ts); err != nil {
		t.Fatalf("read last_heartbeat_at: %v", err)
	}
	return ts
}

func countStatus(t *testing.T, pool *pgxpool.Pool, inputID string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(),
		"SELECT count(*) FROM stream_runtime_status WHERE input_id = $1", inputID).Scan(&n); err != nil {
		t.Fatalf("count status: %v", err)
	}
	return n
}
