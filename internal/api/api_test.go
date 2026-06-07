package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

	"MarketDataBackend/internal/metadata"
	"MarketDataBackend/internal/model"
)

// --- in-memory fake Store ------------------------------------------------

type fakeStore struct {
	mu       sync.Mutex
	groups   map[string]model.MarketGroup
	inputs   map[string][]model.GroupInput
	leases   map[string]*model.GroupLease
	statuses map[string][]model.StreamRuntimeStatus
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		groups:   map[string]model.MarketGroup{},
		inputs:   map[string][]model.GroupInput{},
		leases:   map[string]*model.GroupLease{},
		statuses: map[string][]model.StreamRuntimeStatus{},
	}
}

func (f *fakeStore) CreateGroup(_ context.Context, g model.MarketGroup) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	gid := model.NewGroupID(g.Exchange, g.MarketType, g.Symbol)
	if _, ok := f.groups[gid]; ok {
		return fmt.Errorf("%w: %s", metadata.ErrConflict, gid)
	}
	g.GroupID = gid
	if g.DesiredStatus == "" {
		g.DesiredStatus = model.DesiredStatusRunning
	}
	if g.Weight <= 0 {
		g.Weight = 1
	}
	now := time.Now()
	g.CreatedAt, g.UpdatedAt = now, now
	inputs := g.Inputs
	g.Inputs = nil
	f.groups[gid] = g
	for _, in := range inputs {
		if err := f.addInputLocked(gid, in); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeStore) addInputLocked(gid string, in model.GroupInput) error {
	in.GroupID = gid
	if in.InputID == "" {
		in.InputID = model.NewInputID(gid, in.StreamKey)
	}
	if in.DesiredStatus == "" {
		in.DesiredStatus = model.DesiredStatusRunning
	}
	if in.KafkaGroupID == "" {
		in.KafkaGroupID = model.DefaultKafkaGroupID(gid, in.StreamKey)
	}
	for _, ex := range f.inputs[gid] {
		if ex.StreamKey == in.StreamKey {
			return fmt.Errorf("%w: input %s", metadata.ErrConflict, in.StreamKey)
		}
	}
	f.inputs[gid] = append(f.inputs[gid], in)
	return nil
}

func (f *fakeStore) ListGroups(context.Context) ([]model.MarketGroup, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]model.MarketGroup, 0, len(f.groups))
	for _, g := range f.groups {
		out = append(out, g)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GroupID < out[j].GroupID })
	return out, nil
}

func (f *fakeStore) GetGroup(_ context.Context, groupID string) (model.MarketGroup, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	g, ok := f.groups[groupID]
	if !ok {
		return model.MarketGroup{}, fmt.Errorf("%w: %s", metadata.ErrNotFound, groupID)
	}
	return g, nil
}

func (f *fakeStore) UpdateGroupDesiredStatus(_ context.Context, groupID, status string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	g, ok := f.groups[groupID]
	if !ok {
		return fmt.Errorf("%w: %s", metadata.ErrNotFound, groupID)
	}
	g.DesiredStatus = status
	g.UpdatedAt = time.Now()
	f.groups[groupID] = g
	return nil
}

func (f *fakeStore) AddInputs(_ context.Context, groupID string, inputs []model.GroupInput) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.groups[groupID]; !ok {
		return fmt.Errorf("%w: %s", metadata.ErrNotFound, groupID)
	}
	for _, in := range inputs {
		if err := f.addInputLocked(groupID, in); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeStore) ListGroupInputs(_ context.Context, groupID string) ([]model.GroupInput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := append([]model.GroupInput(nil), f.inputs[groupID]...)
	sort.Slice(out, func(i, j int) bool { return out[i].StreamKey < out[j].StreamKey })
	return out, nil
}

func (f *fakeStore) UpdateInputDesiredStatus(_ context.Context, groupID, streamKey, status string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	ins := f.inputs[groupID]
	for i := range ins {
		if ins[i].StreamKey == streamKey {
			ins[i].DesiredStatus = status
			ins[i].UpdatedAt = time.Now()
			return nil
		}
	}
	return fmt.Errorf("%w: input %s/%s", metadata.ErrNotFound, groupID, streamKey)
}

func (f *fakeStore) GetGroupLease(_ context.Context, groupID string) (*model.GroupLease, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.leases[groupID], nil
}

func (f *fakeStore) ListStreamRuntimeStatus(_ context.Context, groupID string) ([]model.StreamRuntimeStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]model.StreamRuntimeStatus(nil), f.statuses[groupID]...), nil
}

// compile-time check the fake satisfies the consumer interface.
var _ Store = (*fakeStore)(nil)

// --- request helpers -----------------------------------------------------

func newServer(store Store) http.Handler {
	return New(store, nil).Handler()
}

func do(t *testing.T, h http.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatalf("encode body: %v", err)
		}
	}
	req := httptest.NewRequest(method, path, &buf)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decode[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("decode response %q: %v", rec.Body.String(), err)
	}
	return v
}

func validCreateBody(symbol string) createGroupRequest {
	return createGroupRequest{
		Exchange:   "binance",
		MarketType: model.MarketTypeSpot,
		Symbol:     symbol,
		BaseAsset:  "BTC",
		QuoteAsset: "USDT",
		Inputs: []inputRequest{
			{StreamKey: "trade", KafkaTopic: "md.trade", KafkaGroupID: "rt"},
			{StreamKind: "kline", Interval: "1m", KafkaTopic: "md.kline", KafkaGroupID: "rt"},
		},
	}
}

// --- tests ---------------------------------------------------------------

func TestCreateGroup(t *testing.T) {
	h := newServer(newFakeStore())

	rec := do(t, h, http.MethodPost, "/groups", validCreateBody("BTCUSDT"))
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: status = %d, want 201 (%s)", rec.Code, rec.Body.String())
	}
	resp := decode[groupResponse](t, rec)
	if resp.GroupID != "binance:spot:BTCUSDT" || resp.DesiredStatus != model.DesiredStatusRunning {
		t.Fatalf("unexpected group: %+v", resp)
	}
	if len(resp.Inputs) != 2 {
		t.Fatalf("inputs = %d, want 2", len(resp.Inputs))
	}
	// Inputs without any reported status default to pending.
	for _, in := range resp.Inputs {
		if in.Runtime == nil || in.Runtime.ActualStatus != model.ActualStatusPending {
			t.Errorf("input %s runtime = %+v, want pending", in.StreamKey, in.Runtime)
		}
	}
	// kline_1m input derived its stream_kind/interval from stream_kind+interval.
	var kline *inputResponse
	for i := range resp.Inputs {
		if resp.Inputs[i].StreamKey == "kline_1m" {
			kline = &resp.Inputs[i]
		}
	}
	if kline == nil || kline.StreamKind != model.StreamKindKline || kline.Interval != "1m" {
		t.Fatalf("kline input wrong: %+v", kline)
	}
}

func TestCreateGroupDuplicateConflict(t *testing.T) {
	h := newServer(newFakeStore())
	if rec := do(t, h, http.MethodPost, "/groups", validCreateBody("ETHUSDT")); rec.Code != http.StatusCreated {
		t.Fatalf("first create: %d", rec.Code)
	}
	rec := do(t, h, http.MethodPost, "/groups", validCreateBody("ETHUSDT"))
	if rec.Code != http.StatusConflict {
		t.Fatalf("duplicate create: status = %d, want 409", rec.Code)
	}
}

func TestCreateGroupValidation(t *testing.T) {
	h := newServer(newFakeStore())

	cases := []struct {
		name string
		body createGroupRequest
	}{
		{
			name: "invalid market_type",
			body: func() createGroupRequest { b := validCreateBody("BTCUSDT"); b.MarketType = "bogus"; return b }(),
		},
		{
			name: "missing symbol",
			body: func() createGroupRequest { b := validCreateBody(""); b.Symbol = ""; return b }(),
		},
		{
			name: "invalid stream_kind",
			body: func() createGroupRequest {
				b := validCreateBody("BTCUSDT")
				b.Inputs = []inputRequest{{StreamKind: "bogus", KafkaTopic: "t", KafkaGroupID: "g"}}
				return b
			}(),
		},
		{
			name: "kline missing interval",
			body: func() createGroupRequest {
				b := validCreateBody("BTCUSDT")
				b.Inputs = []inputRequest{{StreamKind: "kline", KafkaTopic: "t", KafkaGroupID: "g"}}
				return b
			}(),
		},
		{
			name: "trade with interval",
			body: func() createGroupRequest {
				b := validCreateBody("BTCUSDT")
				b.Inputs = []inputRequest{{StreamKind: "trade", Interval: "1m", KafkaTopic: "t", KafkaGroupID: "g"}}
				return b
			}(),
		},
		{
			name: "inconsistent stream_key and stream_kind",
			body: func() createGroupRequest {
				b := validCreateBody("BTCUSDT")
				b.Inputs = []inputRequest{{StreamKey: "trade", StreamKind: "kline", Interval: "1m", KafkaTopic: "t", KafkaGroupID: "g"}}
				return b
			}(),
		},
		{
			name: "invalid desired_status",
			body: func() createGroupRequest { b := validCreateBody("BTCUSDT"); b.DesiredStatus = "bogus"; return b }(),
		},
		{
			name: "missing kafka_topic",
			body: func() createGroupRequest {
				b := validCreateBody("BTCUSDT")
				b.Inputs = []inputRequest{{StreamKey: "trade", KafkaGroupID: "g"}}
				return b
			}(),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := do(t, h, http.MethodPost, "/groups", tc.body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (%s)", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestPauseResumeGroup(t *testing.T) {
	h := newServer(newFakeStore())
	if rec := do(t, h, http.MethodPost, "/groups", validCreateBody("BTCUSDT")); rec.Code != http.StatusCreated {
		t.Fatalf("create: %d", rec.Code)
	}

	rec := do(t, h, http.MethodPost, "/groups/binance:spot:BTCUSDT/pause", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("pause: status = %d, want 200", rec.Code)
	}
	if g := decode[groupResponse](t, rec); g.DesiredStatus != model.DesiredStatusPaused {
		t.Fatalf("desired_status = %s, want paused", g.DesiredStatus)
	}

	rec = do(t, h, http.MethodPost, "/groups/binance:spot:BTCUSDT/resume", nil)
	if g := decode[groupResponse](t, rec); g.DesiredStatus != model.DesiredStatusRunning {
		t.Fatalf("resume desired_status = %s, want running", g.DesiredStatus)
	}

	// Unknown group -> 404.
	if rec := do(t, h, http.MethodPost, "/groups/none/pause", nil); rec.Code != http.StatusNotFound {
		t.Fatalf("pause unknown: status = %d, want 404", rec.Code)
	}
}

func TestPauseResumeInput(t *testing.T) {
	h := newServer(newFakeStore())
	if rec := do(t, h, http.MethodPost, "/groups", validCreateBody("BTCUSDT")); rec.Code != http.StatusCreated {
		t.Fatalf("create: %d", rec.Code)
	}

	if rec := do(t, h, http.MethodPost, "/groups/binance:spot:BTCUSDT/inputs/trade/pause", nil); rec.Code != http.StatusOK {
		t.Fatalf("pause input: status = %d, want 200", rec.Code)
	}

	// Verify via aggregated GET that only the trade input was paused.
	rec := do(t, h, http.MethodGet, "/groups/binance:spot:BTCUSDT", nil)
	g := decode[groupResponse](t, rec)
	for _, in := range g.Inputs {
		want := model.DesiredStatusRunning
		if in.StreamKey == "trade" {
			want = model.DesiredStatusPaused
		}
		if in.DesiredStatus != want {
			t.Errorf("input %s desired_status = %s, want %s", in.StreamKey, in.DesiredStatus, want)
		}
	}

	// Unknown input -> 404.
	if rec := do(t, h, http.MethodPost, "/groups/binance:spot:BTCUSDT/inputs/nope/resume", nil); rec.Code != http.StatusNotFound {
		t.Fatalf("resume unknown input: status = %d, want 404", rec.Code)
	}
}

func TestGetGroupAggregatesLeaseAndStatus(t *testing.T) {
	store := newFakeStore()
	h := newServer(store)
	if rec := do(t, h, http.MethodPost, "/groups", validCreateBody("BTCUSDT")); rec.Code != http.StatusCreated {
		t.Fatalf("create: %d", rec.Code)
	}
	gid := "binance:spot:BTCUSDT"

	// Seed a lease and a runtime status for the trade input only.
	lag := int64(12)
	off := int64(345)
	store.leases[gid] = &model.GroupLease{
		GroupID: gid, NodeID: "node-a", LeaseExpiresAt: time.Now().Add(time.Minute), Version: 3,
	}
	store.statuses[gid] = []model.StreamRuntimeStatus{{
		InputID:         model.NewInputID(gid, "trade"),
		GroupID:         gid,
		StreamKey:       "trade",
		NodeID:          "node-a",
		ActualStatus:    model.ActualStatusRunning,
		KafkaLag:        &lag,
		CommittedOffset: &off,
		UpdatedAt:       time.Now(),
	}}

	rec := do(t, h, http.MethodGet, "/groups/"+gid, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("get: status = %d, want 200", rec.Code)
	}
	g := decode[groupResponse](t, rec)
	if g.Lease == nil || g.Lease.NodeID != "node-a" || g.Lease.Version != 3 {
		t.Fatalf("lease aggregation wrong: %+v", g.Lease)
	}
	var trade, kline *inputResponse
	for i := range g.Inputs {
		switch g.Inputs[i].StreamKey {
		case "trade":
			trade = &g.Inputs[i]
		case "kline_1m":
			kline = &g.Inputs[i]
		}
	}
	if trade == nil || trade.Runtime == nil || trade.Runtime.ActualStatus != model.ActualStatusRunning {
		t.Fatalf("trade runtime wrong: %+v", trade)
	}
	if trade.Runtime.KafkaLag == nil || *trade.Runtime.KafkaLag != 12 {
		t.Fatalf("trade lag wrong: %+v", trade.Runtime)
	}
	// The kline input never reported -> pending default.
	if kline == nil || kline.Runtime == nil || kline.Runtime.ActualStatus != model.ActualStatusPending {
		t.Fatalf("kline runtime = %+v, want pending", kline)
	}
}

func TestGetUnknownGroup(t *testing.T) {
	h := newServer(newFakeStore())
	if rec := do(t, h, http.MethodGet, "/groups/none", nil); rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestCreateGroupPaused(t *testing.T) {
	h := newServer(newFakeStore())
	body := validCreateBody("BTCUSDT")
	body.DesiredStatus = model.DesiredStatusPaused
	rec := do(t, h, http.MethodPost, "/groups", body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: status = %d (%s)", rec.Code, rec.Body.String())
	}
	if g := decode[groupResponse](t, rec); g.DesiredStatus != model.DesiredStatusPaused {
		t.Fatalf("desired_status = %s, want paused", g.DesiredStatus)
	}
}

func TestCreateGroupDerivesKafkaGroupID(t *testing.T) {
	h := newServer(newFakeStore())
	body := validCreateBody("BTCUSDT")
	// Omit kafka_group_id on both inputs -> server derives a stable default.
	for i := range body.Inputs {
		body.Inputs[i].KafkaGroupID = ""
	}
	rec := do(t, h, http.MethodPost, "/groups", body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: status = %d (%s)", rec.Code, rec.Body.String())
	}
	g := decode[groupResponse](t, rec)
	for _, in := range g.Inputs {
		want := model.DefaultKafkaGroupID(g.GroupID, in.StreamKey)
		if in.KafkaGroupID != want {
			t.Errorf("input %s kafka_group_id = %q, want %q", in.StreamKey, in.KafkaGroupID, want)
		}
	}
}

func TestExpiredLeaseReportedInactive(t *testing.T) {
	store := newFakeStore()
	h := newServer(store)
	if rec := do(t, h, http.MethodPost, "/groups", validCreateBody("BTCUSDT")); rec.Code != http.StatusCreated {
		t.Fatalf("create: %d", rec.Code)
	}
	gid := "binance:spot:BTCUSDT"
	// Seed an already-expired lease (dead owner).
	store.leases[gid] = &model.GroupLease{
		GroupID: gid, NodeID: "dead-node", LeaseExpiresAt: time.Now().Add(-time.Minute), Version: 1,
	}

	rec := do(t, h, http.MethodGet, "/groups/"+gid, nil)
	g := decode[groupResponse](t, rec)
	if g.Lease == nil {
		t.Fatal("lease missing from response")
	}
	if g.Lease.Active {
		t.Errorf("expired lease reported active=true, want false")
	}
}

func TestCreateAndListInputs(t *testing.T) {
	h := newServer(newFakeStore())
	if rec := do(t, h, http.MethodPost, "/groups", validCreateBody("BTCUSDT")); rec.Code != http.StatusCreated {
		t.Fatalf("create: %d", rec.Code)
	}
	gid := "binance:spot:BTCUSDT"

	// Add a new orderbook_delta input.
	body := inputRequest{StreamKind: "orderbook_delta", KafkaTopic: "md.depth", KafkaGroupID: "rt"}
	rec := do(t, h, http.MethodPost, "/groups/"+gid+"/inputs", body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create input: status = %d, want 201 (%s)", rec.Code, rec.Body.String())
	}
	if in := decode[inputResponse](t, rec); in.StreamKey != "orderbook_delta" {
		t.Fatalf("created input stream_key = %s", in.StreamKey)
	}

	// Duplicate -> 409.
	if rec := do(t, h, http.MethodPost, "/groups/"+gid+"/inputs", body); rec.Code != http.StatusConflict {
		t.Fatalf("duplicate input: status = %d, want 409", rec.Code)
	}

	// Adding to an unknown group -> 404.
	if rec := do(t, h, http.MethodPost, "/groups/none/inputs", body); rec.Code != http.StatusNotFound {
		t.Fatalf("input on unknown group: status = %d, want 404", rec.Code)
	}

	// List inputs -> 3 (trade, kline_1m, orderbook_delta).
	rec = do(t, h, http.MethodGet, "/groups/"+gid+"/inputs", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("list inputs: %d", rec.Code)
	}
	if inputs := decode[[]inputResponse](t, rec); len(inputs) != 3 {
		t.Fatalf("inputs = %d, want 3", len(inputs))
	}
}

func TestListGroups(t *testing.T) {
	h := newServer(newFakeStore())
	if rec := do(t, h, http.MethodPost, "/groups", validCreateBody("BTCUSDT")); rec.Code != http.StatusCreated {
		t.Fatalf("create BTC: %d", rec.Code)
	}
	if rec := do(t, h, http.MethodPost, "/groups", validCreateBody("ETHUSDT")); rec.Code != http.StatusCreated {
		t.Fatalf("create ETH: %d", rec.Code)
	}
	rec := do(t, h, http.MethodGet, "/groups", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("list: %d", rec.Code)
	}
	if groups := decode[[]groupResponse](t, rec); len(groups) != 2 {
		t.Fatalf("groups = %d, want 2", len(groups))
	}
}
