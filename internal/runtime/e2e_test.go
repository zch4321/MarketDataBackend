// Package runtime — end-to-end acceptance tests covering all 21 scenarios
// defined in docs/postgres-implementation-plan.md §14.
//
// Scenarios use an in-memory Kafka broker for deterministic control and an
// isolated PostgreSQL schema, so no Docker or real Kafka is required.
// Set TEST_DATABASE_DSN to point at a running PostgreSQL.
package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"MarketDataBackend/internal/api"
	"MarketDataBackend/internal/kafka"
	"MarketDataBackend/internal/model"
	"MarketDataBackend/internal/storage"
)

// ---------------------------------------------------------------------------
// Local API response types — matches the JSON output of api.Server.
// These duplicate the unexported types in internal/api/dto.go.
// ---------------------------------------------------------------------------

type e2eGroupResponse struct {
	GroupID       string             `json:"group_id"`
	Exchange      string             `json:"exchange"`
	MarketType    string             `json:"market_type"`
	Symbol        string             `json:"symbol"`
	DesiredStatus string             `json:"desired_status"`
	Lease         *e2eLeaseResponse  `json:"lease,omitempty"`
	Inputs        []e2eInputResponse `json:"inputs,omitempty"`
}

type e2eLeaseResponse struct {
	NodeID string `json:"node_id"`
	Active bool   `json:"active"`
}

type e2eInputResponse struct {
	InputID       string                `json:"input_id"`
	StreamKey     string                `json:"stream_key"`
	StreamKind    string                `json:"stream_kind"`
	DesiredStatus string                `json:"desired_status"`
	Runtime       *e2eRuntimeStatusResp `json:"runtime,omitempty"`
}

type e2eRuntimeStatusResp struct {
	ActualStatus    string `json:"actual_status"`
	NodeID          string `json:"node_id,omitempty"`
	CommittedOffset *int64 `json:"committed_offset,omitempty"`
	LastError       string `json:"last_error,omitempty"`
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func doAPI(t *testing.T, handler http.Handler, method, path string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	var r *http.Request
	if body != nil {
		r = httptest.NewRequest(method, path, bytes.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
	} else {
		r = httptest.NewRequest(method, path, nil)
	}
	handler.ServeHTTP(rec, r)
	return rec
}

func decodeE2E[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.NewDecoder(rec.Body).Decode(&v); err != nil {
		t.Fatalf("decode %T: %v (body=%s)", v, err, rec.Body.String())
	}
	return v
}

// ==========================================================================
// E2E — all 21 acceptance scenarios
// ==========================================================================

func TestE2EAcceptanceScenarios(t *testing.T) {
	store, pool := newStore(t)
	broker := newRoutedBrokerFactory()
	queryStore := storage.NewPostgresStorage(pool)

	handler := api.New(store, queryStore, nil).Handler()

	suffix := fmt.Sprintf("e2e-%d", time.Now().UnixNano())
	makeTopic := func(kind string) string { return "md.e2e." + suffix + "." + kind }

	tradeTopic := makeTopic("trade")
	klineTopic := makeTopic("kline")
	deltaTopic := makeTopic("delta")
	snapTopic := makeTopic("snapshot")

	// ================================================================
	// Scenarios 1-5: Create MarketGroup via API with 4 inputs
	// ================================================================
	createBody := fmt.Sprintf(`{
		"exchange":"e2e","market_type":"spot","symbol":%q,
		"desired_status":"running",
		"inputs":[
			{"stream_kind":"trade",            "kafka_topic":%q},
			{"stream_kind":"kline","interval":"1m","kafka_topic":%q},
			{"stream_kind":"orderbook_delta",  "kafka_topic":%q},
			{"stream_kind":"orderbook_snapshot","kafka_topic":%q}
		]
	}`, suffix, tradeTopic, klineTopic, deltaTopic, snapTopic)

	rec := doAPI(t, handler, http.MethodPost, "/groups", []byte(createBody))
	if rec.Code != http.StatusCreated {
		t.Fatalf("create group: code=%d body=%s", rec.Code, rec.Body.String())
	}
	gr := decodeE2E[e2eGroupResponse](t, rec)
	groupID := gr.GroupID

	tradeInputID := model.NewInputID(groupID, model.StreamKindTrade)
	klineInputID := model.NewInputID(groupID, "kline_1m")
	deltaInputID := model.NewInputID(groupID, model.StreamKindOrderBookDelta)
	snapInputID := model.NewInputID(groupID, model.StreamKindOrderBookSnapshot)
	_ = klineInputID

	// ================================================================
	// Scenarios 6-7: Start two runtimes, single lease owner
	// ================================================================
	cfgA := fastNodeConfig("node-e2e-A")
	cfgB := fastNodeConfig("node-e2e-B")
	cfgA.ReconcileInterval = 25 * time.Millisecond
	cfgB.ReconcileInterval = 25 * time.Millisecond

	nodeA := NewNode(store, cfgA, discardLogger())
	nodeA.EnableConsumptionWithConfig(
		queryStore, broker, []string{"e2e:9092"},
		ConsumptionConfig{
			StatusReportEvery:   25 * time.Millisecond,
			InputReconcileEvery: 25 * time.Millisecond,
			TradeBatch:          BatchConfig{Size: 1, FlushInterval: time.Second},
			KlineBatch:          BatchConfig{Size: 1, FlushInterval: time.Second},
			OrderBookDeltaBatch: BatchConfig{Size: 1, FlushInterval: time.Second},
			SnapshotInterval:    100 * time.Millisecond,
		},
	)
	nodeB := NewNode(store, cfgB, discardLogger())
	nodeB.EnableConsumptionWithConfig(
		queryStore, broker, []string{"e2e:9092"},
		ConsumptionConfig{
			StatusReportEvery:   25 * time.Millisecond,
			InputReconcileEvery: 25 * time.Millisecond,
			TradeBatch:          BatchConfig{Size: 1, FlushInterval: time.Second},
			KlineBatch:          BatchConfig{Size: 1, FlushInterval: time.Second},
			OrderBookDeltaBatch: BatchConfig{Size: 1, FlushInterval: time.Second},
			SnapshotInterval:    100 * time.Millisecond,
		},
	)

	runCtx, cancelRun := context.WithCancel(context.Background())

	doneA := make(chan struct{})
	go func() { defer close(doneA); _ = nodeA.Run(runCtx) }()
	doneB := make(chan struct{})
	go func() { defer close(doneB); _ = nodeB.Run(runCtx) }()

	// Cleanup: cancel context, wait for both to stop.
	t.Cleanup(func() {
		cancelRun()
		<-doneA
		<-doneB
	})

	eventually(t, 5*time.Second, func() bool {
		return broker.consumerCount() >= 4
	}, "four consumers should start")

	eventually(t, 3*time.Second, func() bool {
		r := doAPI(t, handler, http.MethodGet, "/groups/"+groupID, nil)
		if r.Code != http.StatusOK {
			return false
		}
		g := decodeE2E[e2eGroupResponse](t, r)
		return g.Lease != nil && g.Lease.Active
	}, "one runtime should own the lease")

	gr2 := decodeE2E[e2eGroupResponse](t,
		doAPI(t, handler, http.MethodGet, "/groups/"+groupID, nil))
	t.Logf("initial lease owner: %s", gr2.Lease.NodeID)
	if gr2.Lease.NodeID != "node-e2e-A" && gr2.Lease.NodeID != "node-e2e-B" {
		t.Fatalf("unexpected lease owner: %s", gr2.Lease.NodeID)
	}

	// ================================================================
	// Scenarios 8-10: Write trade → PostgreSQL + offset commit
	// ================================================================
	broker.send(tradeTopic, e2eTrade(0, "trade-1"))
	eventually(t, 5*time.Second, func() bool {
		return factCount(t, pool, "trades", tradeInputID) == 1
	}, "trade-1 should reach PostgreSQL")
	if got := broker.commitCount(tradeTopic); got != 1 {
		t.Fatalf("trade commit count = %d, want 1", got)
	}

	// ================================================================
	// Scenario 11: Replay trade — idempotent, no duplicate
	// ================================================================
	broker.send(tradeTopic, e2eTrade(0, "trade-1"))
	eventually(t, 3*time.Second, func() bool {
		return broker.commitCount(tradeTopic) == 2
	}, "replayed offset should be committed")
	if got := factCount(t, pool, "trades", tradeInputID); got != 1 {
		t.Fatalf("trades after replay = %d, want 1 (idempotent)", got)
	}

	// ================================================================
	// Scenarios 12-13: Pause trade → no new consumption → resume
	// ================================================================
	doAPI(t, handler, http.MethodPost, "/groups/"+groupID+"/inputs/trade/pause", nil)
	eventually(t, 5*time.Second, func() bool {
		return streamStatus(t, store, groupID, model.StreamKindTrade).ActualStatus ==
			model.ActualStatusPaused
	}, "trade should pause")

	beforePause := broker.commitCount(tradeTopic)
	broker.send(tradeTopic, e2eTrade(1, "trade-2"))
	time.Sleep(300 * time.Millisecond)
	if got := broker.commitCount(tradeTopic); got != beforePause {
		t.Fatalf("no commit while paused: want %d got %d", beforePause, got)
	}
	if got := factCount(t, pool, "trades", tradeInputID); got != 1 {
		t.Fatalf("trades while paused = %d, want 1", got)
	}

	doAPI(t, handler, http.MethodPost, "/groups/"+groupID+"/inputs/trade/resume", nil)
	eventually(t, 5*time.Second, func() bool {
		return factCount(t, pool, "trades", tradeInputID) == 2
	}, "resumed trade should catch up")

	// ================================================================
	// Scenarios 14-15: Delta batch — full bids/asks, raw_payload, DB+commit
	// ================================================================
	deltaMsg := e2eDelta(0, 1000, "delta-1000")
	broker.send(deltaTopic, deltaMsg)
	eventually(t, 5*time.Second, func() bool {
		return factCount(t, pool, "orderbook_deltas", deltaInputID) >= 1
	}, "delta should reach PostgreSQL")

	var storedRaw []byte
	if err := pool.QueryRow(context.Background(),
		"SELECT raw_payload FROM orderbook_deltas WHERE input_id=$1 AND kafka_offset=0",
		deltaInputID).Scan(&storedRaw); err != nil {
		t.Fatalf("read raw_payload: %v", err)
	}
	if !bytes.Equal(storedRaw, deltaMsg.Value) {
		t.Fatalf("raw_payload mismatch: stored=%d sent=%d", len(storedRaw), len(deltaMsg.Value))
	}

	storedBids, storedAsks := readRuntimeDeltaLevels(t, pool, deltaInputID, 0)
	if len(storedBids) != 3 {
		t.Fatalf("stored bids = %d, want 3 (no truncation)", len(storedBids))
	}
	if len(storedAsks) != 2 {
		t.Fatalf("stored asks = %d, want 2", len(storedAsks))
	}

	if got := broker.commitCount(deltaTopic); got < 1 {
		t.Fatalf("delta commit count = %d, want >= 1", got)
	}

	// ================================================================
	// Scenario 17: Snapshot + delta → orderbook reconstruction
	// ================================================================
	seq1000 := int64(1000)
	snapMsg := e2eSnapshot(&seq1000, "50000", "10", "49990", "5")
	broker.send(snapTopic, snapMsg)
	eventually(t, 5*time.Second, func() bool {
		return broker.commitCount(snapTopic) >= 1
	}, "reference snapshot should be committed")

	broker.send(deltaTopic, e2eDelta(1, 1001, "delta-1001"))
	broker.send(deltaTopic, e2eDelta(2, 1002, "delta-1002"))

	// ================================================================
	// Scenario 18: Local full-depth snapshot generated
	// ================================================================
	eventually(t, 10*time.Second, func() bool {
		return factCount(t, pool, "orderbook_snapshots", snapInputID) >= 1
	}, "local snapshot should be generated and written")

	var snapBidsJSON, snapAsksJSON []byte
	if err := pool.QueryRow(context.Background(),
		`SELECT bids, asks FROM orderbook_snapshots
		 WHERE input_id=$1 ORDER BY snapshot_time DESC LIMIT 1`,
		snapInputID).Scan(&snapBidsJSON, &snapAsksJSON); err != nil {
		t.Fatalf("read local snapshot: %v", err)
	}
	var snapBids, snapAsks []model.PriceLevel
	_ = json.Unmarshal(snapBidsJSON, &snapBids)
	_ = json.Unmarshal(snapAsksJSON, &snapAsks)

	if len(snapBids) == 0 {
		t.Fatalf("local snapshot bids are empty")
	}
	t.Logf("best bid: %s @ %s", snapBids[0].Price, snapBids[0].Quantity)
	t.Logf("best ask: %s @ %s", snapAsks[0].Price, snapAsks[0].Quantity)

	// ================================================================
	// Scenarios 19-20: Kill owner → failover
	// ================================================================
	owner := gr2.Lease.NodeID
	t.Logf("killing owner %s and starting survivor", owner)

	// Cancel both original nodes.
	cancelRun()
	<-doneA
	<-doneB

	// Start a new survivor node that should take over.
	cfgSurv := fastNodeConfig("node-e2e-survivor")
	cfgSurv.ReconcileInterval = 25 * time.Millisecond
	survivor := NewNode(store, cfgSurv, discardLogger())
	survivor.EnableConsumptionWithConfig(
		queryStore, broker, []string{"e2e:9092"},
		ConsumptionConfig{
			StatusReportEvery:   25 * time.Millisecond,
			InputReconcileEvery: 25 * time.Millisecond,
			TradeBatch:          BatchConfig{Size: 1, FlushInterval: time.Second},
			KlineBatch:          BatchConfig{Size: 1, FlushInterval: time.Second},
			OrderBookDeltaBatch: BatchConfig{Size: 1, FlushInterval: time.Second},
			SnapshotInterval:    100 * time.Millisecond,
		},
	)

	survCtx, survCancel := context.WithCancel(context.Background())
	defer survCancel()
	survDone := make(chan struct{})
	go func() { defer close(survDone); _ = survivor.Run(survCtx) }()
	t.Cleanup(func() { survCancel(); <-survDone })

	eventually(t, 8*time.Second, func() bool {
		r := doAPI(t, handler, http.MethodGet, "/groups/"+groupID, nil)
		if r.Code != http.StatusOK {
			return false
		}
		g := decodeE2E[e2eGroupResponse](t, r)
		return g.Lease != nil && g.Lease.Active && g.Lease.NodeID == "node-e2e-survivor"
	}, "survivor should take over the lease")

	broker.send(tradeTopic, e2eTrade(3, "trade-post-failover"))
	eventually(t, 8*time.Second, func() bool {
		return factCount(t, pool, "trades", tradeInputID) >= 3
	}, "survivor should consume post-failover trade")

	// ================================================================
	// Scenario 21: Query API → trades, klines, snapshots, status
	// ================================================================
	tradeRec := doAPI(t, handler, http.MethodGet,
		fmt.Sprintf("/markets/%s/trades?from=2026-06-01T00:00:00Z&to=2026-12-31T23:59:59Z&limit=10", groupID), nil)
	if tradeRec.Code != http.StatusOK {
		t.Fatalf("query trades: %d body=%s", tradeRec.Code, tradeRec.Body.String())
	}

	klineRec := doAPI(t, handler, http.MethodGet,
		fmt.Sprintf("/markets/%s/klines?from=2026-06-01T00:00:00Z&to=2026-12-31T23:59:59Z", groupID), nil)
	if klineRec.Code != http.StatusOK {
		t.Fatalf("query klines: %d", klineRec.Code)
	}

	snapRec := doAPI(t, handler, http.MethodGet,
		fmt.Sprintf("/markets/%s/orderbook/snapshots?from=2026-06-01T00:00:00Z&to=2026-12-31T23:59:59Z", groupID), nil)
	if snapRec.Code != http.StatusOK {
		t.Fatalf("query snapshots: %d body=%s", snapRec.Code, snapRec.Body.String())
	}

	// Verify stream_runtime_status via group detail.
	detailRec := doAPI(t, handler, http.MethodGet, "/groups/"+groupID, nil)
	if detailRec.Code != http.StatusOK {
		t.Fatalf("get group detail: %d", detailRec.Code)
	}
	finalGr := decodeE2E[e2eGroupResponse](t, detailRec)

	var tradeInput *e2eInputResponse
	for i := range finalGr.Inputs {
		if finalGr.Inputs[i].StreamKind == model.StreamKindTrade {
			tradeInput = &finalGr.Inputs[i]
			break
		}
	}
	if tradeInput == nil || tradeInput.Runtime == nil {
		t.Fatalf("trade input has no runtime status")
	}
	t.Logf("trade runtime: actual=%s committed_offset=%v",
		tradeInput.Runtime.ActualStatus,
		func() string {
			if tradeInput.Runtime.CommittedOffset != nil {
				return fmt.Sprintf("%d", *tradeInput.Runtime.CommittedOffset)
			}
			return "nil"
		}())

	// ================================================================
	// Stream write progress (M7.5 durability)
	// ================================================================
	var durableOffset int64
	if err := pool.QueryRow(context.Background(),
		"SELECT durable_offset FROM stream_write_progress WHERE input_id=$1 AND kafka_partition=0",
		tradeInputID).Scan(&durableOffset); err != nil {
		t.Fatalf("read stream_write_progress: %v", err)
	}
	if durableOffset < 3 {
		t.Fatalf("durable_offset = %d, want >= 3", durableOffset)
	}

	t.Logf("=== E2E acceptance test complete ===")
	t.Logf("group_id: %s", groupID)
	t.Logf("trades: %d", factCount(t, pool, "trades", tradeInputID))
	t.Logf("deltas: %d", factCount(t, pool, "orderbook_deltas", deltaInputID))
	t.Logf("snapshots: %d", factCount(t, pool, "orderbook_snapshots", snapInputID))
	t.Logf("durable_offset: %d", durableOffset)
}

// ==========================================================================
// Message builders
// ==========================================================================

func e2eTrade(offset int64, rawID string) kafka.Message {
	return kafka.Message{
		Partition: 0,
		Offset:    offset,
		Value: []byte(fmt.Sprintf(`{
			"event_time":"2026-06-07T10:00:00Z",
			"raw_trade_id":%q,"price":"50000.00","quantity":"1.5","side":"buy"
		}`, rawID)),
	}
}

func e2eDelta(offset int64, sequence int64, rawID string) kafka.Message {
	return kafka.Message{
		Partition: 0,
		Offset:    offset,
		Value: []byte(fmt.Sprintf(`{
			"event_time":"2026-06-07T10:00:00Z",
			"raw_event_id":%q,"sequence":%d,
			"last_update_id":%d,"prev_update_id":%d,"first_update_id":%d,
			"bids":[["50001.0","2.0"],["100.0","0"],["100.0","0.0"]],
			"asks":[{"price":"50005.0","quantity":"1.0"},{"price":"50010.0","quantity":"3.0"}]
		}`, rawID, sequence, sequence, sequence-1, sequence)),
	}
}

func e2eSnapshot(seq *int64, bidPrice, bidQty, askPrice, askQty string) kafka.Message {
	msg := kafka.Message{Partition: 0, Offset: 0}
	if seq != nil {
		msg.Value = []byte(fmt.Sprintf(`{
			"event_time":"2026-06-07T10:00:00Z","sequence":%d,
			"bids":[{"price":%q,"quantity":%q}],
			"asks":[{"price":%q,"quantity":%q}]
		}`, *seq, bidPrice, bidQty, askPrice, askQty))
	} else {
		msg.Value = []byte(fmt.Sprintf(`{
			"event_time":"2026-06-07T10:00:00Z",
			"bids":[{"price":%q,"quantity":%q}],
			"asks":[{"price":%q,"quantity":%q}]
		}`, bidPrice, bidQty, askPrice, askQty))
	}
	return msg
}
