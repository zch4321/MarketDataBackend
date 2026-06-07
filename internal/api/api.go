// Package api implements the control-plane REST API: creating, pausing,
// resuming and inspecting market groups and their input streams. Responses
// merge desired_status (market_groups), the current lease owner (market_leases)
// and observed runtime status (stream_runtime_status) so callers can explain
// the gap between intended and actual processing.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"MarketDataBackend/internal/metadata"
	"MarketDataBackend/internal/model"
	"MarketDataBackend/internal/storage"
)

// Store is the subset of metadata.Store the API depends on. Defining it here
// (consumer side) keeps the handlers easy to unit test with a fake.
type Store interface {
	CreateGroup(ctx context.Context, g model.MarketGroup) error
	ListGroups(ctx context.Context) ([]model.MarketGroup, error)
	GetGroup(ctx context.Context, groupID string) (model.MarketGroup, error)
	UpdateGroupDesiredStatus(ctx context.Context, groupID, status string) error

	AddInputs(ctx context.Context, groupID string, inputs []model.GroupInput) error
	ListGroupInputs(ctx context.Context, groupID string) ([]model.GroupInput, error)
	UpdateInputDesiredStatus(ctx context.Context, groupID, streamKey, status string) error

	GetGroupLease(ctx context.Context, groupID string) (*model.GroupLease, error)
	ListStreamRuntimeStatus(ctx context.Context, groupID string) ([]model.StreamRuntimeStatus, error)
}

// QueryStore is the subset of storage.MarketDataStorage needed by the query API.
type QueryStore interface {
	QueryTrades(
		ctx context.Context, groupID string, from, to time.Time,
		limit int, cursor *time.Time,
	) (storage.QueryResult[model.Trade], error)
	QueryKlines(
		ctx context.Context, groupID string, from, to time.Time,
		limit int, cursor *time.Time,
	) (storage.QueryResult[model.Kline], error)
	QuerySnapshots(
		ctx context.Context, groupID string, from, to time.Time,
		limit int, cursor *time.Time,
	) (storage.QueryResult[model.OrderBookSnapshot], error)
}

// Server hosts the control-plane HTTP handlers including the M8 query endpoints.
type Server struct {
	store  Store
	query  QueryStore
	logger *slog.Logger
}

// New builds a Server. A nil logger falls back to slog.Default. query may be nil
// when only the control-plane endpoints are needed (tests, minimal deployments).
func New(store Store, query QueryStore, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{store: store, query: query, logger: logger}
}

// Handler returns the fully routed http.Handler. Routing uses the method-aware
// ServeMux patterns and path wildcards (Go 1.22+).
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /groups", s.handleCreateGroup)
	mux.HandleFunc("GET /groups", s.handleListGroups)
	mux.HandleFunc("GET /groups/{group_id}", s.handleGetGroup)
	mux.HandleFunc("POST /groups/{group_id}/pause", s.handlePauseGroup)
	mux.HandleFunc("POST /groups/{group_id}/resume", s.handleResumeGroup)
	mux.HandleFunc("POST /groups/{group_id}/inputs", s.handleCreateInput)
	mux.HandleFunc("GET /groups/{group_id}/inputs", s.handleListInputs)
	mux.HandleFunc("POST /groups/{group_id}/inputs/{stream_key}/pause", s.handlePauseInput)
	mux.HandleFunc("POST /groups/{group_id}/inputs/{stream_key}/resume", s.handleResumeInput)

	// M8: market-data query endpoints.
	mux.HandleFunc("GET /markets/{group_id}/trades", s.handleQueryTrades)
	mux.HandleFunc("GET /markets/{group_id}/klines", s.handleQueryKlines)
	mux.HandleFunc("GET /markets/{group_id}/orderbook/snapshots", s.handleQuerySnapshots)

	return mux
}

// --- group handlers ------------------------------------------------------

func (s *Server) handleCreateGroup(w http.ResponseWriter, r *http.Request) {
	var req createGroupRequest
	if err := decodeJSON(w, r, &req); err != nil {
		badRequest(w, err)
		return
	}
	if req.Exchange == "" || req.MarketType == "" || req.Symbol == "" {
		badRequest(w, fmt.Errorf("exchange, market_type and symbol are required"))
		return
	}
	if !model.IsValidMarketType(req.MarketType) {
		badRequest(w, fmt.Errorf("invalid market_type %q", req.MarketType))
		return
	}
	if req.DesiredStatus != "" && !model.IsValidDesiredStatus(req.DesiredStatus) {
		badRequest(w, fmt.Errorf("invalid desired_status %q", req.DesiredStatus))
		return
	}

	inputs := make([]model.GroupInput, 0, len(req.Inputs))
	for _, ir := range req.Inputs {
		in, err := ir.toModel()
		if err != nil {
			badRequest(w, err)
			return
		}
		inputs = append(inputs, in)
	}

	g := model.MarketGroup{
		Exchange:      req.Exchange,
		MarketType:    req.MarketType,
		Symbol:        req.Symbol,
		BaseAsset:     req.BaseAsset,
		QuoteAsset:    req.QuoteAsset,
		Weight:        req.Weight,
		DesiredStatus: req.DesiredStatus,
		Inputs:        inputs,
	}
	if err := s.store.CreateGroup(r.Context(), g); err != nil {
		s.writeStoreError(w, err)
		return
	}

	gid := model.NewGroupID(req.Exchange, req.MarketType, req.Symbol)
	resp, err := s.aggregateGroup(r.Context(), gid)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, resp)
}

func (s *Server) handleListGroups(w http.ResponseWriter, r *http.Request) {
	groups, err := s.store.ListGroups(r.Context())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	out := make([]groupResponse, 0, len(groups))
	for _, g := range groups {
		out = append(out, newGroupSummary(g))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleGetGroup(w http.ResponseWriter, r *http.Request) {
	gid := r.PathValue("group_id")
	resp, err := s.aggregateGroup(r.Context(), gid)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handlePauseGroup(w http.ResponseWriter, r *http.Request) {
	s.setGroupStatus(w, r, model.DesiredStatusPaused)
}

func (s *Server) handleResumeGroup(w http.ResponseWriter, r *http.Request) {
	s.setGroupStatus(w, r, model.DesiredStatusRunning)
}

func (s *Server) setGroupStatus(w http.ResponseWriter, r *http.Request, status string) {
	gid := r.PathValue("group_id")
	if err := s.store.UpdateGroupDesiredStatus(r.Context(), gid, status); err != nil {
		s.writeStoreError(w, err)
		return
	}
	resp, err := s.aggregateGroup(r.Context(), gid)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// --- input handlers ------------------------------------------------------

func (s *Server) handleCreateInput(w http.ResponseWriter, r *http.Request) {
	gid := r.PathValue("group_id")
	var req inputRequest
	if err := decodeJSON(w, r, &req); err != nil {
		badRequest(w, err)
		return
	}
	in, err := req.toModel()
	if err != nil {
		badRequest(w, err)
		return
	}
	if err := s.store.AddInputs(r.Context(), gid, []model.GroupInput{in}); err != nil {
		s.writeStoreError(w, err)
		return
	}

	// Re-read so the response reflects the stored row (derived ids, defaults).
	inputs, err := s.store.ListGroupInputs(r.Context(), gid)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	for _, stored := range inputs {
		if stored.StreamKey == in.StreamKey {
			writeJSON(w, http.StatusCreated, newInputResponse(stored, nil))
			return
		}
	}
	// Should not happen, but never lie about success.
	writeJSON(w, http.StatusCreated, newInputResponse(in, nil))
}

func (s *Server) handleListInputs(w http.ResponseWriter, r *http.Request) {
	gid := r.PathValue("group_id")
	if _, err := s.store.GetGroup(r.Context(), gid); err != nil {
		s.writeStoreError(w, err)
		return
	}
	inputs, err := s.store.ListGroupInputs(r.Context(), gid)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	statusByInput, err := s.statusMap(r.Context(), gid)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	out := make([]inputResponse, 0, len(inputs))
	for _, in := range inputs {
		out = append(out, newInputResponse(in, lookupStatus(statusByInput, in.InputID)))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handlePauseInput(w http.ResponseWriter, r *http.Request) {
	s.setInputStatus(w, r, model.DesiredStatusPaused)
}

func (s *Server) handleResumeInput(w http.ResponseWriter, r *http.Request) {
	s.setInputStatus(w, r, model.DesiredStatusRunning)
}

func (s *Server) setInputStatus(w http.ResponseWriter, r *http.Request, status string) {
	gid := r.PathValue("group_id")
	streamKey := r.PathValue("stream_key")
	if err := s.store.UpdateInputDesiredStatus(r.Context(), gid, streamKey, status); err != nil {
		s.writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"group_id":       gid,
		"stream_key":     streamKey,
		"desired_status": status,
	})
}

// --- aggregation ---------------------------------------------------------

// aggregateGroup builds the merged group view (group + lease + inputs + runtime
// status). A missing group surfaces metadata.ErrNotFound.
func (s *Server) aggregateGroup(ctx context.Context, groupID string) (groupResponse, error) {
	g, err := s.store.GetGroup(ctx, groupID)
	if err != nil {
		return groupResponse{}, err
	}
	inputs, err := s.store.ListGroupInputs(ctx, groupID)
	if err != nil {
		return groupResponse{}, err
	}
	lease, err := s.store.GetGroupLease(ctx, groupID)
	if err != nil {
		return groupResponse{}, err
	}
	statusByInput, err := s.statusMap(ctx, groupID)
	if err != nil {
		return groupResponse{}, err
	}

	resp := newGroupSummary(g)
	resp.Lease = newLeaseResponse(lease, time.Now())
	resp.Inputs = make([]inputResponse, 0, len(inputs))
	for _, in := range inputs {
		resp.Inputs = append(resp.Inputs, newInputResponse(in, lookupStatus(statusByInput, in.InputID)))
	}
	return resp, nil
}

func (s *Server) statusMap(ctx context.Context, groupID string) (map[string]model.StreamRuntimeStatus, error) {
	statuses, err := s.store.ListStreamRuntimeStatus(ctx, groupID)
	if err != nil {
		return nil, err
	}
	m := make(map[string]model.StreamRuntimeStatus, len(statuses))
	for _, st := range statuses {
		m[st.InputID] = st
	}
	return m, nil
}

func lookupStatus(m map[string]model.StreamRuntimeStatus, inputID string) *model.StreamRuntimeStatus {
	if st, ok := m[inputID]; ok {
		return &st
	}
	return nil
}

// --- helpers -------------------------------------------------------------

func decodeJSON(w http.ResponseWriter, r *http.Request, v any) error {
	// Bound the body and reject unknown / trailing tokens so the management API
	// fails loudly on malformed requests instead of silently ignoring them.
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1 MiB
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("invalid JSON body: %w", err)
	}
	if dec.More() {
		return fmt.Errorf("invalid JSON body: unexpected trailing data")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func badRequest(w http.ResponseWriter, err error) {
	writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
}

// writeStoreError maps domain errors to HTTP status codes; anything unexpected
// is logged and reported as 500.
func (s *Server) writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, metadata.ErrConflict):
		writeJSON(w, http.StatusConflict, errorResponse{Error: err.Error()})
	case errors.Is(err, metadata.ErrNotFound):
		writeJSON(w, http.StatusNotFound, errorResponse{Error: err.Error()})
	default:
		s.logger.Error("control-plane store error", "err", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "internal error"})
	}
}

// compile-time assertion that the concrete store satisfies the narrow Store.
var _ Store = (*metadata.PostgresStore)(nil)

// --- M8 query handlers ----------------------------------------------------

// queryRequest holds the common query-string parameters for all market-data
// query endpoints.
type queryRequest struct {
	From   string // RFC3339, required
	To     string // RFC3339, required
	Limit  int    // optional, default 200, max 1000
	Cursor string // RFC3339, optional (exclusive lower bound for next page)
}

func parseQueryParams(r *http.Request) (queryRequest, error) {
	q := r.URL.Query()
	req := queryRequest{
		From:   q.Get("from"),
		To:     q.Get("to"),
		Cursor: q.Get("cursor"),
	}
	if req.From == "" || req.To == "" {
		return req, fmt.Errorf("from and to are required (RFC3339)")
	}
	if limitStr := q.Get("limit"); limitStr != "" {
		v, err := strconv.Atoi(limitStr)
		if err != nil || v < 0 {
			return req, fmt.Errorf("limit must be a non-negative integer")
		}
		req.Limit = v
	}
	return req, nil
}

func parseTimeRange(from, to string) (time.Time, time.Time, error) {
	f, err := time.Parse(time.RFC3339, from)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("from: %w", err)
	}
	t, err := time.Parse(time.RFC3339, to)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("to: %w", err)
	}
	if !t.After(f) {
		return time.Time{}, time.Time{}, fmt.Errorf("to must be after from")
	}
	return f, t, nil
}

func parseCursor(cursor string) (*time.Time, error) {
	if cursor == "" {
		return nil, nil
	}
	c, err := time.Parse(time.RFC3339, cursor)
	if err != nil {
		return nil, fmt.Errorf("cursor: %w", err)
	}
	return &c, nil
}

type queryResponse struct {
	Rows       any    `json:"rows"`
	NextCursor string `json:"next_cursor,omitempty"`
}

func (s *Server) handleQueryTrades(w http.ResponseWriter, r *http.Request) {
	s.handleQuery(w, r,
		func(ctx context.Context, gid string, from, to time.Time,
			limit int, cursor *time.Time,
		) (any, *time.Time, error) {
			if s.query == nil {
				return nil, nil, fmt.Errorf("query store is not configured")
			}
			result, err := s.query.QueryTrades(ctx, gid, from, to, limit, cursor)
			return result.Rows, result.NextCursor, err
		})
}

func (s *Server) handleQueryKlines(w http.ResponseWriter, r *http.Request) {
	s.handleQuery(w, r,
		func(ctx context.Context, gid string, from, to time.Time,
			limit int, cursor *time.Time,
		) (any, *time.Time, error) {
			if s.query == nil {
				return nil, nil, fmt.Errorf("query store is not configured")
			}
			result, err := s.query.QueryKlines(ctx, gid, from, to, limit, cursor)
			return result.Rows, result.NextCursor, err
		})
}

func (s *Server) handleQuerySnapshots(w http.ResponseWriter, r *http.Request) {
	s.handleQuery(w, r,
		func(ctx context.Context, gid string, from, to time.Time,
			limit int, cursor *time.Time,
		) (any, *time.Time, error) {
			if s.query == nil {
				return nil, nil, fmt.Errorf("query store is not configured")
			}
			result, err := s.query.QuerySnapshots(ctx, gid, from, to, limit, cursor)
			return result.Rows, result.NextCursor, err
		})
}

type queryFn func(
	ctx context.Context, gid string, from, to time.Time,
	limit int, cursor *time.Time,
) (rows any, nextCursor *time.Time, err error)

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request, fn queryFn) {
	gid := r.PathValue("group_id")
	if gid == "" {
		badRequest(w, fmt.Errorf("group_id is required"))
		return
	}

	params, err := parseQueryParams(r)
	if err != nil {
		badRequest(w, err)
		return
	}
	from, to, err := parseTimeRange(params.From, params.To)
	if err != nil {
		badRequest(w, err)
		return
	}
	cursor, err := parseCursor(params.Cursor)
	if err != nil {
		badRequest(w, err)
		return
	}

	// Verify the group exists before hitting the storage layer.
	if s.store != nil {
		if _, err := s.store.GetGroup(r.Context(), gid); err != nil {
			s.writeStoreError(w, err)
			return
		}
	}

	rows, nextCursor, err := fn(r.Context(), gid, from, to, params.Limit, cursor)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	resp := queryResponse{Rows: rows}
	if nextCursor != nil {
		resp.NextCursor = nextCursor.Format(time.RFC3339)
	}
	writeJSON(w, http.StatusOK, resp)
}
