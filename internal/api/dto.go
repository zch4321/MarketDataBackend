package api

import (
	"fmt"
	"time"

	"MarketDataBackend/internal/model"
)

// --- request payloads ----------------------------------------------------

// createGroupRequest is the body of POST /groups.
type createGroupRequest struct {
	Exchange      string         `json:"exchange"`
	MarketType    string         `json:"market_type"`
	Symbol        string         `json:"symbol"`
	BaseAsset     string         `json:"base_asset"`
	QuoteAsset    string         `json:"quote_asset"`
	Weight        int            `json:"weight"`
	DesiredStatus string         `json:"desired_status"`
	Inputs        []inputRequest `json:"inputs"`
}

// inputRequest is the body of POST /groups/{group_id}/inputs and an element of
// createGroupRequest.Inputs. Callers supply either stream_key or
// stream_kind(+interval); the missing form is derived and, when both are given,
// they must agree. kafka_group_id may be omitted (a stable default is derived).
type inputRequest struct {
	StreamKey     string `json:"stream_key"`
	StreamKind    string `json:"stream_kind"`
	Interval      string `json:"interval"`
	KafkaTopic    string `json:"kafka_topic"`
	KafkaGroupID  string `json:"kafka_group_id"`
	KafkaCluster  string `json:"kafka_cluster"`
	DesiredStatus string `json:"desired_status"`
}

// toModel validates the request and converts it to a model.GroupInput. A
// validation failure is surfaced by the caller as HTTP 400.
func (r inputRequest) toModel() (model.GroupInput, error) {
	if r.KafkaTopic == "" {
		return model.GroupInput{}, fmt.Errorf("kafka_topic is required")
	}
	if r.DesiredStatus != "" && !model.IsValidDesiredStatus(r.DesiredStatus) {
		return model.GroupInput{}, fmt.Errorf("invalid desired_status %q", r.DesiredStatus)
	}

	kind, interval, streamKey := r.StreamKind, r.Interval, r.StreamKey
	switch {
	case kind != "":
		if !model.IsValidStreamKind(kind) {
			return model.GroupInput{}, fmt.Errorf("invalid stream_kind %q", kind)
		}
		// StreamKeyFor also enforces the interval rules (kline needs one, the
		// others must not carry one).
		key, err := model.StreamKeyFor(kind, interval)
		if err != nil {
			return model.GroupInput{}, err
		}
		if streamKey != "" && streamKey != key {
			return model.GroupInput{}, fmt.Errorf(
				"stream_key %q does not match stream_kind/interval (expected %q)", streamKey, key)
		}
		streamKey = key
	case streamKey != "":
		k, iv, err := model.ParseStreamKey(streamKey)
		if err != nil {
			return model.GroupInput{}, err
		}
		if interval != "" && interval != iv {
			return model.GroupInput{}, fmt.Errorf(
				"interval %q does not match stream_key %q", interval, streamKey)
		}
		kind, interval = k, iv
	default:
		return model.GroupInput{}, fmt.Errorf("stream_key or stream_kind is required")
	}

	return model.GroupInput{
		StreamKey:     streamKey,
		StreamKind:    kind,
		Interval:      interval,
		KafkaTopic:    r.KafkaTopic,
		KafkaGroupID:  r.KafkaGroupID,
		KafkaCluster:  r.KafkaCluster,
		DesiredStatus: r.DesiredStatus,
		Enabled:       true,
	}, nil
}

// --- response payloads ---------------------------------------------------

type errorResponse struct {
	Error string `json:"error"`
}

// groupResponse merges market_groups (desired_status), market_leases (owner) and
// per-input stream_runtime_status into a single view.
type groupResponse struct {
	GroupID       string          `json:"group_id"`
	Exchange      string          `json:"exchange"`
	MarketType    string          `json:"market_type"`
	Symbol        string          `json:"symbol"`
	BaseAsset     string          `json:"base_asset,omitempty"`
	QuoteAsset    string          `json:"quote_asset,omitempty"`
	DesiredStatus string          `json:"desired_status"`
	Weight        int             `json:"weight"`
	Lease         *leaseResponse  `json:"lease,omitempty"`
	Inputs        []inputResponse `json:"inputs,omitempty"`
	CreatedAt     time.Time       `json:"created_at"`
	UpdatedAt     time.Time       `json:"updated_at"`
}

type leaseResponse struct {
	NodeID         string    `json:"node_id"`
	LeaseExpiresAt time.Time `json:"lease_expires_at"`
	Version        int64     `json:"version"`
	Active         bool      `json:"active"`
}

type inputResponse struct {
	InputID       string                 `json:"input_id"`
	StreamKey     string                 `json:"stream_key"`
	StreamKind    string                 `json:"stream_kind"`
	Interval      string                 `json:"interval,omitempty"`
	KafkaTopic    string                 `json:"kafka_topic"`
	KafkaGroupID  string                 `json:"kafka_group_id"`
	DesiredStatus string                 `json:"desired_status"`
	Runtime       *runtimeStatusResponse `json:"runtime,omitempty"`
}

type runtimeStatusResponse struct {
	ActualStatus    string     `json:"actual_status"`
	NodeID          string     `json:"node_id,omitempty"`
	KafkaLag        *int64     `json:"kafka_lag,omitempty"`
	CommittedOffset *int64     `json:"committed_offset,omitempty"`
	LastError       string     `json:"last_error,omitempty"`
	UpdatedAt       *time.Time `json:"updated_at,omitempty"`
}

// newGroupSummary maps a group's own columns; Lease/Inputs are attached by the
// aggregator.
func newGroupSummary(g model.MarketGroup) groupResponse {
	return groupResponse{
		GroupID:       g.GroupID,
		Exchange:      g.Exchange,
		MarketType:    g.MarketType,
		Symbol:        g.Symbol,
		BaseAsset:     g.BaseAsset,
		QuoteAsset:    g.QuoteAsset,
		DesiredStatus: g.DesiredStatus,
		Weight:        g.Weight,
		CreatedAt:     g.CreatedAt,
		UpdatedAt:     g.UpdatedAt,
	}
}

func newLeaseResponse(l *model.GroupLease, now time.Time) *leaseResponse {
	if l == nil {
		return nil
	}
	return &leaseResponse{
		NodeID:         l.NodeID,
		LeaseExpiresAt: l.LeaseExpiresAt,
		Version:        l.Version,
		Active:         l.LeaseExpiresAt.After(now),
	}
}

// newInputResponse maps an input and its observed status. When the runtime has
// never reported (st == nil) the input is reported as pending.
func newInputResponse(in model.GroupInput, st *model.StreamRuntimeStatus) inputResponse {
	resp := inputResponse{
		InputID:       in.InputID,
		StreamKey:     in.StreamKey,
		StreamKind:    in.StreamKind,
		Interval:      in.Interval,
		KafkaTopic:    in.KafkaTopic,
		KafkaGroupID:  in.KafkaGroupID,
		DesiredStatus: in.DesiredStatus,
	}
	if st == nil {
		resp.Runtime = &runtimeStatusResponse{ActualStatus: model.ActualStatusPending}
		return resp
	}
	updatedAt := st.UpdatedAt
	resp.Runtime = &runtimeStatusResponse{
		ActualStatus:    st.ActualStatus,
		NodeID:          st.NodeID,
		KafkaLag:        st.KafkaLag,
		CommittedOffset: st.CommittedOffset,
		LastError:       st.LastError,
		UpdatedAt:       &updatedAt,
	}
	return resp
}
