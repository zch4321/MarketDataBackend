package metadata

import (
	"context"
	"time"

	"MarketDataBackend/internal/model"
)

// Store is the control-plane metadata interface (market groups, inputs,
// runtime nodes, leases and observed stream status).
type Store interface {
	CreateGroup(ctx context.Context, g model.MarketGroup) error
	UpdateGroupDesiredStatus(ctx context.Context, groupID string, status string) error
	ListRunnableGroups(ctx context.Context) ([]model.MarketGroup, error)
	ListGroups(ctx context.Context) ([]model.MarketGroup, error)
	GetGroup(ctx context.Context, groupID string) (model.MarketGroup, error)

	RegisterRuntimeNode(ctx context.Context, node model.RuntimeNode) error
	HeartbeatRuntimeNode(ctx context.Context, nodeID string, capacity model.RuntimeCapacity) error

	TryAcquireGroupLease(ctx context.Context, groupID string, nodeID string, ttl time.Duration) (bool, error)
	RenewGroupLease(ctx context.Context, groupID string, nodeID string, ttl time.Duration) (bool, error)
	ReleaseGroupLease(ctx context.Context, groupID string, nodeID string) error
	GetGroupLease(ctx context.Context, groupID string) (*model.GroupLease, error)

	AddInputs(ctx context.Context, groupID string, inputs []model.GroupInput) error
	ListGroupInputs(ctx context.Context, groupID string) ([]model.GroupInput, error)
	UpdateInputDesiredStatus(ctx context.Context, groupID string, streamKey string, status string) error

	ReportStreamRuntimeStatus(ctx context.Context, status model.StreamRuntimeStatus) error
	ListStreamRuntimeStatus(ctx context.Context, groupID string) ([]model.StreamRuntimeStatus, error)
}
