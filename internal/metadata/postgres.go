// Package metadata defines the control-plane metadata interface and its
// PostgreSQL implementation (market groups, inputs, runtime nodes, leases and
// observed stream status). The Store contract is fixed in M1; PostgresStore
// lands in M2.
package metadata

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"MarketDataBackend/internal/model"
)

var (
	// ErrConflict is returned when a uniqueness constraint is violated, e.g.
	// creating a group whose (exchange, market_type, symbol) already exists.
	ErrConflict = errors.New("metadata: conflict")
	// ErrNotFound is returned when an update targets a row that does not exist.
	ErrNotFound = errors.New("metadata: not found")
)

// PostgresStore is the PostgreSQL-backed implementation of Store. The pool's
// lifecycle is owned by the caller.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// Compile-time assertion that PostgresStore satisfies the Store interface.
var _ Store = (*PostgresStore)(nil)

// NewPostgresStore wraps an existing pgx pool.
func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

// scanner is satisfied by both pgx.Row and pgx.Rows.
type scanner interface {
	Scan(dest ...any) error
}

// execer is satisfied by both *pgxpool.Pool and pgx.Tx so write helpers work
// inside or outside a transaction.
type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// insertGroupInput inserts one already-normalized input. A duplicate
// (group_id, stream_key) or input_id returns ErrConflict; a missing parent
// group surfaces as a foreign-key violation wrapped in ErrNotFound.
func insertGroupInput(ctx context.Context, q execer, in model.GroupInput) error {
	_, err := q.Exec(ctx,
		`INSERT INTO group_inputs
		   (input_id, group_id, stream_key, stream_kind, interval, enabled,
		    kafka_cluster, kafka_topic, kafka_group_id, desired_status, schema_version)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		in.InputID, in.GroupID, in.StreamKey, in.StreamKind, nullIfEmpty(in.Interval),
		in.Enabled, in.KafkaCluster, in.KafkaTopic, in.KafkaGroupID,
		in.DesiredStatus, in.SchemaVersion)
	switch {
	case isUniqueViolation(err):
		return fmt.Errorf("%w: input %s already exists", ErrConflict, in.StreamKey)
	case isForeignKeyViolation(err):
		return fmt.Errorf("%w: group %s", ErrNotFound, in.GroupID)
	case err != nil:
		return fmt.Errorf("metadata: insert input %s: %w", in.StreamKey, err)
	}
	return nil
}

// CreateGroup inserts a market group and all of its inputs in a single
// transaction. Missing ids / statuses are derived so callers can pass a partial
// MarketGroup. A duplicate (exchange, market_type, symbol) or input returns
// ErrConflict.
func (s *PostgresStore) CreateGroup(ctx context.Context, g model.MarketGroup) error {
	if g.Exchange == "" || g.MarketType == "" || g.Symbol == "" {
		return fmt.Errorf("metadata: exchange, market_type and symbol are required")
	}
	if !model.IsValidMarketType(g.MarketType) {
		return fmt.Errorf("metadata: invalid market_type %q", g.MarketType)
	}
	g, err := normalizeGroup(g)
	if err != nil {
		return err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("metadata: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx,
		`INSERT INTO market_groups
		   (group_id, exchange, market_type, symbol, base_asset, quote_asset, desired_status, weight)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		g.GroupID, g.Exchange, g.MarketType, g.Symbol,
		nullIfEmpty(g.BaseAsset), nullIfEmpty(g.QuoteAsset), g.DesiredStatus, g.Weight,
	); err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("%w: market group %s/%s/%s already exists",
				ErrConflict, g.Exchange, g.MarketType, g.Symbol)
		}
		return fmt.Errorf("metadata: insert group: %w", err)
	}

	for _, in := range g.Inputs {
		if err := insertGroupInput(ctx, tx, in); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("metadata: commit: %w", err)
	}
	return nil
}

// UpdateGroupDesiredStatus sets desired_status (and bumps updated_at) for one
// group. An unknown group returns ErrNotFound; an invalid status is rejected
// before touching the database.
func (s *PostgresStore) UpdateGroupDesiredStatus(ctx context.Context, groupID, status string) error {
	if !model.IsValidDesiredStatus(status) {
		return fmt.Errorf("metadata: invalid desired_status %q", status)
	}
	ct, err := s.pool.Exec(ctx,
		`UPDATE market_groups SET desired_status = $2, updated_at = now() WHERE group_id = $1`,
		groupID, status)
	if err != nil {
		return fmt.Errorf("metadata: update group status: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return fmt.Errorf("%w: group %s", ErrNotFound, groupID)
	}
	return nil
}

// ListRunnableGroups returns every group whose desired_status is running. Inputs
// are not populated; callers fetch them with ListGroupInputs when needed.
func (s *PostgresStore) ListRunnableGroups(ctx context.Context) ([]model.MarketGroup, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT group_id, exchange, market_type, symbol, base_asset, quote_asset,
		        desired_status, weight, created_at, updated_at
		   FROM market_groups
		  WHERE desired_status = $1
		  ORDER BY group_id`, model.DesiredStatusRunning)
	if err != nil {
		return nil, fmt.Errorf("metadata: list runnable groups: %w", err)
	}
	defer rows.Close()

	var out []model.MarketGroup
	for rows.Next() {
		g, err := scanGroup(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

// ListGroups returns every market group regardless of desired_status, ordered by
// group_id. Inputs are not populated.
func (s *PostgresStore) ListGroups(ctx context.Context) ([]model.MarketGroup, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT group_id, exchange, market_type, symbol, base_asset, quote_asset,
		        desired_status, weight, created_at, updated_at
		   FROM market_groups
		  ORDER BY group_id`)
	if err != nil {
		return nil, fmt.Errorf("metadata: list groups: %w", err)
	}
	defer rows.Close()

	var out []model.MarketGroup
	for rows.Next() {
		g, err := scanGroup(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

// GetGroup returns a single market group by id. An unknown group returns
// ErrNotFound. Inputs are not populated; callers use ListGroupInputs.
func (s *PostgresStore) GetGroup(ctx context.Context, groupID string) (model.MarketGroup, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT group_id, exchange, market_type, symbol, base_asset, quote_asset,
		        desired_status, weight, created_at, updated_at
		   FROM market_groups
		  WHERE group_id = $1`, groupID)
	g, err := scanGroup(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.MarketGroup{}, fmt.Errorf("%w: group %s", ErrNotFound, groupID)
	}
	return g, err
}
func (s *PostgresStore) ListGroupInputs(ctx context.Context, groupID string) ([]model.GroupInput, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT input_id, group_id, stream_key, stream_kind, interval, enabled,
		        kafka_cluster, kafka_topic, kafka_group_id, desired_status, schema_version,
		        created_at, updated_at
		   FROM group_inputs
		  WHERE group_id = $1
		  ORDER BY stream_key`, groupID)
	if err != nil {
		return nil, fmt.Errorf("metadata: list group inputs: %w", err)
	}
	defer rows.Close()

	var out []model.GroupInput
	for rows.Next() {
		in, err := scanInput(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, in)
	}
	return out, rows.Err()
}

// AddInputs inserts one or more inputs into an existing group in a single
// transaction. Inputs are normalized (ids, stream_key/kind/interval, defaults)
// like CreateGroup. A duplicate stream_key returns ErrConflict; an unknown
// group returns ErrNotFound.
func (s *PostgresStore) AddInputs(ctx context.Context, groupID string, inputs []model.GroupInput) error {
	if len(inputs) == 0 {
		return nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("metadata: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	for _, in := range inputs {
		ni, err := normalizeInput(groupID, in)
		if err != nil {
			return err
		}
		if err := insertGroupInput(ctx, tx, ni); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("metadata: commit: %w", err)
	}
	return nil
}

// UpdateInputDesiredStatus sets desired_status (and bumps updated_at) for one
// input identified by (group_id, stream_key). An unknown input returns
// ErrNotFound; an invalid status is rejected before touching the database.
func (s *PostgresStore) UpdateInputDesiredStatus(ctx context.Context, groupID, streamKey, status string) error {
	if !model.IsValidDesiredStatus(status) {
		return fmt.Errorf("metadata: invalid desired_status %q", status)
	}
	ct, err := s.pool.Exec(ctx,
		`UPDATE group_inputs SET desired_status = $3, updated_at = now()
		  WHERE group_id = $1 AND stream_key = $2`,
		groupID, streamKey, status)
	if err != nil {
		return fmt.Errorf("metadata: update input status: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return fmt.Errorf("%w: input %s/%s", ErrNotFound, groupID, streamKey)
	}
	return nil
}

// RegisterRuntimeNode upserts a runtime node. It is safe to call repeatedly;
// every field except created_at is refreshed.
func (s *PostgresStore) RegisterRuntimeNode(ctx context.Context, node model.RuntimeNode) error {
	if node.Status == "" {
		node.Status = model.NodeStatusAlive
	}
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO runtime_nodes
		   (node_id, hostname, pod_name, status, max_groups, current_groups,
		    max_weight, current_weight, last_heartbeat_at, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now(), now(), now())
		 ON CONFLICT (node_id) DO UPDATE SET
		    hostname          = EXCLUDED.hostname,
		    pod_name          = EXCLUDED.pod_name,
		    status            = EXCLUDED.status,
		    max_groups        = EXCLUDED.max_groups,
		    current_groups    = EXCLUDED.current_groups,
		    max_weight        = EXCLUDED.max_weight,
		    current_weight    = EXCLUDED.current_weight,
		    last_heartbeat_at = now(),
		    updated_at        = now()`,
		node.NodeID, node.Hostname, nullIfEmpty(node.PodName), node.Status,
		node.MaxGroups, node.CurrentGroups, node.MaxWeight, node.CurrentWeight,
	); err != nil {
		return fmt.Errorf("metadata: register runtime node: %w", err)
	}
	return nil
}

// HeartbeatRuntimeNode refreshes last_heartbeat_at and the mutable capacity of a
// previously registered node. An unknown node returns ErrNotFound.
func (s *PostgresStore) HeartbeatRuntimeNode(ctx context.Context, nodeID string, capacity model.RuntimeCapacity) error {
	ct, err := s.pool.Exec(ctx,
		`UPDATE runtime_nodes
		    SET current_groups = $2, current_weight = $3,
		        last_heartbeat_at = now(), updated_at = now()
		  WHERE node_id = $1`,
		nodeID, capacity.CurrentGroups, capacity.CurrentWeight)
	if err != nil {
		return fmt.Errorf("metadata: heartbeat runtime node: %w", err)
	}
	if ct.RowsAffected() == 0 {
		return fmt.Errorf("%w: runtime node %s", ErrNotFound, nodeID)
	}
	return nil
}

// TryAcquireGroupLease atomically grabs the lease for a group when it is free,
// expired, or already owned by nodeID (in which case it is extended). It returns
// false (without error) when another node holds a still-valid lease. The
// expiry is computed from the database clock so node clock skew is irrelevant.
func (s *PostgresStore) TryAcquireGroupLease(ctx context.Context, groupID, nodeID string, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		return false, fmt.Errorf("metadata: lease ttl must be > 0")
	}
	var owner string
	err := s.pool.QueryRow(ctx,
		`INSERT INTO market_leases (group_id, node_id, lease_expires_at, version, acquired_at, updated_at)
		 VALUES ($1, $2, now() + $3::double precision * interval '1 second', 1, now(), now())
		 ON CONFLICT (group_id) DO UPDATE SET
		    node_id          = EXCLUDED.node_id,
		    lease_expires_at = EXCLUDED.lease_expires_at,
		    version          = market_leases.version + 1,
		    acquired_at      = CASE WHEN market_leases.node_id = EXCLUDED.node_id
		                            THEN market_leases.acquired_at ELSE now() END,
		    updated_at       = now()
		 WHERE market_leases.lease_expires_at < now()
		    OR market_leases.node_id = EXCLUDED.node_id
		 RETURNING node_id`,
		groupID, nodeID, ttl.Seconds()).Scan(&owner)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// Conflict row exists, owned by someone else and still valid.
		return false, nil
	case isForeignKeyViolation(err):
		return false, fmt.Errorf("%w: group %s", ErrNotFound, groupID)
	case err != nil:
		return false, fmt.Errorf("metadata: acquire lease: %w", err)
	}
	return owner == nodeID, nil
}

// RenewGroupLease extends the lease only when nodeID is still the recorded
// owner AND the lease has not already expired. Renewing an expired lease is
// rejected (returns false) so a stalled owner cannot silently resurrect a lease
// that another node is entitled to take over.
func (s *PostgresStore) RenewGroupLease(ctx context.Context, groupID, nodeID string, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		return false, fmt.Errorf("metadata: lease ttl must be > 0")
	}
	var owner string
	err := s.pool.QueryRow(ctx,
		`UPDATE market_leases
		    SET lease_expires_at = now() + $3::double precision * interval '1 second',
		        version          = version + 1,
		        updated_at       = now()
		  WHERE group_id = $1 AND node_id = $2 AND lease_expires_at >= now()
		  RETURNING node_id`,
		groupID, nodeID, ttl.Seconds()).Scan(&owner)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("metadata: renew lease: %w", err)
	}
	return true, nil
}

// ReleaseGroupLease drops the lease only when nodeID owns it. Releasing a lease
// owned by another node (or an absent lease) is a no-op, not an error.
func (s *PostgresStore) ReleaseGroupLease(ctx context.Context, groupID, nodeID string) error {
	if _, err := s.pool.Exec(ctx,
		`DELETE FROM market_leases WHERE group_id = $1 AND node_id = $2`,
		groupID, nodeID); err != nil {
		return fmt.Errorf("metadata: release lease: %w", err)
	}
	return nil
}

// GetGroupLease returns the current lease for a group, or (nil, nil) when the
// group has no lease. The lease may already be expired; callers compare
// lease_expires_at against now to decide.
func (s *PostgresStore) GetGroupLease(ctx context.Context, groupID string) (*model.GroupLease, error) {
	var l model.GroupLease
	err := s.pool.QueryRow(ctx,
		`SELECT group_id, node_id, lease_expires_at, version, acquired_at, updated_at
		   FROM market_leases WHERE group_id = $1`, groupID).
		Scan(&l.GroupID, &l.NodeID, &l.LeaseExpiresAt, &l.Version, &l.AcquiredAt, &l.UpdatedAt)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("metadata: get lease: %w", err)
	}
	return &l, nil
}

// ReportStreamRuntimeStatus upserts the observed runtime state of a single
// input stream keyed by input_id. The first call inserts; later calls overwrite
// lag / offsets / last_error / timestamps.
func (s *PostgresStore) ReportStreamRuntimeStatus(ctx context.Context, st model.StreamRuntimeStatus) error {
	if st.ActualStatus == "" {
		st.ActualStatus = model.ActualStatusPending
	}
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO stream_runtime_status
		   (input_id, group_id, stream_key, node_id, actual_status,
		    kafka_partition, kafka_lag, committed_offset, high_watermark_offset,
		    last_event_time, last_processed_time, last_error, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, now())
		 ON CONFLICT (input_id) DO UPDATE SET
		    group_id              = EXCLUDED.group_id,
		    stream_key            = EXCLUDED.stream_key,
		    node_id               = EXCLUDED.node_id,
		    actual_status         = EXCLUDED.actual_status,
		    kafka_partition       = EXCLUDED.kafka_partition,
		    kafka_lag             = EXCLUDED.kafka_lag,
		    committed_offset      = EXCLUDED.committed_offset,
		    high_watermark_offset = EXCLUDED.high_watermark_offset,
		    last_event_time       = EXCLUDED.last_event_time,
		    last_processed_time   = EXCLUDED.last_processed_time,
		    last_error            = EXCLUDED.last_error,
		    updated_at            = now()`,
		st.InputID, st.GroupID, st.StreamKey, nullIfEmpty(st.NodeID), st.ActualStatus,
		st.KafkaPartition, st.KafkaLag, st.CommittedOffset, st.HighWatermarkOffset,
		st.LastEventTime, st.LastProcessedTime, nullIfEmpty(st.LastError),
	); err != nil {
		return fmt.Errorf("metadata: report stream status: %w", err)
	}
	return nil
}

// ListStreamRuntimeStatus returns the observed runtime status of every input in
// a group, keyed by input_id, ordered by stream_key. Inputs that have never
// reported simply have no row here; the API layer fills in a pending default.
func (s *PostgresStore) ListStreamRuntimeStatus(ctx context.Context, groupID string) ([]model.StreamRuntimeStatus, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT input_id, group_id, stream_key, node_id, actual_status,
		        kafka_partition, kafka_lag, committed_offset, high_watermark_offset,
		        last_event_time, last_processed_time, last_error, updated_at
		   FROM stream_runtime_status
		  WHERE group_id = $1
		  ORDER BY stream_key`, groupID)
	if err != nil {
		return nil, fmt.Errorf("metadata: list stream status: %w", err)
	}
	defer rows.Close()

	var out []model.StreamRuntimeStatus
	for rows.Next() {
		st, err := scanStreamStatus(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

// normalizeGroup fills derivable defaults so callers can pass a partial group:
// ids, desired_status, weight, and (per input) the stream triple, kafka_cluster,
// kafka_group_id and schema_version. It returns an error when an input is
// internally inconsistent (see normalizeInput) or desired_status is invalid.
func normalizeGroup(g model.MarketGroup) (model.MarketGroup, error) {
	if g.GroupID == "" {
		g.GroupID = model.NewGroupID(g.Exchange, g.MarketType, g.Symbol)
	}
	if g.DesiredStatus == "" {
		g.DesiredStatus = model.DesiredStatusRunning
	} else if !model.IsValidDesiredStatus(g.DesiredStatus) {
		return model.MarketGroup{}, fmt.Errorf("metadata: invalid desired_status %q", g.DesiredStatus)
	}
	if g.Weight <= 0 {
		g.Weight = 1
	}

	inputs := make([]model.GroupInput, len(g.Inputs))
	for i, in := range g.Inputs {
		ni, err := normalizeInput(g.GroupID, in)
		if err != nil {
			return model.MarketGroup{}, err
		}
		inputs[i] = ni
	}
	g.Inputs = inputs
	return g, nil
}

// normalizeInput fills derivable defaults for a single input and validates that
// the stream triple is internally consistent. It returns an error (rather than
// silently swallowing one) when:
//   - neither stream_key nor stream_kind is supplied;
//   - stream_kind is unknown;
//   - the interval rules are violated (kline needs one, the others must not);
//   - a supplied stream_key contradicts the derived stream_kind/interval; or
//   - kafka_topic is empty.
//
// kafka_group_id defaults to model.DefaultKafkaGroupID when omitted.
func normalizeInput(groupID string, in model.GroupInput) (model.GroupInput, error) {
	in.GroupID = groupID

	switch {
	case in.StreamKind != "":
		if !model.IsValidStreamKind(in.StreamKind) {
			return model.GroupInput{}, fmt.Errorf("metadata: invalid stream_kind %q", in.StreamKind)
		}
		key, err := model.StreamKeyFor(in.StreamKind, in.Interval)
		if err != nil {
			return model.GroupInput{}, fmt.Errorf("metadata: %w", err)
		}
		if in.StreamKey != "" && in.StreamKey != key {
			return model.GroupInput{}, fmt.Errorf(
				"metadata: stream_key %q inconsistent with stream_kind/interval (want %q)", in.StreamKey, key)
		}
		in.StreamKey = key
	case in.StreamKey != "":
		kind, interval, err := model.ParseStreamKey(in.StreamKey)
		if err != nil {
			return model.GroupInput{}, fmt.Errorf("metadata: %w", err)
		}
		if in.Interval != "" && in.Interval != interval {
			return model.GroupInput{}, fmt.Errorf(
				"metadata: interval %q inconsistent with stream_key %q", in.Interval, in.StreamKey)
		}
		in.StreamKind = kind
		in.Interval = interval
	default:
		return model.GroupInput{}, fmt.Errorf("metadata: input requires stream_key or stream_kind")
	}

	if in.KafkaTopic == "" {
		return model.GroupInput{}, fmt.Errorf("metadata: input %s requires kafka_topic", in.StreamKey)
	}
	if in.InputID == "" {
		in.InputID = model.NewInputID(groupID, in.StreamKey)
	}
	if in.DesiredStatus == "" {
		in.DesiredStatus = model.DesiredStatusRunning
	} else if !model.IsValidDesiredStatus(in.DesiredStatus) {
		return model.GroupInput{}, fmt.Errorf("metadata: invalid desired_status %q", in.DesiredStatus)
	}
	if in.KafkaCluster == "" {
		in.KafkaCluster = "default"
	}
	if in.KafkaGroupID == "" {
		in.KafkaGroupID = model.DefaultKafkaGroupID(groupID, in.StreamKey)
	}
	if in.SchemaVersion <= 0 {
		in.SchemaVersion = 1
	}
	return in, nil
}

func scanGroup(row scanner) (model.MarketGroup, error) {
	var (
		g           model.MarketGroup
		base, quote *string
	)
	if err := row.Scan(&g.GroupID, &g.Exchange, &g.MarketType, &g.Symbol,
		&base, &quote, &g.DesiredStatus, &g.Weight, &g.CreatedAt, &g.UpdatedAt); err != nil {
		return model.MarketGroup{}, fmt.Errorf("metadata: scan group: %w", err)
	}
	g.BaseAsset = derefString(base)
	g.QuoteAsset = derefString(quote)
	return g, nil
}

func scanInput(row scanner) (model.GroupInput, error) {
	var (
		in       model.GroupInput
		interval *string
	)
	if err := row.Scan(&in.InputID, &in.GroupID, &in.StreamKey, &in.StreamKind, &interval,
		&in.Enabled, &in.KafkaCluster, &in.KafkaTopic, &in.KafkaGroupID, &in.DesiredStatus,
		&in.SchemaVersion, &in.CreatedAt, &in.UpdatedAt); err != nil {
		return model.GroupInput{}, fmt.Errorf("metadata: scan input: %w", err)
	}
	in.Interval = derefString(interval)
	return in, nil
}

func scanStreamStatus(row scanner) (model.StreamRuntimeStatus, error) {
	var (
		st              model.StreamRuntimeStatus
		nodeID, lastErr *string
	)
	if err := row.Scan(&st.InputID, &st.GroupID, &st.StreamKey, &nodeID, &st.ActualStatus,
		&st.KafkaPartition, &st.KafkaLag, &st.CommittedOffset, &st.HighWatermarkOffset,
		&st.LastEventTime, &st.LastProcessedTime, &lastErr, &st.UpdatedAt); err != nil {
		return model.StreamRuntimeStatus{}, fmt.Errorf("metadata: scan stream status: %w", err)
	}
	st.NodeID = derefString(nodeID)
	st.LastError = derefString(lastErr)
	return st, nil
}

func nullIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func derefString(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func isForeignKeyViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23503"
}
