-- 000001_init_metadata.up.sql
-- Control-plane metadata tables: market groups, inputs, runtime nodes, leases
-- and observed stream runtime status.

CREATE TABLE market_groups
(
    group_id       text PRIMARY KEY,
    exchange       text        NOT NULL,
    market_type    text        NOT NULL,
    symbol         text        NOT NULL,
    base_asset     text,
    quote_asset    text,

    desired_status text        NOT NULL
        CHECK (desired_status IN ('running', 'paused', 'disabled')),

    weight         integer     NOT NULL DEFAULT 1,

    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now(),

    UNIQUE (exchange, market_type, symbol)
);

CREATE TABLE group_inputs
(
    input_id       text PRIMARY KEY,
    group_id       text        NOT NULL
        REFERENCES market_groups (group_id) ON DELETE CASCADE,

    stream_key     text        NOT NULL,
    stream_kind    text        NOT NULL
        CHECK (stream_kind IN ('trade', 'kline', 'orderbook_delta', 'orderbook_snapshot')),
    interval       text,

    enabled        boolean     NOT NULL DEFAULT true,

    kafka_cluster  text        NOT NULL DEFAULT 'default',
    kafka_topic    text        NOT NULL,
    kafka_group_id text        NOT NULL,

    desired_status text        NOT NULL
        CHECK (desired_status IN ('running', 'paused', 'disabled')),

    schema_version integer     NOT NULL DEFAULT 1,

    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now(),

    UNIQUE (group_id, stream_key)
);

CREATE INDEX group_inputs_group_id_idx ON group_inputs (group_id);

CREATE TABLE runtime_nodes
(
    node_id           text PRIMARY KEY,
    hostname          text        NOT NULL,
    pod_name          text,
    status            text        NOT NULL
        CHECK (status IN ('alive', 'draining', 'dead')),

    max_groups        integer     NOT NULL DEFAULT 0,
    current_groups    integer     NOT NULL DEFAULT 0,
    max_weight        integer     NOT NULL DEFAULT 0,
    current_weight    integer     NOT NULL DEFAULT 0,

    last_heartbeat_at timestamptz NOT NULL DEFAULT now(),
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE market_leases
(
    group_id         text PRIMARY KEY
        REFERENCES market_groups (group_id) ON DELETE CASCADE,
    node_id          text        NOT NULL,
    lease_expires_at timestamptz NOT NULL,
    version          bigint      NOT NULL DEFAULT 0,

    acquired_at      timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX market_leases_node_id_idx ON market_leases (node_id);

CREATE TABLE stream_runtime_status
(
    input_id              text PRIMARY KEY
        REFERENCES group_inputs (input_id) ON DELETE CASCADE,
    group_id              text        NOT NULL,
    stream_key            text        NOT NULL,

    node_id               text,
    actual_status         text        NOT NULL
        CHECK (actual_status IN ('pending', 'starting', 'running', 'paused', 'error', 'stopped')),

    kafka_partition       integer,
    kafka_lag             bigint,
    committed_offset      bigint,
    high_watermark_offset bigint,

    last_event_time       timestamptz,
    last_processed_time   timestamptz,
    last_error            text,

    updated_at            timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX stream_runtime_status_group_id_idx ON stream_runtime_status (group_id);

