-- 000002_init_market_data.up.sql
-- Market-data fact tables: trades, klines, orderbook_deltas, orderbook_snapshots.
-- All numeric market values use numeric (never float); bids/asks use jsonb.
-- Every fact table supports at-least-once idempotent writes via unique keys.

CREATE TABLE trades
(
    id                 bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    group_id           text        NOT NULL,
    input_id           text        NOT NULL,

    event_time         timestamptz NOT NULL,
    exchange_time      timestamptz,
    local_receive_time timestamptz,

    trade_id           text,
    raw_trade_id       text,

    price              numeric     NOT NULL,
    quantity           numeric     NOT NULL,
    side               text        NOT NULL CHECK (side IN ('buy', 'sell')),
    is_aggregated      boolean     NOT NULL DEFAULT false,

    kafka_topic        text        NOT NULL,
    kafka_partition    integer     NOT NULL,
    kafka_offset       bigint      NOT NULL,
    schema_version     integer     NOT NULL DEFAULT 1,
    ingested_at        timestamptz NOT NULL DEFAULT now()
);

-- Fallback idempotency key: input + kafka coordinates.
CREATE UNIQUE INDEX trades_input_offset_uidx
    ON trades (input_id, kafka_partition, kafka_offset);
-- Preferred idempotency key when the exchange supplies a stable raw trade id.
CREATE UNIQUE INDEX trades_input_raw_trade_uidx
    ON trades (input_id, raw_trade_id) WHERE raw_trade_id IS NOT NULL;
CREATE INDEX trades_group_event_time_idx ON trades (group_id, event_time);

CREATE TABLE klines
(
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    group_id        text        NOT NULL,
    input_id        text        NOT NULL,

    source          text        NOT NULL CHECK (source IN ('exchange', 'computed')),
    interval        text        NOT NULL,
    open_time       timestamptz NOT NULL,
    close_time      timestamptz NOT NULL,

    open            numeric     NOT NULL,
    high            numeric     NOT NULL,
    low             numeric     NOT NULL,
    close           numeric     NOT NULL,
    volume          numeric     NOT NULL,
    quote_volume    numeric,
    trade_count     bigint,

    is_closed       boolean     NOT NULL DEFAULT false,
    revision        bigint      NOT NULL DEFAULT 0,

    kafka_topic     text        NOT NULL,
    kafka_partition integer     NOT NULL,
    kafka_offset    bigint      NOT NULL,
    updated_at      timestamptz NOT NULL DEFAULT now(),

    UNIQUE (group_id, source, interval, open_time)
);

CREATE INDEX klines_group_interval_open_idx ON klines (group_id, interval, open_time);

CREATE TABLE orderbook_deltas
(
    id                 bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    group_id           text        NOT NULL,
    input_id           text        NOT NULL,

    event_time         timestamptz NOT NULL,
    exchange_time      timestamptz,
    local_receive_time timestamptz,

    raw_event_id       text,
    first_update_id    bigint,
    last_update_id     bigint,
    prev_update_id     bigint,
    sequence           bigint,

    bids               jsonb       NOT NULL DEFAULT '[]'::jsonb,
    asks               jsonb       NOT NULL DEFAULT '[]'::jsonb,

    kafka_topic        text        NOT NULL,
    kafka_partition    integer     NOT NULL,
    kafka_offset       bigint      NOT NULL,
    ingested_at        timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX orderbook_deltas_input_offset_uidx
    ON orderbook_deltas (input_id, kafka_partition, kafka_offset);
CREATE UNIQUE INDEX orderbook_deltas_input_raw_event_uidx
    ON orderbook_deltas (input_id, raw_event_id) WHERE raw_event_id IS NOT NULL;
CREATE INDEX orderbook_deltas_group_event_time_idx ON orderbook_deltas (group_id, event_time);

CREATE TABLE orderbook_snapshots
(
    snapshot_id   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    group_id      text        NOT NULL,
    input_id      text        NOT NULL,

    snapshot_time timestamptz NOT NULL,
    sequence      bigint,
    depth_limit   integer,

    bids          jsonb       NOT NULL DEFAULT '[]'::jsonb,
    asks          jsonb       NOT NULL DEFAULT '[]'::jsonb,

    created_at    timestamptz NOT NULL DEFAULT now(),

    UNIQUE (input_id, snapshot_time)
);

CREATE INDEX orderbook_snapshots_group_time_idx ON orderbook_snapshots (group_id, snapshot_time);

