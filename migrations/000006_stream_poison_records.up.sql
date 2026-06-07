-- M10 dead-letter path: store Kafka records that cannot be decoded or
-- validated so the runtime can skip their offsets safely.
CREATE TABLE IF NOT EXISTS stream_poison_records (
    id              bigserial    PRIMARY KEY,
    input_id        text         NOT NULL,
    group_id        text         NOT NULL,
    stream_kind     text         NOT NULL,
    kafka_partition integer      NOT NULL,
    kafka_offset    bigint       NOT NULL,
    error_message   text         NOT NULL,
    raw_payload     bytea,
    recorded_at     timestamptz  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_poison_input_time
    ON stream_poison_records (input_id, recorded_at);
