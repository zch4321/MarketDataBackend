-- 000003_tighten_metadata_constraints.down.sql

ALTER TABLE group_inputs
    DROP CONSTRAINT IF EXISTS group_inputs_stream_key_consistent,
    DROP CONSTRAINT IF EXISTS group_inputs_stream_key_not_empty;

ALTER TABLE market_groups
    DROP CONSTRAINT IF EXISTS market_groups_market_type_check;
