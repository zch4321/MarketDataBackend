-- 000003_tighten_metadata_constraints.up.sql
-- Defense-in-depth integrity for metadata, complementing application-layer
-- validation:
--   * market_type must be a known venue type.
--   * group_inputs.stream_key must be non-empty and consistent with
--     stream_kind/interval (kline carries an interval and uses the
--     "kline_<interval>" key; every other kind has no interval and uses the
--     stream_kind itself as the key).

ALTER TABLE market_groups
    ADD CONSTRAINT market_groups_market_type_check
        CHECK (market_type IN ('spot', 'margin', 'futures', 'swap', 'option'));

ALTER TABLE group_inputs
    ADD CONSTRAINT group_inputs_stream_key_not_empty
        CHECK (stream_key <> ''),
    ADD CONSTRAINT group_inputs_stream_key_consistent
        CHECK (
            (stream_kind = 'kline'
                AND interval IS NOT NULL AND interval <> ''
                AND stream_key = 'kline_' || interval)
            OR
            (stream_kind <> 'kline'
                AND interval IS NULL
                AND stream_key = stream_kind)
        );
