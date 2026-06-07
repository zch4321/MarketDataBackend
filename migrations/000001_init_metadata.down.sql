-- 000001_init_metadata.down.sql
-- Drop control-plane metadata tables in reverse dependency order.

DROP TABLE IF EXISTS stream_runtime_status;
DROP TABLE IF EXISTS market_leases;
DROP TABLE IF EXISTS runtime_nodes;
DROP TABLE IF EXISTS group_inputs;
DROP TABLE IF EXISTS market_groups;

