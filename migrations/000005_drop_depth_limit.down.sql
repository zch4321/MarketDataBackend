-- 000005_drop_depth_limit.down.sql

-- Reverse the depth_limit column removal. The column is restored as a nullable
-- integer — the value for rows inserted during the migration-5 window is NULL.
-- This is a best-effort rollback; the column may already be absent if the
-- down migration runs against a database that never had it.

ALTER TABLE orderbook_snapshots
 ADD COLUMN IF NOT EXISTS depth_limit integer;
