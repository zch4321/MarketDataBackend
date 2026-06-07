-- 000005_drop_depth_limit.up.sql

-- depth_limit was part of the early orderbook_snapshots schema but is not used
-- by the M8 internally-generated full-depth snapshots. Full-depth snapshots
-- preserve every non-zero price level; their depth_limit is always NULL.
-- Dropping the column simplifies the model and the insert path.

ALTER TABLE orderbook_snapshots
 DROP COLUMN IF EXISTS depth_limit;
