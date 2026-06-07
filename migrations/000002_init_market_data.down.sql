-- 000002_init_market_data.down.sql
-- Drop market-data fact tables.

DROP TABLE IF EXISTS orderbook_snapshots;
DROP TABLE IF EXISTS orderbook_deltas;
DROP TABLE IF EXISTS klines;
DROP TABLE IF EXISTS trades;

