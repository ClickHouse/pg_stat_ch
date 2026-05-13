-- Migration: add parent_query_id column
--
-- Introduced in pg_stat_ch 0.4.x. Each event now carries the query_id of its
-- calling query (e.g. the plpgsql function that issued an SPI statement).
-- Top-level queries emit 0. Use WHERE parent_query_id = 0 in aggregations to
-- avoid double-counting CPU and duration across nested calls.
--
-- Run against your ClickHouse instance before upgrading the extension:
--   clickhouse-client < migrations/001_add_parent_query_id.sql

ALTER TABLE pg_stat_ch.events_raw
    ADD COLUMN IF NOT EXISTS parent_query_id UInt64 DEFAULT 0;
