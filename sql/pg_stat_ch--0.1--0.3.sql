\echo Use "ALTER EXTENSION pg_stat_ch UPDATE TO '0.3'" to load this file. \quit

-- pg_stat_ch_stats() gained an 11th OUT column (dsa_oom_count) in v0.3.5,
-- but the extension's default_version stayed at '0.1' through v0.3.6, so
-- installs from v0.1.x through v0.3.4 still have the 10-column variant in
-- their catalog while running the 11-column binary. Drop and recreate to
-- land on the canonical shape; idempotent for v0.3.5+ installs that
-- already have 11 columns.
DROP FUNCTION pg_stat_ch_stats();

CREATE FUNCTION pg_stat_ch_stats(
  OUT enqueued_events bigint,
  OUT dropped_events bigint,
  OUT exported_events bigint,
  OUT send_failures bigint,
  OUT last_success_ts timestamptz,
  OUT last_error_text text,
  OUT last_error_ts timestamptz,
  OUT queue_size integer,
  OUT queue_capacity integer,
  OUT queue_usage_pct double precision,
  OUT dsa_oom_count bigint
)
RETURNS record
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

COMMENT ON FUNCTION pg_stat_ch_stats()
IS 'Returns pg_stat_ch queue and exporter statistics';
