-- pg_stat_ch extension SQL definitions

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_stat_ch" to load this file. \quit

-- Version function
CREATE FUNCTION pg_stat_ch_version()
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

COMMENT ON FUNCTION pg_stat_ch_version()
IS 'Returns the pg_stat_ch extension version';

-- Stats function (11 columns)
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

-- Reset function
CREATE FUNCTION pg_stat_ch_reset() RETURNS void
AS 'MODULE_PATHNAME', 'pg_stat_ch_reset'
LANGUAGE C STRICT;

COMMENT ON FUNCTION pg_stat_ch_reset()
IS 'Resets all pg_stat_ch queue counters to zero';

-- Flush function (trigger immediate export)
CREATE FUNCTION pg_stat_ch_flush() RETURNS void
AS 'MODULE_PATHNAME', 'pg_stat_ch_flush'
LANGUAGE C STRICT;

COMMENT ON FUNCTION pg_stat_ch_flush()
IS 'Trigger immediate flush of queued events to ClickHouse';
