-- Verify the 0.1 → 0.3 migration handles both shapes a database with
-- extversion='0.1' could be in:
--   A) v0.1.x..v0.3.4 install — pg_stat_ch_stats() has 10 OUT columns
--   B) v0.3.5..v0.3.6 install — pg_stat_ch_stats() already has 11 columns
-- Promote (A) to 11. Leave (B) untouched so dependent objects survive.
--
-- We don't ship a 0.1 SQL file any more; both states are constructed by
-- manipulating the catalog directly after a fresh 0.3 install.

-- ---------------------------------------------------------------------------
-- Scenario A: legacy 10-column shape with extversion='0.1'
CREATE EXTENSION pg_stat_ch;
ALTER EXTENSION pg_stat_ch DROP FUNCTION pg_stat_ch_stats();
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
  OUT queue_usage_pct double precision
)
RETURNS record AS '$libdir/pg_stat_ch' LANGUAGE C STRICT;
ALTER EXTENSION pg_stat_ch ADD FUNCTION pg_stat_ch_stats();
UPDATE pg_extension SET extversion = '0.1' WHERE extname = 'pg_stat_ch';

SELECT extversion FROM pg_extension WHERE extname = 'pg_stat_ch';
SELECT array_length(proallargtypes, 1) AS pre_upgrade_outargs
  FROM pg_proc
 WHERE proname = 'pg_stat_ch_stats' AND pronamespace = 'public'::regnamespace;

ALTER EXTENSION pg_stat_ch UPDATE TO '0.3';

SELECT extversion FROM pg_extension WHERE extname = 'pg_stat_ch';
SELECT array_length(proallargtypes, 1) AS post_upgrade_outargs
  FROM pg_proc
 WHERE proname = 'pg_stat_ch_stats' AND pronamespace = 'public'::regnamespace;
SELECT dsa_oom_count = 0 AS dsa_oom_present FROM pg_stat_ch_stats();

DROP EXTENSION pg_stat_ch;

-- ---------------------------------------------------------------------------
-- Scenario B: 11-column shape with extversion stuck at '0.1' and a view
-- depending on pg_stat_ch_stats(). Migration must be a no-op so the view
-- survives.
CREATE EXTENSION pg_stat_ch;
CREATE VIEW pg_stat_ch_dep AS SELECT dsa_oom_count FROM pg_stat_ch_stats();
UPDATE pg_extension SET extversion = '0.1' WHERE extname = 'pg_stat_ch';

SELECT extversion FROM pg_extension WHERE extname = 'pg_stat_ch';
SELECT array_length(proallargtypes, 1) AS pre_upgrade_outargs
  FROM pg_proc
 WHERE proname = 'pg_stat_ch_stats' AND pronamespace = 'public'::regnamespace;

ALTER EXTENSION pg_stat_ch UPDATE TO '0.3';

SELECT extversion FROM pg_extension WHERE extname = 'pg_stat_ch';
SELECT array_length(proallargtypes, 1) AS post_upgrade_outargs
  FROM pg_proc
 WHERE proname = 'pg_stat_ch_stats' AND pronamespace = 'public'::regnamespace;
SELECT count(*) >= 0 AS dependent_view_intact FROM pg_stat_ch_dep;

DROP VIEW pg_stat_ch_dep;
DROP EXTENSION pg_stat_ch;
