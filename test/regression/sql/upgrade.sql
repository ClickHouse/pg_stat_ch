-- Verify the 0.1 → 0.3 upgrade path. Two scenarios share extversion='0.1'
-- but differ in catalog shape:
--   A) v0.1.x..v0.3.4 install — pg_stat_ch_stats() has 10 OUT columns
--   B) v0.3.5+ install       — pg_stat_ch_stats() already has 11 OUT columns
-- The migration must promote (A) to 11 columns and leave (B) untouched
-- (so dependent objects keep working).

-- ---------------------------------------------------------------------------
-- Scenario A: 10-column legacy install
CREATE EXTENSION pg_stat_ch VERSION '0.1';

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
-- Scenario B: 11-column install (v0.3.5+) where extversion was never bumped.
-- Construct that state by installing 0.3 fresh, attaching a dependent view,
-- then forcing extversion back to '0.1' so ALTER EXTENSION .. UPDATE actually
-- runs the migration.
CREATE EXTENSION pg_stat_ch;
SELECT extversion FROM pg_extension WHERE extname = 'pg_stat_ch';
SELECT array_length(proallargtypes, 1) AS pre_upgrade_outargs
  FROM pg_proc
 WHERE proname = 'pg_stat_ch_stats' AND pronamespace = 'public'::regnamespace;

CREATE VIEW pg_stat_ch_dep AS SELECT dsa_oom_count FROM pg_stat_ch_stats();
UPDATE pg_extension SET extversion = '0.1' WHERE extname = 'pg_stat_ch';

ALTER EXTENSION pg_stat_ch UPDATE TO '0.3';

SELECT extversion FROM pg_extension WHERE extname = 'pg_stat_ch';
SELECT array_length(proallargtypes, 1) AS post_upgrade_outargs
  FROM pg_proc
 WHERE proname = 'pg_stat_ch_stats' AND pronamespace = 'public'::regnamespace;
SELECT count(*) >= 0 AS dependent_view_intact FROM pg_stat_ch_dep;

DROP VIEW pg_stat_ch_dep;
DROP EXTENSION pg_stat_ch;
