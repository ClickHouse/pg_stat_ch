-- Verify the 0.1 → 0.3 upgrade path migrates pg_stat_ch_stats()
-- from 10 OUT columns to 11 (adding dsa_oom_count).

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
