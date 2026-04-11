-- Stats and reset function test
CREATE EXTENSION pg_stat_ch;
SELECT pg_stat_ch_reset();
SELECT queue_capacity > 0 AS has_capacity FROM pg_stat_ch_stats();
SELECT dsa_oom_count = 0 AS has_no_dsa_ooms FROM pg_stat_ch_stats();
DROP EXTENSION pg_stat_ch;
