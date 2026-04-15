-- GUC configuration test
CREATE EXTENSION pg_stat_ch;
SELECT name FROM pg_settings WHERE name LIKE 'pg_stat_ch.%' ORDER BY name COLLATE "C";
SHOW pg_stat_ch.enabled;
SHOW pg_stat_ch.string_area_size;
-- Test log_min_elevel GUC
SHOW pg_stat_ch.log_min_elevel;
SET pg_stat_ch.log_min_elevel = 'error';
SHOW pg_stat_ch.log_min_elevel;
SET pg_stat_ch.log_min_elevel = 'notice';
SHOW pg_stat_ch.log_min_elevel;
RESET pg_stat_ch.log_min_elevel;
SHOW pg_stat_ch.log_min_elevel;
-- Test normalize_cache_max GUC
SHOW pg_stat_ch.normalize_cache_max;
SET pg_stat_ch.normalize_cache_max = 128;
SHOW pg_stat_ch.normalize_cache_max;
RESET pg_stat_ch.normalize_cache_max;
SHOW pg_stat_ch.normalize_cache_max;
-- Test that clickhouse_password is hidden from non-superusers (issue #3)
CREATE ROLE psch_test_user LOGIN;
SET ROLE psch_test_user;
-- Should error: must be superuser
SHOW pg_stat_ch.clickhouse_password;
-- Should return no rows (GUC_SUPERUSER_ONLY hides from pg_settings)
SELECT name, setting FROM pg_settings WHERE name = 'pg_stat_ch.clickhouse_password';
RESET ROLE;
DROP ROLE psch_test_user;
DROP EXTENSION pg_stat_ch;
