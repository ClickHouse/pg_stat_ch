-- GUC configuration test
CREATE EXTENSION pg_stat_ch;
SELECT name FROM pg_settings WHERE name LIKE 'pg_stat_ch.%' ORDER BY name;
SHOW pg_stat_ch.enabled;
-- Test log_min_elevel GUC
SHOW pg_stat_ch.log_min_elevel;
SET pg_stat_ch.log_min_elevel = 'error';
SHOW pg_stat_ch.log_min_elevel;
SET pg_stat_ch.log_min_elevel = 'notice';
SHOW pg_stat_ch.log_min_elevel;
RESET pg_stat_ch.log_min_elevel;
SHOW pg_stat_ch.log_min_elevel;
DROP EXTENSION pg_stat_ch;
