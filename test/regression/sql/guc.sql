-- GUC configuration test
CREATE EXTENSION pg_stat_ch;
SELECT name FROM pg_settings WHERE name LIKE 'pg_stat_ch.%' ORDER BY name;
SHOW pg_stat_ch.enabled;
DROP EXTENSION pg_stat_ch;
