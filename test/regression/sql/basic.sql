-- Basic extension lifecycle
CREATE EXTENSION pg_stat_ch;
SELECT extname FROM pg_extension WHERE extname = 'pg_stat_ch';
DROP EXTENSION pg_stat_ch;
