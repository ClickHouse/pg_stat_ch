-- Version function test
CREATE EXTENSION pg_stat_ch;
SELECT length(pg_stat_ch_version()) > 0 AS has_version;
DROP EXTENSION pg_stat_ch;
