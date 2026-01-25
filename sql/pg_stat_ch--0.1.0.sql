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
