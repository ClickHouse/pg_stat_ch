\echo Use "ALTER EXTENSION pg_stat_ch UPDATE TO '0.4'" to load this file. \quit

-- 0.4 adds the memory-budget observability surface for the consolidated
-- pg_stat_ch.memory_limit GUC (OTEL_REWRITE_DESIGN.md §6).
CREATE FUNCTION pg_stat_ch_memory()
RETURNS TABLE(component text, budget_bytes bigint, source text)
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

COMMENT ON FUNCTION pg_stat_ch_memory()
IS 'Returns the resolved pg_stat_ch.memory_limit budget per component and export drop counters';
