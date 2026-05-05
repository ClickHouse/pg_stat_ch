\echo Use "ALTER EXTENSION pg_stat_ch UPDATE TO '0.3'" to load this file. \quit

-- pg_stat_ch_stats() gained an 11th OUT column (dsa_oom_count) in v0.3.5,
-- but the extension's default_version stayed at '0.1' through v0.3.6. So
-- a database with extversion='0.1' could be in either of two states:
--   - installed from v0.1.x..v0.3.4 → catalog has 10 OUT columns
--   - installed from v0.3.5..v0.3.6 → catalog already has 11 OUT columns
-- Only rewrite the function when the catalog is the legacy 10-column shape.
-- Avoids touching it (and breaking dependent views) when it is already
-- up-to-date.
DO $migrate$
DECLARE
  current_outargs int;
BEGIN
  SELECT array_length(proallargtypes, 1)
    INTO current_outargs
    FROM pg_proc
   WHERE proname = 'pg_stat_ch_stats'
     AND pronamespace = 'public'::regnamespace;

  IF current_outargs IS DISTINCT FROM 11 THEN
    EXECUTE 'DROP FUNCTION pg_stat_ch_stats()';
    EXECUTE $ddl$
      CREATE FUNCTION pg_stat_ch_stats(
        OUT enqueued_events bigint,
        OUT dropped_events bigint,
        OUT exported_events bigint,
        OUT send_failures bigint,
        OUT last_success_ts timestamptz,
        OUT last_error_text text,
        OUT last_error_ts timestamptz,
        OUT queue_size integer,
        OUT queue_capacity integer,
        OUT queue_usage_pct double precision,
        OUT dsa_oom_count bigint
      )
      RETURNS record
      AS 'MODULE_PATHNAME'
      LANGUAGE C STRICT
    $ddl$;
    EXECUTE $c$COMMENT ON FUNCTION pg_stat_ch_stats() IS 'Returns pg_stat_ch queue and exporter statistics'$c$;
  END IF;
END
$migrate$;
