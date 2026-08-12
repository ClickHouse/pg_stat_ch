\echo Use "ALTER EXTENSION pg_stat_ch UPDATE TO '0.4'" to load this file. \quit

-- 0.4 is a no-op on the Postgres side: no functions, types, or other SQL
-- objects changed. The default_version bump exists purely so
-- `ALTER EXTENSION pg_stat_ch UPDATE` has a migration path to follow —
-- without this file, an install pinned at extversion='0.3' would have no
-- way to reach '0.4' once that becomes the control file's default_version.
--
-- What actually changed in this release lives entirely on the ClickHouse
-- side: the exporter can now write to a new unified `events_raw` schema
-- (see schema/migrations/) via the `pg_stat_ch.use_unified_arrow_exporter`
-- GUC, replacing the legacy `query_logs_arrow` wire shape. That's an
-- opt-in producer/schema change, not a change to this extension's SQL
-- interface, so there's nothing to migrate here.
