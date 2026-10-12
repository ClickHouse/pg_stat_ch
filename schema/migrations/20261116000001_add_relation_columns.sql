-- Adds structured relation columns: namespace, name, and relkind kept as
-- separate parallel arrays. Collected in src/hooks/hooks.c
-- (ExecutorCheckPerms_hook).

-- +goose Up

-- +goose StatementBegin
ALTER TABLE pg_stat_ch.events_raw
    ADD COLUMN IF NOT EXISTS relation_namespaces Array(LowCardinality(String)) COMMENT 'Schema name per entry, positionally parallel to relation_names/relation_kinds.',
    ADD COLUMN IF NOT EXISTS relation_names Array(LowCardinality(String)) COMMENT 'Relation name per entry, positionally parallel to relation_namespaces/relation_kinds.',
    ADD COLUMN IF NOT EXISTS relation_kinds Array(FixedString(1)) COMMENT 'pg_class.relkind char per entry (r=table, v=view, m=matview, f=foreign table, p=partitioned table, ...).',
    ADD COLUMN IF NOT EXISTS relations_truncated Bool DEFAULT false COMMENT 'True if pg_stat_ch.relations_max_len was exceeded and this list is incomplete.';
-- +goose StatementEnd

-- +goose Down

-- Keep the columns on rollback: ClickHouse refuses to drop them while
-- events_recent_1h's SELECT * depends on them, and producers that don't send
-- them still insert.
SELECT 1;
