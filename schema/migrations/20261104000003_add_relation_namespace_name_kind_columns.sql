-- Adds structured relation columns: namespace, name, and relkind kept as
-- separate parallel arrays, alongside the existing `relations` column.

-- +goose Up

-- +goose StatementBegin
ALTER TABLE pg_stat_ch.events_raw
    ADD COLUMN relation_namespaces Array(LowCardinality(String)) COMMENT 'Schema name per entry, positionally parallel to relation_names/relation_kinds.',
    ADD COLUMN relation_names Array(LowCardinality(String)) COMMENT 'Relation name per entry, positionally parallel to relation_namespaces/relation_kinds.',
    ADD COLUMN relation_kinds Array(FixedString(1)) COMMENT 'pg_class.relkind char per entry (r=table, v=view, m=matview, f=foreign table, p=partitioned table, ...).',
    ADD COLUMN relations_truncated Bool DEFAULT false COMMENT 'True if pg_stat_ch.relations_max_len was exceeded and this list is incomplete.';
-- +goose StatementEnd

-- +goose Down

-- Bracket the column drop with a view rebuild (same pattern the next
-- migration's Down uses), or the frozen view keeps expecting these columns.

-- +goose StatementBegin
DROP VIEW pg_stat_ch.events_recent_1h;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE pg_stat_ch.events_raw
    DROP COLUMN relation_namespaces,
    DROP COLUMN relation_names,
    DROP COLUMN relation_kinds,
    DROP COLUMN relations_truncated;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE MATERIALIZED VIEW pg_stat_ch.events_recent_1h
ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY (instance_ubid, ts)
TTL toDateTime(ts) + INTERVAL 1 HOUR DELETE
POPULATE
AS
SELECT *
FROM pg_stat_ch.events_raw
WHERE ts > now() - INTERVAL 1 HOUR;
-- +goose StatementEnd
