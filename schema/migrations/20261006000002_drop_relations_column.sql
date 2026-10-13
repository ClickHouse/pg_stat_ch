-- Drops the old `relations` column, now fully replaced by
-- relation_namespaces/relation_names/relation_kinds/relations_truncated.
-- Recreates events_recent_1h (a SELECT * view, schema frozen at CREATE
-- time) so it picks up the new columns. POPULATE backfills the last hour
-- from events_raw so the recreate doesn't leave it empty; can race a
-- concurrent insert during the backfill window (ClickHouse's own caveat),
-- acceptable for an infrequent, operator-triggered migration.

-- +goose Up

-- +goose StatementBegin
DROP VIEW pg_stat_ch.events_recent_1h;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE pg_stat_ch.events_raw
    DROP COLUMN relations;
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

-- +goose Down

-- +goose StatementBegin
DROP VIEW pg_stat_ch.events_recent_1h;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE pg_stat_ch.events_raw
    ADD COLUMN relations Array(LowCardinality(String)) COMMENT 'Tables/views touched by the query, as schema.relname (views suffixed with *). Empty for utility statements and queries truncated past pg_stat_ch.relations_max_len.';
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
