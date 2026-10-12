-- Adds the relations array (tables/views a query touches); collected in
-- src/hooks/hooks.c (ExecutorCheckPerms_hook).

-- +goose Up

-- +goose StatementBegin
ALTER TABLE pg_stat_ch.events_raw
    ADD COLUMN relations Array(LowCardinality(String)) COMMENT 'Tables/views touched by the query, as schema.relname (views suffixed with *). Empty for utility statements and queries truncated past pg_stat_ch.relations_max_len.';
-- +goose StatementEnd

-- +goose Down

-- Bracket the column drop with a view rebuild, same pattern the later
-- migrations use, since events_recent_1h's SELECT * schema is frozen at
-- CREATE time and otherwise blocks or outlives this ALTER.

-- +goose StatementBegin
DROP VIEW pg_stat_ch.events_recent_1h;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE pg_stat_ch.events_raw DROP COLUMN relations;
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
