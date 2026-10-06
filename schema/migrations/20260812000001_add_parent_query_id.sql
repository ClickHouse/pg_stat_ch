-- Add parent_query_id to events_raw: links nested SPI-issued queries (e.g.
-- statements a plpgsql function runs internally) back to the top-level
-- query_id that invoked them, so aggregations can exclude nested events
-- and avoid double-counting CPU/duration.

-- +goose Up

-- +goose StatementBegin
ALTER TABLE pg_stat_ch.events_raw
    ADD COLUMN IF NOT EXISTS parent_query_id Int64 DEFAULT 0
    COMMENT 'query_id of the calling query (e.g. the plpgsql function that issued this SPI statement). 0 for top-level queries. Use WHERE parent_query_id = 0 to restrict aggregations to top-level queries and avoid double-counting CPU and duration. Signedness matches query_id so the two columns compare/join without explicit casts.'
    AFTER query_id;
-- +goose StatementEnd

-- +goose Down

-- +goose StatementBegin
ALTER TABLE pg_stat_ch.events_raw DROP COLUMN IF EXISTS parent_query_id;
-- +goose StatementEnd
