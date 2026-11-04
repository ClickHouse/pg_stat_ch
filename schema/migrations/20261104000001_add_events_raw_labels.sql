-- Add the labels column for sqlcommenter query labels.
--
-- With pg_stat_ch.track_labels on, the bgworker parses the /* key='value' */
-- comment that ends each query and exports the pairs as a JSON object string.
-- Events without labels, and producers that don't send the column, store '{}'.

-- +goose Up

-- +goose StatementBegin
ALTER TABLE pg_stat_ch.events_raw
    ADD COLUMN IF NOT EXISTS labels String DEFAULT '{}'
    COMMENT 'Query labels from sqlcommenter comments, as a JSON object string. Read one with JSONExtractString(labels, ''controller''). Empty {} when no labels are present. See: https://google.github.io/sqlcommenter/'
    AFTER query_text;
-- +goose StatementEnd

-- +goose Down

-- Keep the column on rollback: ClickHouse refuses to drop it while
-- events_recent_1h's SELECT * depends on it, and producers that don't send it
-- still insert.
SELECT 1;
