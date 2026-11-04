-- Add the labels column for sqlcommenter query labels.
--
-- The bgworker parses the last /* key='value' */ comment in each query and
-- exports the pairs as a JSON object string. Events without labels, and
-- producers older than this column, store '{}'.

-- +goose Up

-- +goose StatementBegin
ALTER TABLE pg_stat_ch.events_raw
    ADD COLUMN IF NOT EXISTS labels String DEFAULT '{}'
    COMMENT 'Query labels from sqlcommenter comments, as a JSON object string. Read one with JSONExtractString(labels, ''controller''). Empty {} when no labels are present. See: https://google.github.io/sqlcommenter/'
    AFTER query_text;
-- +goose StatementEnd

-- +goose Down

-- +goose StatementBegin
ALTER TABLE pg_stat_ch.events_raw DROP COLUMN IF EXISTS labels;
-- +goose StatementEnd
