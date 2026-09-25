-- +goose Up
ALTER TABLE pg_stat_ch.events_raw
    ADD COLUMN IF NOT EXISTS instance_uuid String DEFAULT ''
    COMMENT 'Postgres service UUID supplied by pg_stat_ch.extra_attributes; empty when unset.'
    AFTER instance_ubid;

-- +goose Down
-- Retain the additive column on rollback: events_recent_1h's SELECT * depends
-- on it, and older producers can still insert without supplying it.
SELECT 1;
