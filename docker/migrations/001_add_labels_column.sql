-- Migration 001: Add labels column for sqlcommenter support
--
-- This migration adds the `labels` column to `events_raw` for existing
-- installations. New installations already include this column via
-- docker/init/00-schema.sql.
--
-- Run with:
--   clickhouse-client < docker/migrations/001_add_labels_column.sql
--
-- Safe to re-run: ALTER TABLE ADD COLUMN IF NOT EXISTS is idempotent.

ALTER TABLE pg_stat_ch.events_raw
    ADD COLUMN IF NOT EXISTS labels String DEFAULT '{}'
    COMMENT 'Query labels from sqlcommenter comments (key=value pairs in /* */ blocks). Access subpaths directly: labels.controller, labels.action. Empty {} when no labels present. See: https://google.github.io/sqlcommenter/'
    AFTER query;
