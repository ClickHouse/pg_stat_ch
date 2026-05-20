-- Bootstrap migration: seeds the Goose version table so that subsequent
-- migrations have a baseline. The SELECT 1 statements are intentional
-- no-ops — do not delete this file.

-- +goose Up
SELECT 1;

-- +goose Down
SELECT 1;
