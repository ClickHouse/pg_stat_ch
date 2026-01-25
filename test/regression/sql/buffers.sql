-- Test buffer and WAL usage capture
CREATE EXTENSION pg_stat_ch;
SELECT pg_stat_ch_reset();

-- Create table with data to generate buffer activity
CREATE TABLE test_buffers (id int, data text);
INSERT INTO test_buffers SELECT i, repeat('x', 100) FROM generate_series(1, 1000) i;

-- Force checkpoint to clear buffers
CHECKPOINT;

-- Query that reads buffers
SELECT count(*) FROM test_buffers WHERE id > 500;

-- Verify events captured
SELECT
  (SELECT count(*) FROM pg_stat_ch_stats() WHERE enqueued_events > 0) > 0
    AS events_captured;

DROP TABLE test_buffers;
DROP EXTENSION pg_stat_ch;
