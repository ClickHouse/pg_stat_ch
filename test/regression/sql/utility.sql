-- Test ProcessUtility hook captures DDL statements
CREATE EXTENSION pg_stat_ch;
SELECT pg_stat_ch_reset();

-- Utility statements (go through ProcessUtility_hook)
CREATE TABLE test_utility (id int);
CREATE INDEX idx_test_utility ON test_utility(id);

-- DML statements (go through Executor hooks)
INSERT INTO test_utility VALUES(1), (2), (3);
SELECT * FROM test_utility;
UPDATE test_utility SET id = id + 10;
DELETE FROM test_utility WHERE id > 11;

-- More utility statements
TRUNCATE test_utility;
DROP TABLE test_utility;

-- Verify events were captured
SELECT
  (SELECT count(*) FROM pg_stat_ch_stats() WHERE enqueued_events > 0) > 0
    AS events_captured;

DROP EXTENSION pg_stat_ch;
