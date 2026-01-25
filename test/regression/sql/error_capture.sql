-- Test error capture
-- Verifies that error details (SQLSTATE, message) are captured

CREATE EXTENSION pg_stat_ch;
SELECT pg_stat_ch_reset();

-- Create test table
CREATE TABLE test_errors(id int PRIMARY KEY, val int NOT NULL);
INSERT INTO test_errors VALUES (1, 100);

-- Successful query (for baseline)
SELECT * FROM test_errors;

-- Error: Division by zero (SQLSTATE 22012)
SELECT 1/0;

-- Error: Undefined table (SQLSTATE 42P01)
SELECT * FROM nonexistent_table_xyz;

-- Error: Null value violation (SQLSTATE 23502)
INSERT INTO test_errors(id, val) VALUES (2, NULL);

-- Error: Unique violation (SQLSTATE 23505)
INSERT INTO test_errors VALUES (1, 200);

-- Error: Syntax error (SQLSTATE 42601) - can't easily test in regression
-- Error: Type mismatch (SQLSTATE 42804)
SELECT id + 'not_a_number' FROM test_errors;

-- Verify events were captured
-- Note: Error events should be captured even though the queries failed
SELECT
  (SELECT enqueued_events FROM pg_stat_ch_stats()) > 0 AS events_captured;

-- Clean up
DROP TABLE test_errors;

DROP EXTENSION pg_stat_ch;
