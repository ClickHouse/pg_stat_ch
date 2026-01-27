-- Test DROP DATABASE barrier handling
-- Verifies bgworker uses procsignal_sigusr1_handler for SIGUSR1
--
-- This tests the fix for a bug where the bgworker used a custom SIGUSR1 handler
-- instead of procsignal_sigusr1_handler, preventing it from acknowledging
-- ProcSignalBarrier events. DROP DATABASE uses barriers to coordinate file
-- descriptor closure across all backends, so it would hang indefinitely.

CREATE EXTENSION pg_stat_ch;

-- Test 1: Verify bgworker is running
SELECT count(*) AS bgworker_count
FROM pg_stat_activity
WHERE backend_type = 'pg_stat_ch exporter';

-- Test 2: CREATE DATABASE
CREATE DATABASE testdb_barrier;

-- Test 3: DROP DATABASE with timeout safety
-- If the barrier bug exists, this hangs. statement_timeout catches it.
SET statement_timeout = '5s';
DROP DATABASE testdb_barrier;
RESET statement_timeout;

-- Test 4: Verify bgworker survived the barrier event
SELECT count(*) AS bgworker_count
FROM pg_stat_activity
WHERE backend_type = 'pg_stat_ch exporter';

-- Test 5: Multiple CREATE/DROP cycles
CREATE DATABASE testdb_cycle_1;
DROP DATABASE testdb_cycle_1;
CREATE DATABASE testdb_cycle_2;
DROP DATABASE testdb_cycle_2;

-- Test 6: Verify stats function works (bgworker healthy)
SELECT enqueued_events >= 0 AS stats_ok FROM pg_stat_ch_stats();

DROP EXTENSION pg_stat_ch;
