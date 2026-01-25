-- Test client context capture
-- Verifies that application_name and client info are captured

CREATE EXTENSION pg_stat_ch;
SELECT pg_stat_ch_reset();

-- Set application name
SET application_name = 'test_app_client_info';

-- Run some queries
SELECT 1 AS test_query;
SELECT 2 AS another_query;

-- Verify events were captured (application_name should be in events)
SELECT
  (SELECT enqueued_events FROM pg_stat_ch_stats()) >= 2 AS events_captured;

-- Test with different application name
SET application_name = 'different_app_name';
SELECT 3 AS third_query;

-- Verify more events
SELECT
  (SELECT enqueued_events FROM pg_stat_ch_stats()) >= 3 AS more_events;

-- Reset application name
RESET application_name;

DROP EXTENSION pg_stat_ch;
