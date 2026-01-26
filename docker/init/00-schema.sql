-- ClickHouse schema for pg_stat_ch events
-- This matches the schema exported by the pg_stat_ch extension

CREATE DATABASE IF NOT EXISTS pg_stat_ch;

DROP TABLE IF EXISTS pg_stat_ch.events_raw;

CREATE TABLE pg_stat_ch.events_raw
(
    ts_start DateTime64(6, 'UTC'),
    duration_us UInt64,
    db LowCardinality(String),
    username LowCardinality(String),
    pid Int32,
    query_id UInt64,
    cmd_type LowCardinality(String),
    rows UInt64,
    query String CODEC(ZSTD(3)),

    shared_blks_hit Int64,
    shared_blks_read Int64,
    shared_blks_dirtied Int64,
    shared_blks_written Int64,
    local_blks_hit Int64,
    local_blks_read Int64,
    local_blks_dirtied Int64,
    local_blks_written Int64,
    temp_blks_read Int64,
    temp_blks_written Int64,

    shared_blk_read_time_us Int64,
    shared_blk_write_time_us Int64,
    local_blk_read_time_us Int64,
    local_blk_write_time_us Int64,
    temp_blk_read_time_us Int64,
    temp_blk_write_time_us Int64,

    wal_records Int64,
    wal_fpi Int64,
    wal_bytes UInt64,

    cpu_user_time_us Int64,
    cpu_sys_time_us Int64,

    jit_functions Int32,
    jit_generation_time_us Int32,
    jit_deform_time_us Int32,
    jit_inlining_time_us Int32,
    jit_optimization_time_us Int32,
    jit_emission_time_us Int32,

    parallel_workers_planned Int16,
    parallel_workers_launched Int16,

    err_sqlstate FixedString(5),
    err_elevel UInt8,
    err_message String CODEC(ZSTD(3)),

    app LowCardinality(String),
    client_addr String
)
ENGINE = MergeTree
PARTITION BY toDate(ts_start)
ORDER BY (ts_start, db, query_id);

-- MV1: recent events (last 1 hour, TTL)
-- Physically stored and bounded by TTL for fast recent-event queries

DROP TABLE IF EXISTS pg_stat_ch.events_recent_1h;

CREATE MATERIALIZED VIEW pg_stat_ch.events_recent_1h
ENGINE = MergeTree
PARTITION BY toDate(ts_start)
ORDER BY (ts_start, db, query_id)
TTL toDateTime(ts_start) + INTERVAL 1 HOUR DELETE
AS
SELECT *
FROM pg_stat_ch.events_raw;

-- MV2: query stats rollup (5-minute buckets, fast dashboards)
-- Stores aggregate states for correct merging over time

DROP TABLE IF EXISTS pg_stat_ch.query_stats_5m;

CREATE MATERIALIZED VIEW pg_stat_ch.query_stats_5m
(
    bucket DateTime,
    db LowCardinality(String),
    query_id UInt64,
    cmd_type LowCardinality(String),

    calls_state AggregateFunction(count),
    duration_sum_state AggregateFunction(sum, UInt64),
    duration_min_state AggregateFunction(min, UInt64),
    duration_max_state AggregateFunction(max, UInt64),
    duration_q_state AggregateFunction(quantilesTDigest(0.95, 0.99), UInt64),

    rows_sum_state AggregateFunction(sum, UInt64),
    shared_hit_sum_state AggregateFunction(sum, Int64),
    shared_read_sum_state AggregateFunction(sum, Int64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMMDD(bucket)
ORDER BY (bucket, db, query_id, cmd_type)
AS
SELECT
    toStartOfInterval(toDateTime(ts_start), INTERVAL 5 MINUTE) AS bucket,
    db,
    query_id,
    cmd_type,

    countState() AS calls_state,
    sumState(duration_us) AS duration_sum_state,
    minState(duration_us) AS duration_min_state,
    maxState(duration_us) AS duration_max_state,
    quantilesTDigestState(0.95, 0.99)(duration_us) AS duration_q_state,

    sumState(rows) AS rows_sum_state,
    sumState(shared_blks_hit) AS shared_hit_sum_state,
    sumState(shared_blks_read) AS shared_read_sum_state
FROM pg_stat_ch.events_raw
GROUP BY bucket, db, query_id, cmd_type;

-- MV3: load overview by db + app + user (1-minute buckets)
-- "Who is generating load" dashboard MV

DROP TABLE IF EXISTS pg_stat_ch.db_app_user_1m;

CREATE MATERIALIZED VIEW pg_stat_ch.db_app_user_1m
(
    bucket DateTime,
    db LowCardinality(String),
    app LowCardinality(String),
    username LowCardinality(String),
    cmd_type LowCardinality(String),

    calls_state AggregateFunction(count),
    duration_sum_state AggregateFunction(sum, UInt64),
    duration_q_state AggregateFunction(quantilesTDigest(0.95, 0.99), UInt64),
    errors_sum_state AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMMDD(bucket)
ORDER BY (bucket, db, app, username, cmd_type)
AS
SELECT
    toStartOfMinute(toDateTime(ts_start)) AS bucket,
    db,
    app,
    username,
    cmd_type,

    countState() AS calls_state,
    sumState(duration_us) AS duration_sum_state,
    quantilesTDigestState(0.95, 0.99)(duration_us) AS duration_q_state,
    sumState(toUInt64(err_elevel > 0)) AS errors_sum_state
FROM pg_stat_ch.events_raw
GROUP BY bucket, db, app, username, cmd_type;

-- MV4: errors with messages (for error investigation)
-- Stores recent errors with full context for debugging

DROP TABLE IF EXISTS pg_stat_ch.errors_recent;

CREATE MATERIALIZED VIEW pg_stat_ch.errors_recent
ENGINE = MergeTree
PARTITION BY toDate(ts_start)
ORDER BY (ts_start, db, err_sqlstate)
TTL toDateTime(ts_start) + INTERVAL 7 DAY DELETE
AS
SELECT
    ts_start,
    db,
    username,
    app,
    client_addr,
    pid,
    query_id,
    err_sqlstate,
    err_elevel,
    err_message,
    query
FROM pg_stat_ch.events_raw
WHERE err_elevel > 0;

-- Example query to view recent errors:
--
-- SELECT
--     ts_start,
--     db,
--     username,
--     app,
--     err_sqlstate,
--     err_elevel,
--     err_message,
--     substring(query, 1, 200) AS query_preview
-- FROM pg_stat_ch.errors_recent
-- WHERE ts_start > now() - INTERVAL 1 HOUR
-- ORDER BY ts_start DESC
-- LIMIT 100;

-- Example "finalize" query for query_stats_5m:
--
-- WITH quantilesTDigestMerge(0.95, 0.99)(duration_q_state) AS q
-- SELECT
--   db,
--   query_id,
--   cmd_type,
--   countMerge(calls_state) AS calls,
--   sumMerge(duration_sum_state) AS total_time_us,
--   q[1] AS p95_time_us,
--   q[2] AS p99_time_us,
--   sumMerge(rows_sum_state) AS total_rows
-- FROM pg_stat_ch.query_stats_5m
-- WHERE bucket >= now() - INTERVAL 1 HOUR
-- GROUP BY db, query_id, cmd_type
-- ORDER BY p99_time_us DESC
-- LIMIT 50;
