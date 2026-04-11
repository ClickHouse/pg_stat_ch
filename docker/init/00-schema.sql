-- ============================================================================
-- ClickHouse schema for pg_stat_ch events
-- ============================================================================
--
-- CANONICAL SCHEMA REFERENCE
--
-- This file is the single source of truth for the pg_stat_ch ClickHouse schema.
-- It serves a dual role:
--   1. Docker init script (applied automatically by docker-compose)
--   2. Schema documentation (column comments, MV explanations)
--
-- For production deployments:  clickhouse-client < docker/init/00-schema.sql
-- For documentation:           see docs/clickhouse.md
--
-- ============================================================================
--
-- This schema is designed for the pg_stat_ch PostgreSQL extension which exports
-- raw query execution telemetry to ClickHouse. All aggregation (p50/p95/p99,
-- top queries, error rates) happens in ClickHouse via materialized views.
--
-- ============================================================================

CREATE DATABASE IF NOT EXISTS pg_stat_ch;

DROP TABLE IF EXISTS pg_stat_ch.events_raw;

-- ============================================================================
-- events_raw: Raw query execution events
-- ============================================================================
--
-- This is the primary table storing one row per query execution. Events are
-- exported in batches by the pg_stat_ch background worker.
--
-- Partitioned by date for efficient time-range queries and data retention.
-- Ordered by ts_start for efficient global time-range scans.
--
-- ============================================================================

CREATE TABLE pg_stat_ch.events_raw
(
    -- ========================================================================
    -- Core identity and timing
    -- ========================================================================
    ts_start DateTime64(6, 'UTC') COMMENT 'Query execution start timestamp (UTC). Used for time-range filtering and partitioning.',

    duration_us UInt64 COMMENT 'Total query execution time in microseconds. HIGH: slow query, investigate EXPLAIN. LOW: fast query. Compare with p95/p99 from query_stats_5m to identify outliers.',

    db LowCardinality(String) COMMENT 'PostgreSQL database name. Use for multi-tenant filtering and per-database load analysis.',

    username LowCardinality(String) COMMENT 'PostgreSQL user/role name. Useful for auditing and per-user resource tracking.',

    pid Int32 COMMENT 'PostgreSQL backend process ID. Correlate with pg_stat_activity for session debugging.',

    query_id Int64 COMMENT '64-bit hash identifying normalized queries. Queries differing only in constants share the same query_id. Use for aggregating statistics across similar queries.',

    cmd_type LowCardinality(String) COMMENT 'Command type: SELECT, INSERT, UPDATE, DELETE, MERGE, UTILITY, or UNKNOWN. Use for workload characterization (read-heavy vs write-heavy).',

    rows UInt64 COMMENT 'Rows returned (SELECT) or affected (INSERT/UPDATE/DELETE). HIGH: large result sets or bulk operations. LOW: point queries. Watch for unexpected HIGH values indicating missing WHERE clauses.',

    query String COMMENT 'Full SQL query text (may be truncated). Used for debugging and query analysis.',

    labels JSON(max_dynamic_paths=64) COMMENT 'Query labels from sqlcommenter comments (key=value pairs in /* */ blocks). Access subpaths directly: labels.controller, labels.action. Empty {} when no labels present. See: https://google.github.io/sqlcommenter/',

    -- ========================================================================
    -- Shared buffer metrics (main buffer cache)
    -- ========================================================================
    -- These metrics show how queries interact with PostgreSQL shared_buffers.
    -- Cache hit ratio = shared_blks_hit / (shared_blks_hit + shared_blks_read)
    -- Target: >99% for OLTP workloads, >95% for mixed workloads.
    -- ========================================================================
    shared_blks_hit Int64 COMMENT 'Shared buffer hits (blocks found in cache). HIGH: good cache efficiency, hot data. LOW on frequently-accessed data: increase shared_buffers or data not fitting in cache.',

    shared_blks_read Int64 COMMENT 'Shared blocks read from disk. HIGH: cache misses, cold data, or working set exceeds shared_buffers. LOW: good cache hit ratio. High reads + low hits = tune shared_buffers or add indexes.',

    shared_blks_dirtied Int64 COMMENT 'Shared blocks dirtied (modified) by this query. HIGH: write-heavy query, checkpoint pressure. LOW: read-only or small writes. Monitor with checkpoint_warning for tuning.',

    shared_blks_written Int64 COMMENT 'Shared blocks written directly to disk by backend (not bgwriter/checkpointer). HIGH: backend doing syncs, bgwriter falling behind. Ideally LOW; writes should go through bgwriter. Increase bgwriter_lru_maxpages if high.',

    -- ========================================================================
    -- Local buffer metrics (temp tables within session)
    -- ========================================================================
    -- Local buffers are used for temporary tables (CREATE TEMP TABLE).
    -- These are session-private and don't benefit from shared_buffers.
    -- ========================================================================
    local_blks_hit Int64 COMMENT 'Local buffer hits (temp table blocks in session cache). HIGH: repeated temp table access. LOW: temp tables not reused or not used.',

    local_blks_read Int64 COMMENT 'Local blocks read from disk. HIGH: large temp tables exceeding temp_buffers. Consider increasing temp_buffers or restructuring query.',

    local_blks_dirtied Int64 COMMENT 'Local blocks dirtied. HIGH: heavy temp table writes. Monitor for queries creating large temp tables.',

    local_blks_written Int64 COMMENT 'Local blocks written to disk. HIGH: temp tables spilling to disk due to temp_buffers exhaustion. Increase temp_buffers.',

    -- ========================================================================
    -- Temp file metrics (sorts, hashes exceeding work_mem)
    -- ========================================================================
    -- Temp files are used when work_mem is insufficient for sorts, hashes,
    -- or materialization. Unlike local buffers, these are always on disk.
    -- Non-zero values indicate work_mem pressure.
    -- ========================================================================
    temp_blks_read Int64 COMMENT 'Temp file blocks read. HIGH: external sorts/hashes re-reading spilled data. Indicates work_mem too small for query. Consider increasing work_mem (session or global).',

    temp_blks_written Int64 COMMENT 'Temp file blocks written. HIGH: sorts/hashes/CTEs spilling to disk. First place to look when query is slow. Increase work_mem or optimize query to reduce intermediate data.',

    -- ========================================================================
    -- I/O timing metrics (requires track_io_timing=on)
    -- ========================================================================
    -- These show actual time spent on I/O, not just block counts.
    -- Useful for identifying storage bottlenecks vs CPU bottlenecks.
    -- All values in microseconds. Zero if track_io_timing=off.
    -- ========================================================================
    shared_blk_read_time_us Int64 COMMENT 'Time reading shared blocks (μs). HIGH: storage slow, many cache misses, or high I/O latency. Compare with shared_blks_read to get avg latency per block. >10ms/block may indicate storage issues.',

    shared_blk_write_time_us Int64 COMMENT 'Time writing shared blocks (μs). HIGH: storage write latency issues, synchronous commits. Usually low unless backend forced to write (bgwriter behind).',

    local_blk_read_time_us Int64 COMMENT 'Time reading local blocks (μs). HIGH: temp tables on slow storage. Consider faster temp tablespace or reducing temp table usage.',

    local_blk_write_time_us Int64 COMMENT 'Time writing local blocks (μs). HIGH: temp table writes slow. Check temp tablespace storage performance.',

    temp_blk_read_time_us Int64 COMMENT 'Time reading temp files (μs). HIGH: work_mem spills causing I/O bottleneck. Increase work_mem or use faster temp storage.',

    temp_blk_write_time_us Int64 COMMENT 'Time writing temp files (μs). HIGH: sorting/hashing spilling to slow storage. Often dominates query time when work_mem is undersized.',

    -- ========================================================================
    -- WAL (Write-Ahead Log) metrics
    -- ========================================================================
    -- WAL is the transaction log ensuring durability. These metrics show
    -- write activity and are critical for replication bandwidth planning.
    -- ========================================================================
    wal_records Int64 COMMENT 'WAL records generated. HIGH: write-intensive query, bulk operations. LOW: read-only or few writes. Useful for estimating replication bandwidth: wal_bytes/duration_us gives write rate.',

    wal_fpi Int64 COMMENT 'WAL full-page images (FPI) generated. HIGH: many pages modified for first time since checkpoint, or full_page_writes=on with frequent page modifications. Increases WAL volume significantly. Consider longer checkpoint_timeout if very high.',

    wal_bytes UInt64 COMMENT 'Total WAL bytes generated. HIGH: large transactions, COPY, bulk UPDATE/DELETE. Directly impacts replication lag and wal_keep_size. Monitor for capacity planning: sum(wal_bytes) over time = WAL generation rate.',

    -- ========================================================================
    -- CPU time metrics
    -- ========================================================================
    -- User vs system time breakdown. Requires getrusage() support (most systems).
    -- Note: May include time from parallel workers on some systems.
    -- ========================================================================
    cpu_user_time_us Int64 COMMENT 'CPU time in user mode (μs). HIGH: CPU-intensive query (parsing, computation). If HIGH but duration_us also HIGH, query is CPU-bound. If LOW but duration HIGH, query is I/O-bound.',

    cpu_sys_time_us Int64 COMMENT 'CPU time in kernel mode (μs). HIGH: heavy I/O syscalls, context switches. Usually low relative to user time. Very high may indicate lock contention or I/O issues.',

    -- ========================================================================
    -- JIT compilation metrics (PostgreSQL 15+)
    -- ========================================================================
    -- JIT (Just-In-Time) compilation can speed up complex queries but has
    -- compilation overhead. Generally beneficial for long-running analytical
    -- queries; overhead may hurt short OLTP queries.
    -- Zero values if JIT not triggered (query below jit_above_cost threshold).
    -- ========================================================================
    jit_functions Int32 COMMENT 'Functions JIT-compiled. HIGH: complex expressions, many tuple operations. >0 means JIT was triggered. Compare jit_*_time_us vs duration_us to see if JIT overhead is worth it.',

    jit_generation_time_us Int32 COMMENT 'Time generating JIT code (μs). Initial compilation overhead. HIGH relative to duration_us: JIT overhead exceeds benefit for this query. Consider raising jit_above_cost.',

    jit_deform_time_us Int32 COMMENT 'Time JIT-compiling tuple deform functions (μs). Tuple deforming unpacks rows from storage format. HIGH: many columns, complex types. PG17+ reports this separately.',

    jit_inlining_time_us Int32 COMMENT 'Time inlining functions (μs). Inlining can dramatically speed up execution but has compile cost. HIGH: many function calls being inlined. If too high, consider jit_inline_above_cost.',

    jit_optimization_time_us Int32 COMMENT 'Time optimizing JIT code (μs). LLVM optimization passes. HIGH: complex code being heavily optimized. If disproportionate to execution, consider jit_optimize_above_cost.',

    jit_emission_time_us Int32 COMMENT 'Time emitting final JIT code (μs). Final machine code generation. Usually smaller than optimization time. Very high may indicate LLVM issues.',

    -- ========================================================================
    -- Parallel worker metrics (PostgreSQL 17+/18+)
    -- ========================================================================
    -- Parallel query uses multiple workers to speed up large sequential scans,
    -- aggregates, and joins. Workers share work and combine results.
    -- ========================================================================
    parallel_workers_planned Int16 COMMENT 'Parallel workers planned by optimizer. Based on table size, parallel_tuple_cost, max_parallel_workers_per_gather. >0 means query could benefit from parallelism.',

    parallel_workers_launched Int16 COMMENT 'Workers actually launched. If < planned: max_parallel_workers limit hit, or workers unavailable. Consistently lower than planned = increase max_parallel_workers or max_worker_processes.',

    -- ========================================================================
    -- Error information
    -- ========================================================================
    -- Captured via emit_log_hook when a query produces an error or warning.
    -- Useful for error tracking, debugging, and monitoring error rates.
    -- ========================================================================
    err_sqlstate FixedString(5) COMMENT 'SQL standard 5-character error code. Examples: 42P01=undefined_table, 23505=unique_violation, 42601=syntax_error, 57014=query_canceled. See PostgreSQL error codes appendix.',

    err_elevel UInt8 COMMENT 'Error severity level. 0=none (success), 19=WARNING, 21=ERROR, 22=FATAL, 23=PANIC. Filter err_elevel>=21 for actual errors. WARNING (19) indicates potential issues.',

    err_message String COMMENT 'Error message text. Contains the human-readable error description. May include DETAIL and HINT in some cases.',

    -- ========================================================================
    -- Client context
    -- ========================================================================
    -- Identifies the source of queries for debugging and load attribution.
    -- ========================================================================
    app LowCardinality(String) COMMENT 'Client application_name. Set via connection string or SET application_name. Use for identifying load sources: "pgAdmin", "myapp-api", "pg_dump", etc.',

    client_addr String COMMENT 'Client IP address. Useful for geographic analysis, debugging connection issues, and identifying load sources by host.'
)
ENGINE = MergeTree
PARTITION BY toDate(ts_start)
ORDER BY ts_start;


-- ============================================================================
-- Materialized View 1: events_recent_1h
-- ============================================================================
--
-- PURPOSE: Fast access to recent events for real-time debugging and monitoring.
--
-- USE CASES:
--   - "Show me queries from the last 5 minutes"
--   - "What's happening right now on database X?"
--   - Real-time dashboards with sub-second refresh
--
-- WHY A SEPARATE MV?
--   - TTL keeps data bounded (auto-deletes after 1 hour)
--   - Smaller data volume = faster queries
--   - events_raw can have longer retention (days/weeks)
--
-- EXAMPLE QUERY:
--   SELECT ts_start, db, duration_us/1000 AS ms, substring(query, 1, 100)
--   FROM pg_stat_ch.events_recent_1h
--   WHERE ts_start > now() - INTERVAL 5 MINUTE
--   ORDER BY ts_start DESC
--   LIMIT 50;
--
-- ============================================================================

DROP TABLE IF EXISTS pg_stat_ch.events_recent_1h;

CREATE MATERIALIZED VIEW pg_stat_ch.events_recent_1h
ENGINE = MergeTree
PARTITION BY toDate(ts_start)
ORDER BY ts_start
TTL toDateTime(ts_start) + INTERVAL 1 HOUR DELETE
AS
SELECT *
FROM pg_stat_ch.events_raw;


-- ============================================================================
-- Materialized View 2: query_stats_5m
-- ============================================================================
--
-- PURPOSE: Pre-aggregated query statistics in 5-minute buckets for dashboards.
--
-- USE CASES:
--   - Dashboard charts: QPS, latency trends, error rates over time
--   - Identifying slow queries by p95/p99 latency
--   - Capacity planning: queries per second by database
--
-- AGGREGATE STATES:
--   This MV stores intermediate "state" columns that must be finalized with
--   -Merge functions when querying. This allows correct aggregation across
--   multiple time buckets.
--
-- HOW TO QUERY:
--   - countMerge(calls_state)           -> total call count
--   - sumMerge(duration_sum_state)      -> total duration
--   - minMerge(duration_min_state)      -> minimum duration
--   - maxMerge(duration_max_state)      -> maximum duration
--   - quantilesTDigestMerge(0.95, 0.99)(duration_q_state) -> [p95, p99]
--
-- EXAMPLE: Top 10 slowest query_ids by p99 latency in the last hour
--
--   SELECT
--     query_id,
--     cmd_type,
--     countMerge(calls_state) AS calls,
--     round(sumMerge(duration_sum_state) / countMerge(calls_state) / 1000, 2) AS avg_ms,
--     round(quantilesTDigestMerge(0.95, 0.99)(duration_q_state)[1] / 1000, 2) AS p95_ms,
--     round(quantilesTDigestMerge(0.95, 0.99)(duration_q_state)[2] / 1000, 2) AS p99_ms
--   FROM pg_stat_ch.query_stats_5m
--   WHERE bucket >= now() - INTERVAL 1 HOUR
--   GROUP BY query_id, cmd_type
--   ORDER BY p99_ms DESC
--   LIMIT 10;
--
-- ============================================================================

DROP TABLE IF EXISTS pg_stat_ch.query_stats_5m;

CREATE MATERIALIZED VIEW pg_stat_ch.query_stats_5m
(
    bucket DateTime COMMENT '5-minute time bucket start',
    db LowCardinality(String) COMMENT 'Database name',
    query_id Int64 COMMENT 'Normalized query identifier',
    cmd_type LowCardinality(String) COMMENT 'Command type (SELECT, INSERT, etc.)',

    calls_state AggregateFunction(count) COMMENT 'Call count state. Finalize with countMerge().',
    duration_sum_state AggregateFunction(sum, UInt64) COMMENT 'Total duration state. Finalize with sumMerge().',
    duration_min_state AggregateFunction(min, UInt64) COMMENT 'Min duration state. Finalize with minMerge().',
    duration_max_state AggregateFunction(max, UInt64) COMMENT 'Max duration state. Finalize with maxMerge().',
    duration_q_state AggregateFunction(quantilesTDigest(0.95, 0.99), UInt64) COMMENT 'Latency percentiles state. Finalize with quantilesTDigestMerge(0.95, 0.99)().',

    rows_sum_state AggregateFunction(sum, UInt64) COMMENT 'Total rows state. Finalize with sumMerge().',
    shared_hit_sum_state AggregateFunction(sum, Int64) COMMENT 'Buffer hits state. Finalize with sumMerge().',
    shared_read_sum_state AggregateFunction(sum, Int64) COMMENT 'Buffer reads state. Finalize with sumMerge(). Cache hit ratio = hits / (hits + reads).'
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


-- ============================================================================
-- Materialized View 3: db_app_user_1m
-- ============================================================================
--
-- PURPOSE: Load overview grouped by database, application, and user.
--
-- USE CASES:
--   - "Which application is generating the most load?"
--   - "Which user is responsible for the slow queries?"
--   - Capacity planning and chargeback by tenant/application
--   - Identifying misbehaving applications or users
--
-- EXAMPLE: Load by application in the last 30 minutes
--
--   SELECT
--     app,
--     countMerge(calls_state) AS total_queries,
--     round(sumMerge(duration_sum_state) / 1000000, 2) AS total_seconds,
--     round(quantilesTDigestMerge(0.95, 0.99)(duration_q_state)[2] / 1000, 2) AS p99_ms,
--     sumMerge(errors_sum_state) AS errors
--   FROM pg_stat_ch.db_app_user_1m
--   WHERE bucket >= now() - INTERVAL 30 MINUTE
--   GROUP BY app
--   ORDER BY total_seconds DESC;
--
-- EXAMPLE: Error rate by database and user
--
--   SELECT
--     db,
--     username,
--     countMerge(calls_state) AS queries,
--     sumMerge(errors_sum_state) AS errors,
--     round(100 * sumMerge(errors_sum_state) / countMerge(calls_state), 2) AS error_pct
--   FROM pg_stat_ch.db_app_user_1m
--   WHERE bucket >= now() - INTERVAL 1 HOUR
--   GROUP BY db, username
--   HAVING errors > 0
--   ORDER BY error_pct DESC;
--
-- ============================================================================

DROP TABLE IF EXISTS pg_stat_ch.db_app_user_1m;

CREATE MATERIALIZED VIEW pg_stat_ch.db_app_user_1m
(
    bucket DateTime COMMENT '1-minute time bucket start',
    db LowCardinality(String) COMMENT 'Database name',
    app LowCardinality(String) COMMENT 'Application name',
    username LowCardinality(String) COMMENT 'PostgreSQL username',
    cmd_type LowCardinality(String) COMMENT 'Command type',

    calls_state AggregateFunction(count) COMMENT 'Query count state. Finalize with countMerge().',
    duration_sum_state AggregateFunction(sum, UInt64) COMMENT 'Total duration state (μs). Finalize with sumMerge().',
    duration_q_state AggregateFunction(quantilesTDigest(0.95, 0.99), UInt64) COMMENT 'Latency percentiles state. Finalize with quantilesTDigestMerge(0.95, 0.99)().',
    errors_sum_state AggregateFunction(sum, UInt64) COMMENT 'Error count state. Finalize with sumMerge().'
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


-- ============================================================================
-- Materialized View 4: errors_recent
-- ============================================================================
--
-- PURPOSE: Recent errors for debugging and incident investigation.
--
-- USE CASES:
--   - "What errors happened in the last hour?"
--   - Incident investigation: correlate errors with specific queries
--   - Error rate monitoring and alerting
--   - Identifying recurring error patterns by SQLSTATE
--
-- RETENTION:
--   7-day TTL keeps error history for investigation while bounding storage.
--   Adjust TTL based on your incident response SLA.
--
-- EXAMPLE: Recent errors with query context
--
--   SELECT
--     ts_start,
--     db,
--     username,
--     app,
--     err_sqlstate,
--     err_message,
--     substring(query, 1, 200) AS query_preview
--   FROM pg_stat_ch.errors_recent
--   WHERE ts_start > now() - INTERVAL 1 HOUR
--   ORDER BY ts_start DESC
--   LIMIT 100;
--
-- EXAMPLE: Error breakdown by SQLSTATE
--
--   SELECT
--     err_sqlstate,
--     count() AS occurrences,
--     uniq(query_id) AS unique_queries,
--     any(err_message) AS sample_message
--   FROM pg_stat_ch.errors_recent
--   WHERE ts_start > now() - INTERVAL 24 HOUR
--   GROUP BY err_sqlstate
--   ORDER BY occurrences DESC;
--
-- COMMON SQLSTATE CODES:
--   23505 - unique_violation (duplicate key)
--   42P01 - undefined_table
--   42703 - undefined_column
--   23503 - foreign_key_violation
--   42601 - syntax_error
--   57014 - query_canceled (statement_timeout)
--   40001 - serialization_failure (retry transaction)
--   53100 - disk_full
--   53200 - out_of_memory
--
-- ============================================================================

DROP TABLE IF EXISTS pg_stat_ch.errors_recent;

CREATE MATERIALIZED VIEW pg_stat_ch.errors_recent
ENGINE = MergeTree
PARTITION BY toDate(ts_start)
ORDER BY ts_start
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
