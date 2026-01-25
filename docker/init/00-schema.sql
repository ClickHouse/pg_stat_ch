-- ClickHouse schema for pg_stat_ch events
-- This matches the schema exported by the pg_stat_ch extension

CREATE DATABASE IF NOT EXISTS pg_stat_ch;

CREATE TABLE IF NOT EXISTS pg_stat_ch.events_raw (
    -- Basic columns
    ts_start DateTime64(6),
    duration_us UInt64,
    db String,
    username String,
    pid Int32,
    query_id Int64,
    cmd_type String,
    rows UInt64,
    query String,

    -- Buffer usage columns
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

    -- I/O timing columns (microseconds)
    shared_blk_read_time_us Int64,
    shared_blk_write_time_us Int64,
    local_blk_read_time_us Int64,
    local_blk_write_time_us Int64,
    temp_blk_read_time_us Int64,
    temp_blk_write_time_us Int64,

    -- WAL usage columns
    wal_records Int64,
    wal_fpi Int64,
    wal_bytes UInt64,

    -- CPU time columns (microseconds)
    cpu_user_time_us Int64,
    cpu_sys_time_us Int64,

    -- JIT columns
    jit_functions Int32,
    jit_generation_time_us Int32,
    jit_deform_time_us Int32,
    jit_inlining_time_us Int32,
    jit_optimization_time_us Int32,
    jit_emission_time_us Int32,

    -- Parallel worker columns
    parallel_workers_planned Int16,
    parallel_workers_launched Int16,

    -- Error columns
    err_sqlstate FixedString(5),
    err_elevel UInt8,

    -- Client context columns
    app String,
    client_addr String
) ENGINE = MergeTree()
ORDER BY (ts_start, db, query_id)
PARTITION BY toYYYYMMDD(ts_start);

-- View for easy querying of recent events
CREATE VIEW IF NOT EXISTS pg_stat_ch.events_recent AS
SELECT *
FROM pg_stat_ch.events_raw
WHERE ts_start >= now() - INTERVAL 1 HOUR
ORDER BY ts_start DESC;

-- View for aggregated statistics per query
CREATE VIEW IF NOT EXISTS pg_stat_ch.query_stats AS
SELECT
    db,
    query_id,
    cmd_type,
    count() AS calls,
    sum(duration_us) AS total_time_us,
    avg(duration_us) AS mean_time_us,
    min(duration_us) AS min_time_us,
    max(duration_us) AS max_time_us,
    quantile(0.95)(duration_us) AS p95_time_us,
    quantile(0.99)(duration_us) AS p99_time_us,
    sum(rows) AS total_rows,
    sum(shared_blks_hit) AS total_shared_blks_hit,
    sum(shared_blks_read) AS total_shared_blks_read
FROM pg_stat_ch.events_raw
GROUP BY db, query_id, cmd_type;
