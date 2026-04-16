-- ============================================================================
-- ClickHouse schema for Arrow IPC passthrough ingestion
-- ============================================================================
--
-- This table matches the Arrow schema produced by ArrowBatchBuilder exactly.
-- ClickHouse ingests Arrow IPC bytes via: INSERT INTO query_logs FORMAT ArrowStream
--
-- Column names and types must match the Arrow schema 1:1. ClickHouse maps:
--   Arrow timestamp[ns, tz=UTC]     -> DateTime64(9, 'UTC')
--   Arrow dictionary(int32, utf8)   -> LowCardinality(String)
--   Arrow utf8                      -> String
--   Arrow int32                     -> Int32
--   Arrow uint32                    -> UInt32
--   Arrow uint64                    -> UInt64
-- ============================================================================

CREATE TABLE IF NOT EXISTS pg_stat_ch.query_logs
(
    -- Core identity and timing
    ts                         DateTime64(9, 'UTC'),
    severity                   LowCardinality(String) DEFAULT '',
    body                       String DEFAULT '',
    trace_id                   String DEFAULT '',
    span_id                    String DEFAULT '',
    query_id                   String,
    db_name                    LowCardinality(String),
    db_user                    LowCardinality(String),
    db_operation               LowCardinality(String),
    app                        LowCardinality(String),
    query_text                 String,
    pid                        String,
    err_message                String,
    err_sqlstate               LowCardinality(String),
    err_elevel                 Int32,

    -- Timing
    duration_us                UInt64,

    -- Row count
    rows                       UInt64,

    -- Shared buffer metrics
    shared_blks_hit            UInt64,
    shared_blks_read           UInt64,
    shared_blks_written        UInt64,
    shared_blks_dirtied        UInt64,
    shared_blk_read_time_us    UInt64,
    shared_blk_write_time_us   UInt64,

    -- Local buffer metrics
    local_blks_hit             UInt64,
    local_blks_read            UInt64,
    local_blks_written         UInt64,
    local_blks_dirtied         UInt64,

    -- Temp file metrics
    temp_blks_read             UInt64,
    temp_blks_written          UInt64,
    temp_blk_read_time_us      UInt64,
    temp_blk_write_time_us     UInt64,

    -- WAL metrics
    wal_records                UInt64,
    wal_bytes                  UInt64,
    wal_fpi                    UInt64,

    -- CPU metrics
    cpu_user_time_us           UInt64,
    cpu_sys_time_us            UInt64,

    -- JIT metrics
    jit_functions              UInt64,
    jit_generation_time_us     UInt64,
    jit_inlining_time_us       UInt64,
    jit_optimization_time_us   UInt64,
    jit_emission_time_us       UInt64,
    jit_deform_time_us         UInt64,

    -- Parallel worker metrics
    parallel_workers_planned   UInt32,
    parallel_workers_launched  UInt32,

    -- Resource attributes (from extra_attributes GUC)
    instance_ubid              String DEFAULT '',
    server_ubid                String DEFAULT '',
    server_role                LowCardinality(String) DEFAULT '',
    region                     LowCardinality(String) DEFAULT '',
    cell                       LowCardinality(String) DEFAULT '',
    service_version            LowCardinality(String) DEFAULT '',
    host_id                    String DEFAULT '',
    pod_name                   String DEFAULT ''
)
ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY ts;
