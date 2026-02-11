// pg_stat_ch event structure for query telemetry
//
// FIXED-SIZE DESIGN RATIONALE:
// We use fixed-size events (~6KB each) instead of variable-length events for several
// reasons borrowed from PostgreSQL's design philosophy:
//
// 1. SIMPLICITY: Fixed-size events allow simple ring buffer math without fragmentation.
//    Variable-length would require a memory allocator in shared memory (complex).
//
// 2. LOCK-FREE READS: Fixed size enables the consumer to read without coordinating
//    with producers. Variable-length would need reference counting or complex GC.
//
// 3. PREDICTABLE MEMORY: Total memory = capacity * sizeof(PschEvent). Easy to reason
//    about and configure. Variable-length has unpredictable peak usage.
//
// 4. FAST COPY: memcpy() of ~6KB is ~90ns on modern CPUs with cache. Variable-length
//    would require copying arbitrary amounts of data under lock.
//
// ALTERNATIVE CONSIDERED: pg_stat_monitor uses DSA (Dynamic Shared Area) for
// variable-length query text. This allows unlimited query length but adds complexity
// with DSA allocation, OOM handling, and pointer management. For telemetry/metrics,
// 2KB query text is sufficient - we don't need full query text for aggregation.
#ifndef PG_STAT_CH_SRC_QUEUE_EVENT_H_
#define PG_STAT_CH_SRC_QUEUE_EVENT_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"

#include "datatype/timestamp.h"

// Maximum query text length stored in events (truncated if longer)
// 2KB is enough for most queries; full query text is available via pg_stat_statements
#define PSCH_MAX_QUERY_LEN 2048

// ============================================================================
// Log field buffer sizes (compile-time configurable)
// ============================================================================
// Users can override these via compiler flags before building:
//   cmake -DCMAKE_CXX_FLAGS="-DPSCH_MAX_LOG_MESSAGE_LEN=4096"
//
// Trade-off: Larger buffers capture more context but increase memory usage.
// Event size affects total shared memory: queue_capacity * sizeof(PschEvent)
// ============================================================================

// Maximum log message length (renamed from PSCH_MAX_ERR_MSG_LEN)
#ifndef PSCH_MAX_LOG_MESSAGE_LEN
#define PSCH_MAX_LOG_MESSAGE_LEN 2048
#endif

// Source file name (e.g., "postgres.c")
#ifndef PSCH_MAX_LOG_FILENAME_LEN
#define PSCH_MAX_LOG_FILENAME_LEN 64
#endif

// Function name (e.g., "ExecInitNode")
#ifndef PSCH_MAX_LOG_FUNCNAME_LEN
#define PSCH_MAX_LOG_FUNCNAME_LEN 64
#endif

// DETAIL message (additional error context)
#ifndef PSCH_MAX_LOG_DETAIL_LEN
#define PSCH_MAX_LOG_DETAIL_LEN 512
#endif

// HINT message (suggested user action)
#ifndef PSCH_MAX_LOG_HINT_LEN
#define PSCH_MAX_LOG_HINT_LEN 256
#endif

// CONTEXT message (call stack/location context)
#ifndef PSCH_MAX_LOG_CONTEXT_LEN
#define PSCH_MAX_LOG_CONTEXT_LEN 512
#endif

// Command type values (matching PostgreSQL's CmdType enum)
enum PschCmdType {
  PSCH_CMD_UNKNOWN = 0,
  PSCH_CMD_SELECT = 1,
  PSCH_CMD_UPDATE = 2,
  PSCH_CMD_INSERT = 3,
  PSCH_CMD_DELETE = 4,
  PSCH_CMD_MERGE = 5,
  PSCH_CMD_UTILITY = 6,
  PSCH_CMD_NOTHING = 7
};

// Event structure stored in shared memory queue (~6KB fixed size)
//
// VERSION-SPECIFIC FIELDS: All fields are unconditionally present regardless of
// PostgreSQL version. This keeps the struct size fixed for ring buffer simplicity.
// Fields marked with "PGxx+" are zero when running on older versions. The exporter
// sends all fields; ClickHouse handles NULL/zero appropriately in aggregations.
struct PschEvent {
  // Timing information
  TimestampTz ts_start;  // Query start timestamp (microseconds since epoch)
  uint64 duration_us;    // Execution duration in microseconds

  // Identity (OIDs are stable identifiers; names can be renamed via ALTER)
  Oid dbid;     // Database OID (0 for system processes without a database)
  Oid userid;   // User OID (0 for system processes)
  int32 pid;    // Backend process ID
  uint64 queryid;        // Query ID (from pg_stat_statements)
  bool top_level;        // True if this is a top-level query
  PschCmdType cmd_type;  // Command type (SELECT, UPDATE, etc.)

  // Results
  uint64 rows;  // Number of rows affected/returned

  // Buffer usage
  int64 shared_blks_hit;
  int64 shared_blks_read;
  int64 shared_blks_dirtied;
  int64 shared_blks_written;
  int64 local_blks_hit;
  int64 local_blks_read;
  int64 local_blks_dirtied;
  int64 local_blks_written;
  int64 temp_blks_read;
  int64 temp_blks_written;

  // I/O timing (microseconds)
  int64 shared_blk_read_time_us;
  int64 shared_blk_write_time_us;
  int64 local_blk_read_time_us;   // PG17+
  int64 local_blk_write_time_us;  // PG17+
  int64 temp_blk_read_time_us;    // PG15+
  int64 temp_blk_write_time_us;   // PG15+

  // WAL usage
  int64 wal_records;
  int64 wal_fpi;
  uint64 wal_bytes;

  // CPU time (microseconds, from getrusage delta)
  int64 cpu_user_time_us;
  int64 cpu_sys_time_us;

  // JIT instrumentation (PG15+, 0 if not available)
  int32 jit_functions;
  int32 jit_generation_time_us;
  int32 jit_deform_time_us;  // PG17+ only
  int32 jit_inlining_time_us;
  int32 jit_optimization_time_us;
  int32 jit_emission_time_us;

  // Parallel workers (PG18+, 0 if not available)
  int16 parallel_workers_planned;
  int16 parallel_workers_launched;

  // ========================================================================
  // Log info (from emit_log_hook)
  // ========================================================================
  // Basic log fields
  char log_sqlstate[6];    // SQLSTATE code (e.g., "42P01")
  uint8 log_elevel;        // Error level (0=success, 19=WARNING, 21=ERROR, 22=FATAL)
  uint16 log_message_len;  // Actual length of log message
  char log_message[PSCH_MAX_LOG_MESSAGE_LEN];  // Log message text (truncated if necessary)

  // Source location (where the log was emitted in PostgreSQL code)
  char log_filename[PSCH_MAX_LOG_FILENAME_LEN];  // Source file name (e.g., "postgres.c")
  uint8 log_filename_len;                        // Actual length
  char log_funcname[PSCH_MAX_LOG_FUNCNAME_LEN];  // Function name (e.g., "ExecInitNode")
  uint8 log_funcname_len;                        // Actual length
  int32 log_lineno;                              // Source line number

  // Extended log info (DETAIL, HINT, CONTEXT from ErrorData)
  char log_detail[PSCH_MAX_LOG_DETAIL_LEN];  // DETAIL message
  uint16 log_detail_len;                     // Actual length
  char log_hint[PSCH_MAX_LOG_HINT_LEN];      // HINT message
  uint16 log_hint_len;                       // Actual length
  char log_context[PSCH_MAX_LOG_CONTEXT_LEN];  // CONTEXT message
  uint16 log_context_len;                      // Actual length

  // Client context
  char application_name[64];   // Application name (NAMEDATALEN=64)
  uint8 application_name_len;  // Actual length
  char client_addr[46];        // Client IP address (INET6_ADDRSTRLEN=46)
  uint8 client_addr_len;       // Actual length

  // Query text (null-terminated, truncated if necessary)
  uint16 query_len;  // Actual length of query text
  char query[PSCH_MAX_QUERY_LEN];

  // Normalized query text (constants replaced with $1, $2, ...)
  uint16 query_normalized_len;
  char query_normalized[PSCH_MAX_QUERY_LEN];
};

// Ensure the struct has expected size characteristics
#define PSCH_EVENT_SIZE sizeof(PschEvent)

#ifdef __cplusplus
}
#endif


#endif  // PG_STAT_CH_SRC_QUEUE_EVENT_H_
