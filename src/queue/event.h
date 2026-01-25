// pg_stat_ch event structure for query telemetry
//
// FIXED-SIZE DESIGN RATIONALE:
// We use fixed-size events (~2KB each) instead of variable-length events for several
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
// 4. FAST COPY: memcpy() of 2KB is ~20ns on modern CPUs with cache. Variable-length
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

// Event structure stored in shared memory queue (~2KB fixed size)
struct PschEvent {
  // Timing information
  TimestampTz ts_start;  // Query start timestamp (microseconds since epoch)
  uint64 duration_us;    // Execution duration in microseconds

  // Identity
  Oid dbid;              // Database OID
  Oid userid;            // User OID
  char datname[64];      // Database name (NAMEDATALEN=64, resolved at capture)
  uint8 datname_len;     // Actual length
  char username[64];     // User name (NAMEDATALEN=64, resolved at capture)
  uint8 username_len;    // Actual length
  int32 pid;             // Backend process ID
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

  // Error info (from emit_log_hook)
  char err_sqlstate[6];  // SQLSTATE code (e.g., "42P01")
  uint8 err_elevel;      // Error level (0=success, WARNING=19, ERROR=20)
  uint8 _padding3;       // Alignment

  // Client context
  char application_name[64];   // Application name (NAMEDATALEN=64)
  uint8 application_name_len;  // Actual length
  char client_addr[46];        // Client IP address (INET6_ADDRSTRLEN=46)
  uint8 client_addr_len;       // Actual length

  // Query text (null-terminated, truncated if necessary)
  uint16 query_len;  // Actual length of query text
  char query[PSCH_MAX_QUERY_LEN];
};

// Ensure the struct has expected size characteristics
#define PSCH_EVENT_SIZE sizeof(PschEvent)

#ifdef __cplusplus
}
#endif

#endif  // PG_STAT_CH_SRC_QUEUE_EVENT_H_
