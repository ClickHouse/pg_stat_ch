// pg_stat_ch event structure for query telemetry
#ifndef PG_STAT_CH_SRC_QUEUE_EVENT_H_
#define PG_STAT_CH_SRC_QUEUE_EVENT_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"
#include "datatype/timestamp.h"

// Maximum query text length stored in events
#define PSCH_MAX_QUERY_LEN 2048

// Command type values (matching PostgreSQL's CmdType enum)
typedef enum PschCmdType {
  PSCH_CMD_UNKNOWN = 0,
  PSCH_CMD_SELECT = 1,
  PSCH_CMD_UPDATE = 2,
  PSCH_CMD_INSERT = 3,
  PSCH_CMD_DELETE = 4,
  PSCH_CMD_MERGE = 5,
  PSCH_CMD_UTILITY = 6,
  PSCH_CMD_NOTHING = 7
} PschCmdType;

// Event structure stored in shared memory queue (~2KB fixed size)
typedef struct PschEvent {
  // Timing information
  TimestampTz ts_start;    // Query start timestamp (microseconds since epoch)
  uint64 duration_us;      // Execution duration in microseconds

  // Identity
  Oid dbid;                // Database OID
  Oid userid;              // User OID
  int32 pid;               // Backend process ID
  uint64 queryid;          // Query ID (from pg_stat_statements)
  bool top_level;          // True if this is a top-level query
  PschCmdType cmd_type;    // Command type (SELECT, UPDATE, etc.)

  // Results
  uint64 rows;             // Number of rows affected/returned

  // Query text (null-terminated, truncated if necessary)
  uint16 query_len;        // Actual length of query text
  char query[PSCH_MAX_QUERY_LEN];
} PschEvent;

// Ensure the struct has expected size characteristics
#define PSCH_EVENT_SIZE sizeof(PschEvent)

#ifdef __cplusplus
}
#endif

#endif  // PG_STAT_CH_SRC_QUEUE_EVENT_H_
