// pg_stat_ch executor hooks implementation

extern "C" {
#include "postgres.h"

#include "access/parallel.h"
#include "executor/executor.h"
#include "executor/instrument.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "tcop/utility.h"
#include "utils/timestamp.h"

#if PG_VERSION_NUM >= 140000
#include "nodes/queryjumble.h"
#endif
}

#include "hooks/hooks.h"
#include "config/guc.h"
#include "queue/event.h"
#include "queue/shmem.h"

// Previous hook values for chaining
static ExecutorStart_hook_type prev_executor_start = nullptr;
static ExecutorRun_hook_type prev_executor_run = nullptr;
static ExecutorFinish_hook_type prev_executor_finish = nullptr;
static ExecutorEnd_hook_type prev_executor_end = nullptr;
static ProcessUtility_hook_type prev_process_utility = nullptr;

// For ProcessUtility - manual buffer/WAL snapshots (no totaltime available)
static BufferUsage utility_bufusage_start;
static WalUsage utility_walusage_start;
static instr_time utility_start_time;

// Track nesting level to identify top-level queries
static int nesting_level = 0;

// Track whether the current query started at top level
static bool current_query_is_top_level = false;

// Track query start time for duration calculation
static TimestampTz query_start_ts = 0;

// Convert PostgreSQL CmdType to our PschCmdType
static PschCmdType ConvertCmdType(CmdType cmd) {
  switch (cmd) {
    case CMD_SELECT:
      return PSCH_CMD_SELECT;
    case CMD_UPDATE:
      return PSCH_CMD_UPDATE;
    case CMD_INSERT:
      return PSCH_CMD_INSERT;
    case CMD_DELETE:
      return PSCH_CMD_DELETE;
#if PG_VERSION_NUM >= 150000
    case CMD_MERGE:
      return PSCH_CMD_MERGE;
#endif
    case CMD_UTILITY:
      return PSCH_CMD_UTILITY;
    case CMD_NOTHING:
      return PSCH_CMD_NOTHING;
    default:
      return PSCH_CMD_UNKNOWN;
  }
}

// Build a PschEvent from a QueryDesc
static void BuildEventFromQueryDesc(QueryDesc* query_desc, PschEvent* event) {
  MemSet(event, 0, sizeof(*event));

  event->ts_start = query_start_ts;
  event->dbid = MyDatabaseId;
  event->userid = GetUserId();
  event->pid = MyProcPid;
  event->queryid = query_desc->plannedstmt->queryId;
  event->top_level = current_query_is_top_level;
  event->cmd_type = ConvertCmdType(query_desc->operation);
  event->rows = query_desc->estate->es_processed;

  if (query_desc->totaltime != nullptr) {
    // Duration from instrumentation (seconds -> microseconds)
    event->duration_us =
        static_cast<uint64>(query_desc->totaltime->total * 1000000.0);

    // Buffer usage from instrumentation
    BufferUsage* buf = &query_desc->totaltime->bufusage;
    event->shared_blks_hit = buf->shared_blks_hit;
    event->shared_blks_read = buf->shared_blks_read;
    event->shared_blks_dirtied = buf->shared_blks_dirtied;
    event->shared_blks_written = buf->shared_blks_written;
    event->local_blks_hit = buf->local_blks_hit;
    event->local_blks_read = buf->local_blks_read;
    event->local_blks_dirtied = buf->local_blks_dirtied;
    event->local_blks_written = buf->local_blks_written;
    event->temp_blks_read = buf->temp_blks_read;
    event->temp_blks_written = buf->temp_blks_written;

    // I/O timing - field names changed in PG17
#if PG_VERSION_NUM >= 170000
    event->shared_blk_read_time_us =
        INSTR_TIME_GET_MICROSEC(buf->shared_blk_read_time);
    event->shared_blk_write_time_us =
        INSTR_TIME_GET_MICROSEC(buf->shared_blk_write_time);
    event->local_blk_read_time_us =
        INSTR_TIME_GET_MICROSEC(buf->local_blk_read_time);
    event->local_blk_write_time_us =
        INSTR_TIME_GET_MICROSEC(buf->local_blk_write_time);
#else
    // PG16 and earlier: blk_read_time/blk_write_time (no local block timing)
    event->shared_blk_read_time_us = INSTR_TIME_GET_MICROSEC(buf->blk_read_time);
    event->shared_blk_write_time_us =
        INSTR_TIME_GET_MICROSEC(buf->blk_write_time);
#endif
#if PG_VERSION_NUM >= 150000
    event->temp_blk_read_time_us =
        INSTR_TIME_GET_MICROSEC(buf->temp_blk_read_time);
    event->temp_blk_write_time_us =
        INSTR_TIME_GET_MICROSEC(buf->temp_blk_write_time);
#endif

    // WAL usage from instrumentation
    WalUsage* wal = &query_desc->totaltime->walusage;
    event->wal_records = wal->wal_records;
    event->wal_fpi = wal->wal_fpi;
    event->wal_bytes = wal->wal_bytes;
  } else {
    // Fallback: calculate duration from wall clock
    TimestampTz now = GetCurrentTimestamp();
    event->duration_us = static_cast<uint64>(now - query_start_ts);
  }

  // Query text
  if (query_desc->sourceText != nullptr) {
    size_t len = strlen(query_desc->sourceText);
    if (len >= PSCH_MAX_QUERY_LEN) {
      len = PSCH_MAX_QUERY_LEN - 1;
    }
    memcpy(event->query, query_desc->sourceText, len);
    event->query[len] = '\0';
    event->query_len = static_cast<uint16>(len);
  }
}

extern "C" {

static void PschExecutorStart(QueryDesc* query_desc, int eflags) {
  // Skip if this is a parallel worker
  if (IsParallelWorker()) {
    if (prev_executor_start != nullptr) {
      prev_executor_start(query_desc, eflags);
    } else {
      standard_ExecutorStart(query_desc, eflags);
    }
    return;
  }

  // Record if this is a top-level query (before nesting_level changes in Run)
  if (nesting_level == 0) {
    current_query_is_top_level = true;
    query_start_ts = GetCurrentTimestamp();
  } else {
    current_query_is_top_level = false;
  }

  // Call previous hook or standard function
  if (prev_executor_start != nullptr) {
    prev_executor_start(query_desc, eflags);
  } else {
    standard_ExecutorStart(query_desc, eflags);
  }

  // Set up instrumentation if enabled and query has a valid queryId
  if (psch_enabled && query_desc->plannedstmt->queryId != UINT64CONST(0)) {
    if (query_desc->totaltime == nullptr) {
      MemoryContext oldcxt =
          MemoryContextSwitchTo(query_desc->estate->es_query_cxt);
#if PG_VERSION_NUM < 140000
      query_desc->totaltime = InstrAlloc(1, INSTRUMENT_ALL);
#else
      query_desc->totaltime = InstrAlloc(1, INSTRUMENT_ALL, false);
#endif
      MemoryContextSwitchTo(oldcxt);
    }
  }
}

#if PG_VERSION_NUM >= 180000
static void PschExecutorRun(QueryDesc* query_desc, ScanDirection direction,
                            uint64 count) {
#else
static void PschExecutorRun(QueryDesc* query_desc, ScanDirection direction,
                            uint64 count, bool execute_once) {
#endif
  // Skip parallel workers
  if (IsParallelWorker()) {
#if PG_VERSION_NUM >= 180000
    if (prev_executor_run != nullptr) {
      prev_executor_run(query_desc, direction, count);
    } else {
      standard_ExecutorRun(query_desc, direction, count);
    }
#else
    if (prev_executor_run != nullptr) {
      prev_executor_run(query_desc, direction, count, execute_once);
    } else {
      standard_ExecutorRun(query_desc, direction, count, execute_once);
    }
#endif
    return;
  }

  nesting_level++;
  PG_TRY();
  {
#if PG_VERSION_NUM >= 180000
    if (prev_executor_run != nullptr) {
      prev_executor_run(query_desc, direction, count);
    } else {
      standard_ExecutorRun(query_desc, direction, count);
    }
#else
    if (prev_executor_run != nullptr) {
      prev_executor_run(query_desc, direction, count, execute_once);
    } else {
      standard_ExecutorRun(query_desc, direction, count, execute_once);
    }
#endif
  }
  PG_CATCH();
  {
    nesting_level--;
    PG_RE_THROW();
  }
  PG_END_TRY();
  nesting_level--;
}

static void PschExecutorFinish(QueryDesc* query_desc) {
  // Skip parallel workers
  if (IsParallelWorker()) {
    if (prev_executor_finish != nullptr) {
      prev_executor_finish(query_desc);
    } else {
      standard_ExecutorFinish(query_desc);
    }
    return;
  }

  nesting_level++;
  PG_TRY();
  {
    if (prev_executor_finish != nullptr) {
      prev_executor_finish(query_desc);
    } else {
      standard_ExecutorFinish(query_desc);
    }
  }
  PG_CATCH();
  {
    nesting_level--;
    PG_RE_THROW();
  }
  PG_END_TRY();
  nesting_level--;
}

static void PschExecutorEnd(QueryDesc* query_desc) {
  // Skip if disabled, parallel worker, or no valid queryId
  if (!psch_enabled || IsParallelWorker() ||
      query_desc->plannedstmt->queryId == UINT64CONST(0)) {
    if (prev_executor_end != nullptr) {
      prev_executor_end(query_desc);
    } else {
      standard_ExecutorEnd(query_desc);
    }
    return;
  }

  // Finalize instrumentation
  if (query_desc->totaltime != nullptr) {
    InstrEndLoop(query_desc->totaltime);
  }

  // Build and enqueue the event
  PschEvent event;
  BuildEventFromQueryDesc(query_desc, &event);
  PschEnqueueEvent(&event);

  // Call previous hook or standard function
  if (prev_executor_end != nullptr) {
    prev_executor_end(query_desc);
  } else {
    standard_ExecutorEnd(query_desc);
  }
}

// Build a PschEvent for utility statements (no QueryDesc available)
static void BuildEventForUtility(PschEvent* event, const char* queryString,
                                 TimestampTz start_ts, uint64 duration_us,
                                 bool is_top_level, uint64 rows,
                                 BufferUsage* bufusage, WalUsage* walusage) {
  MemSet(event, 0, sizeof(*event));

  event->ts_start = start_ts;
  event->duration_us = duration_us;
  event->dbid = MyDatabaseId;
  event->userid = GetUserId();
  event->pid = MyProcPid;
  event->queryid = 0;  // Utility statements don't have queryId
  event->top_level = is_top_level;
  event->cmd_type = PSCH_CMD_UTILITY;
  event->rows = rows;

  // Buffer usage from computed delta
  event->shared_blks_hit = bufusage->shared_blks_hit;
  event->shared_blks_read = bufusage->shared_blks_read;
  event->shared_blks_dirtied = bufusage->shared_blks_dirtied;
  event->shared_blks_written = bufusage->shared_blks_written;
  event->local_blks_hit = bufusage->local_blks_hit;
  event->local_blks_read = bufusage->local_blks_read;
  event->local_blks_dirtied = bufusage->local_blks_dirtied;
  event->local_blks_written = bufusage->local_blks_written;
  event->temp_blks_read = bufusage->temp_blks_read;
  event->temp_blks_written = bufusage->temp_blks_written;

  // I/O timing - field names changed in PG17
#if PG_VERSION_NUM >= 170000
  event->shared_blk_read_time_us =
      INSTR_TIME_GET_MICROSEC(bufusage->shared_blk_read_time);
  event->shared_blk_write_time_us =
      INSTR_TIME_GET_MICROSEC(bufusage->shared_blk_write_time);
  event->local_blk_read_time_us =
      INSTR_TIME_GET_MICROSEC(bufusage->local_blk_read_time);
  event->local_blk_write_time_us =
      INSTR_TIME_GET_MICROSEC(bufusage->local_blk_write_time);
#else
  event->shared_blk_read_time_us =
      INSTR_TIME_GET_MICROSEC(bufusage->blk_read_time);
  event->shared_blk_write_time_us =
      INSTR_TIME_GET_MICROSEC(bufusage->blk_write_time);
#endif
#if PG_VERSION_NUM >= 150000
  event->temp_blk_read_time_us =
      INSTR_TIME_GET_MICROSEC(bufusage->temp_blk_read_time);
  event->temp_blk_write_time_us =
      INSTR_TIME_GET_MICROSEC(bufusage->temp_blk_write_time);
#endif

  // WAL usage
  event->wal_records = walusage->wal_records;
  event->wal_fpi = walusage->wal_fpi;
  event->wal_bytes = walusage->wal_bytes;

  // Query text
  if (queryString != nullptr) {
    size_t len = strlen(queryString);
    if (len >= PSCH_MAX_QUERY_LEN) {
      len = PSCH_MAX_QUERY_LEN - 1;
    }
    memcpy(event->query, queryString, len);
    event->query[len] = '\0';
    event->query_len = static_cast<uint16>(len);
  }
}

// ProcessUtility hook - captures DDL and utility statements
#if PG_VERSION_NUM >= 140000
static void PschProcessUtility(PlannedStmt* pstmt, const char* queryString,
                               bool readOnlyTree, ProcessUtilityContext context,
                               ParamListInfo params, QueryEnvironment* queryEnv,
                               DestReceiver* dest, QueryCompletion* qc) {
#else
static void PschProcessUtility(PlannedStmt* pstmt, const char* queryString,
                               ProcessUtilityContext context,
                               ParamListInfo params, QueryEnvironment* queryEnv,
                               DestReceiver* dest, QueryCompletion* qc) {
#endif
  Node* parsetree = pstmt->utilityStmt;
  bool should_track = psch_enabled && !IsParallelWorker();

  // Skip EXECUTE/PREPARE/DEALLOCATE to avoid double-counting
  // (per pg_stat_monitor pattern)
  if (should_track && (IsA(parsetree, ExecuteStmt) ||
                       IsA(parsetree, PrepareStmt) ||
                       IsA(parsetree, DeallocateStmt))) {
    should_track = false;
  }

  if (should_track) {
    // Snapshot before execution (ProcessUtility has no totaltime)
    utility_bufusage_start = pgBufferUsage;
    utility_walusage_start = pgWalUsage;
    INSTR_TIME_SET_CURRENT(utility_start_time);

    bool is_top_level = (nesting_level == 0);
    TimestampTz start_ts = GetCurrentTimestamp();

    nesting_level++;
    PG_TRY();
    {
#if PG_VERSION_NUM >= 140000
      if (prev_process_utility) {
        prev_process_utility(pstmt, queryString, readOnlyTree, context, params,
                             queryEnv, dest, qc);
      } else {
        standard_ProcessUtility(pstmt, queryString, readOnlyTree, context,
                                params, queryEnv, dest, qc);
      }
#else
      if (prev_process_utility) {
        prev_process_utility(pstmt, queryString, context, params, queryEnv,
                             dest, qc);
      } else {
        standard_ProcessUtility(pstmt, queryString, context, params, queryEnv,
                                dest, qc);
      }
#endif
    }
    PG_CATCH();
    {
      nesting_level--;
      PG_RE_THROW();
    }
    PG_END_TRY();
    nesting_level--;

    // Calculate timing delta
    instr_time duration;
    INSTR_TIME_SET_CURRENT(duration);
    INSTR_TIME_SUBTRACT(duration, utility_start_time);

    // Calculate buffer/WAL deltas
    BufferUsage bufusage_delta;
    WalUsage walusage_delta;
    memset(&bufusage_delta, 0, sizeof(BufferUsage));
    memset(&walusage_delta, 0, sizeof(WalUsage));
    BufferUsageAccumDiff(&bufusage_delta, &pgBufferUsage,
                         &utility_bufusage_start);
    WalUsageAccumDiff(&walusage_delta, &pgWalUsage, &utility_walusage_start);

    // Row count for COPY/FETCH/SELECT/REFRESH
    uint64 rows = 0;
    if (qc && (qc->commandTag == CMDTAG_COPY || qc->commandTag == CMDTAG_FETCH ||
               qc->commandTag == CMDTAG_SELECT ||
               qc->commandTag == CMDTAG_REFRESH_MATERIALIZED_VIEW)) {
      rows = qc->nprocessed;
    }

    // Build and enqueue event
    PschEvent event;
    BuildEventForUtility(&event, queryString, start_ts,
                         INSTR_TIME_GET_MICROSEC(duration), is_top_level, rows,
                         &bufusage_delta, &walusage_delta);
    PschEnqueueEvent(&event);
  } else {
    // Not tracking - still call the utility
#if PG_VERSION_NUM >= 140000
    if (prev_process_utility) {
      prev_process_utility(pstmt, queryString, readOnlyTree, context, params,
                           queryEnv, dest, qc);
    } else {
      standard_ProcessUtility(pstmt, queryString, readOnlyTree, context, params,
                              queryEnv, dest, qc);
    }
#else
    if (prev_process_utility) {
      prev_process_utility(pstmt, queryString, context, params, queryEnv, dest,
                           qc);
    } else {
      standard_ProcessUtility(pstmt, queryString, context, params, queryEnv,
                              dest, qc);
    }
#endif
  }
}

void PschInstallHooks(void) {
#if PG_VERSION_NUM >= 140000
  // Enable query ID calculation
  EnableQueryId();
#endif

  prev_executor_start = ExecutorStart_hook;
  ExecutorStart_hook = PschExecutorStart;

  prev_executor_run = ExecutorRun_hook;
  ExecutorRun_hook = PschExecutorRun;

  prev_executor_finish = ExecutorFinish_hook;
  ExecutorFinish_hook = PschExecutorFinish;

  prev_executor_end = ExecutorEnd_hook;
  ExecutorEnd_hook = PschExecutorEnd;

  prev_process_utility = ProcessUtility_hook;
  ProcessUtility_hook = PschProcessUtility;
}

}  // extern "C"
