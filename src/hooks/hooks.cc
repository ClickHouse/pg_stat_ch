// pg_stat_ch executor hooks implementation

extern "C" {
#include "postgres.h"

#include "access/parallel.h"
#include "executor/executor.h"
#include "executor/instrument.h"
#include "miscadmin.h"
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

  // Timing
  event->ts_start = query_start_ts;
  if (query_desc->totaltime != nullptr) {
    // totaltime->total is in seconds (double), convert to microseconds
    event->duration_us =
        static_cast<uint64>(query_desc->totaltime->total * 1000000.0);
  } else {
    // Fallback: calculate from wall clock
    TimestampTz now = GetCurrentTimestamp();
    event->duration_us = static_cast<uint64>(now - query_start_ts);
  }

  // Identity
  event->dbid = MyDatabaseId;
  event->userid = GetUserId();
  event->pid = MyProcPid;
  event->queryid = query_desc->plannedstmt->queryId;
  event->top_level = current_query_is_top_level;
  event->cmd_type = ConvertCmdType(query_desc->operation);

  // Results
  event->rows = query_desc->estate->es_processed;

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
}

}  // extern "C"
