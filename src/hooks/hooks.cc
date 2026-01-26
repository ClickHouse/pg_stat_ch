// pg_stat_ch executor hooks implementation

#include <array>
#include <sys/resource.h>

extern "C" {
#include "postgres.h"

#include "access/parallel.h"
#include "access/xact.h"
#include "commands/dbcommands.h"
#include "common/ip.h"
#include "executor/executor.h"
#include "executor/instrument.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "tcop/utility.h"
#include "utils/backend_status.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"

#if PG_VERSION_NUM >= 140000
#include "nodes/queryjumble.h"
#endif

#if PG_VERSION_NUM >= 150000
#include "jit/jit.h"
#endif
}

#include "config/guc.h"
#include "hooks/hooks.h"
#include "queue/event.h"
#include "queue/shmem.h"

// Previous hook values for chaining
static ExecutorStart_hook_type prev_executor_start = nullptr;
static ExecutorRun_hook_type prev_executor_run = nullptr;
static ExecutorFinish_hook_type prev_executor_finish = nullptr;
static ExecutorEnd_hook_type prev_executor_end = nullptr;
static ProcessUtility_hook_type prev_process_utility = nullptr;
static emit_log_hook_type prev_emit_log_hook = nullptr;

// Track nesting level to identify top-level queries
static int nesting_level = 0;

// CPU time tracking via getrusage
static struct rusage rusage_start;

// Deadlock prevention for emit_log_hook
static bool disable_error_capture = false;

// Track whether the current query started at top level
static bool current_query_is_top_level = false;

// Track query start time for duration calculation
static TimestampTz query_start_ts = 0;

// System initialization flag - set after hooks are installed and shmem is ready
static bool system_init = false;

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

// Calculate time difference in microseconds
static int64 TimeDiffMicrosec(struct timeval end, struct timeval start) {
  return (static_cast<int64>(end.tv_sec - start.tv_sec) * 1000000LL) +
         static_cast<int64>(end.tv_usec - start.tv_usec);
}

// Unpack SQLSTATE code from PostgreSQL's packed format to string
static void UnpackSqlState(int sql_state, char* buf) {
  for (int i = 0; i < 5; i++) {
    buf[i] = PGUNSIXBIT(sql_state);
    sql_state >>= 6;
  }
  buf[5] = '\0';
}

// Trim leading and trailing whitespace in-place, return new length
static size_t TrimWhitespace(char* str, size_t len) {
  // Trim trailing first (no memmove needed)
  while (len > 0 && isspace(static_cast<unsigned char>(str[len - 1]))) {
    len--;
  }
  str[len] = '\0';

  // Trim leading (requires memmove if any)
  size_t start = 0;
  while (start < len && isspace(static_cast<unsigned char>(str[start]))) {
    start++;
  }
  if (start > 0) {
    len -= start;
    memmove(str, str + start, len + 1);  // +1 for null terminator
  }
  return len;
}

// Get PgBackendStatus for the current backend (version-compatible)
static PgBackendStatus* GetBackendStatus(void) {
#if PG_VERSION_NUM >= 170000
  return pgstat_get_beentry_by_proc_number(MyProcNumber);
#else
  // PG16 and earlier: iterate through all backends
  LocalPgBackendStatus* local_beentry;
  int num_backends = pgstat_fetch_stat_numbackends();
  for (int i = 1; i <= num_backends; i++) {
    local_beentry = pgstat_get_local_beentry_by_index(i);
    if (local_beentry == nullptr) {
      continue;
    }
    PgBackendStatus* beentry = &local_beentry->backendStatus;
    if (beentry->st_procpid == MyProcPid) {
      return beentry;
    }
  }
  return nullptr;
#endif
}

// Get application name for current backend
// Returns the length of the string copied to buf
static int GetApplicationName(char* buf, int buf_size) {
  int len;
  // Try application_name GUC first (always up-to-date)
  if (application_name != nullptr && application_name[0] != '\0') {
    len = snprintf(buf, buf_size, "%s", application_name);
    len = (len >= buf_size) ? buf_size - 1 : len;
    return static_cast<int>(TrimWhitespace(buf, len));
  }

  // Fall back to backend status
  PgBackendStatus* beentry = GetBackendStatus();
  if (beentry != nullptr && beentry->st_appname != nullptr) {
    len = snprintf(buf, buf_size, "%s", beentry->st_appname);
    len = (len >= buf_size) ? buf_size - 1 : len;
    return static_cast<int>(TrimWhitespace(buf, len));
  }

  buf[0] = '\0';
  return 0;
}

// Get client address for current backend as a string
// Returns the length of the string copied to buf
static int GetClientAddress(char* buf, int buf_size) {
  buf[0] = '\0';

  PgBackendStatus* beentry = GetBackendStatus();
  if (beentry == nullptr) {
    return 0;
  }

  // Get the client address as a string
  std::array<char, NI_MAXHOST> remote_host{};

  int ret = pg_getnameinfo_all(&beentry->st_clientaddr.addr, beentry->st_clientaddr.salen,
                               remote_host.data(), remote_host.size(), nullptr, 0,
                               NI_NUMERICHOST | NI_NUMERICSERV);

  if (ret != 0 || remote_host[0] == '\0') {
    return 0;
  }

  // Handle local connections
  if (strcmp(remote_host.data(), "[local]") == 0) {
    int len = snprintf(buf, buf_size, "127.0.0.1");
    return (len >= buf_size) ? buf_size - 1 : len;
  }

  int len = snprintf(buf, buf_size, "%s", remote_host.data());
  return (len >= buf_size) ? buf_size - 1 : len;
}

// Copy buffer usage counters to event
static void CopyBufferUsage(PschEvent* event, const BufferUsage* buf) {
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
}

// Copy I/O timing to event (version-aware)
static void CopyIoTiming(PschEvent* event, const BufferUsage* buf) {
#if PG_VERSION_NUM >= 170000
  event->shared_blk_read_time_us = INSTR_TIME_GET_MICROSEC(buf->shared_blk_read_time);
  event->shared_blk_write_time_us = INSTR_TIME_GET_MICROSEC(buf->shared_blk_write_time);
  event->local_blk_read_time_us = INSTR_TIME_GET_MICROSEC(buf->local_blk_read_time);
  event->local_blk_write_time_us = INSTR_TIME_GET_MICROSEC(buf->local_blk_write_time);
#else
  // PG16 and earlier: blk_read_time/blk_write_time (no local block timing)
  event->shared_blk_read_time_us = INSTR_TIME_GET_MICROSEC(buf->blk_read_time);
  event->shared_blk_write_time_us = INSTR_TIME_GET_MICROSEC(buf->blk_write_time);
#endif
#if PG_VERSION_NUM >= 150000
  event->temp_blk_read_time_us = INSTR_TIME_GET_MICROSEC(buf->temp_blk_read_time);
  event->temp_blk_write_time_us = INSTR_TIME_GET_MICROSEC(buf->temp_blk_write_time);
#endif
}

// Copy WAL usage to event
static void CopyWalUsage(PschEvent* event, const WalUsage* wal) {
  event->wal_records = wal->wal_records;
  event->wal_fpi = wal->wal_fpi;
  event->wal_bytes = wal->wal_bytes;
}

// Copy client context (application name, client address) to event
static void CopyClientContext(PschEvent* event) {
  event->application_name_len = static_cast<uint8>(
      GetApplicationName(event->application_name, sizeof(event->application_name)));
  event->client_addr_len =
      static_cast<uint8>(GetClientAddress(event->client_addr, sizeof(event->client_addr)));
}

// Copy query text to event with truncation
static void CopyQueryText(PschEvent* event, const char* query_text) {
  if (query_text != nullptr) {
    size_t len = strlen(query_text);
    if (len >= PSCH_MAX_QUERY_LEN) {
      len = PSCH_MAX_QUERY_LEN - 1;
    }
    memcpy(event->query, query_text, len);
    event->query[len] = '\0';
    // Trim whitespace
    event->query_len = static_cast<uint16>(TrimWhitespace(event->query, len));
  }
}

// Resolve database and user names from OIDs (pg_stat_monitor pattern)
// Must be called from a transaction context (foreground hooks)
static void ResolveNames(PschEvent* event) {
  const char* datname = nullptr;
  const char* username = nullptr;

  // Only resolve names if we're in a transaction state (catalog access is safe)
  if (IsTransactionState()) {
    datname = get_database_name(event->dbid);
    username = GetUserNameFromId(event->userid, true);
  }

  // Fall back to placeholder if name resolution fails
  if (datname == nullptr) {
    datname = "<unknown>";
  }
  if (username == nullptr) {
    username = "<unknown>";
  }

  // Copy names to event with length tracking
  size_t dlen = strlen(datname);
  if (dlen >= sizeof(event->datname)) {
    dlen = sizeof(event->datname) - 1;
  }
  memcpy(event->datname, datname, dlen);
  event->datname[dlen] = '\0';
  event->datname_len = static_cast<uint8>(dlen);

  size_t ulen = strlen(username);
  if (ulen >= sizeof(event->username)) {
    ulen = sizeof(event->username) - 1;
  }
  memcpy(event->username, username, ulen);
  event->username[ulen] = '\0';
  event->username_len = static_cast<uint8>(ulen);

  // Note: get_database_name returns palloc'd memory that will be freed at end of
  // transaction, GetUserNameFromId also returns palloc'd memory. We don't need to
  // explicitly pfree since we're in a transaction context and using the memory
  // immediately.
}

// Copy JIT instrumentation to event (PG15+)
static void CopyJitInstrumentation(PschEvent* event, QueryDesc* query_desc) {
#if PG_VERSION_NUM >= 150000
  if (query_desc->estate->es_jit != nullptr) {
    JitInstrumentation* jit = &query_desc->estate->es_jit->instr;
    event->jit_functions = static_cast<int32>(jit->created_functions);
    event->jit_generation_time_us =
        static_cast<int32>(INSTR_TIME_GET_MICROSEC(jit->generation_counter));
    event->jit_inlining_time_us =
        static_cast<int32>(INSTR_TIME_GET_MICROSEC(jit->inlining_counter));
    event->jit_optimization_time_us =
        static_cast<int32>(INSTR_TIME_GET_MICROSEC(jit->optimization_counter));
    event->jit_emission_time_us =
        static_cast<int32>(INSTR_TIME_GET_MICROSEC(jit->emission_counter));
#if PG_VERSION_NUM >= 170000
    event->jit_deform_time_us = static_cast<int32>(INSTR_TIME_GET_MICROSEC(jit->deform_counter));
#endif
  }
#else
  (void)event;
  (void)query_desc;
#endif
}

// Copy parallel worker info to event (PG18+)
static void CopyParallelWorkerInfo(PschEvent* event, QueryDesc* query_desc) {
#if PG_VERSION_NUM >= 180000
  if (query_desc->estate != nullptr) {
    event->parallel_workers_planned =
        static_cast<int16>(query_desc->estate->es_parallel_workers_to_launch);
    event->parallel_workers_launched =
        static_cast<int16>(query_desc->estate->es_parallel_workers_launched);
  }
#else
  (void)event;
  (void)query_desc;
#endif
}

// Build a PschEvent from a QueryDesc
static void BuildEventFromQueryDesc(QueryDesc* query_desc, PschEvent* event, int64 cpu_user_us,
                                    int64 cpu_sys_us) {
  MemSet(event, 0, sizeof(*event));

  // Basic identification
  event->ts_start = query_start_ts;
  event->dbid = MyDatabaseId;
  event->userid = GetUserId();
  ResolveNames(event);
  event->pid = MyProcPid;
  event->queryid = query_desc->plannedstmt->queryId;
  event->top_level = current_query_is_top_level;
  event->cmd_type = ConvertCmdType(query_desc->operation);
  event->rows = query_desc->estate->es_processed;
  event->cpu_user_time_us = cpu_user_us;
  event->cpu_sys_time_us = cpu_sys_us;

  // Instrumentation data (duration, buffer, WAL)
  if (query_desc->totaltime != nullptr) {
#if PG_VERSION_NUM >= 190000
    event->duration_us = static_cast<uint64>(INSTR_TIME_GET_MICROSEC(query_desc->totaltime->total));
#else
    event->duration_us = static_cast<uint64>(query_desc->totaltime->total * 1000000.0);
#endif
    CopyBufferUsage(event, &query_desc->totaltime->bufusage);
    CopyIoTiming(event, &query_desc->totaltime->bufusage);
    CopyWalUsage(event, &query_desc->totaltime->walusage);
  } else {
    event->duration_us = static_cast<uint64>(GetCurrentTimestamp() - query_start_ts);
  }

  CopyJitInstrumentation(event, query_desc);
  CopyParallelWorkerInfo(event, query_desc);
  CopyClientContext(event);
  CopyQueryText(event, query_desc->sourceText);
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
    // Capture CPU time baseline for top-level queries
    if (psch_enabled) {
      getrusage(RUSAGE_SELF, &rusage_start);
    }
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
      MemoryContext oldcxt = MemoryContextSwitchTo(query_desc->estate->es_query_cxt);
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
static void PschExecutorRun(QueryDesc* query_desc, ScanDirection direction, uint64 count) {
#else
static void PschExecutorRun(QueryDesc* query_desc, ScanDirection direction, uint64 count,
                            bool execute_once) {
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
  PG_FINALLY();
  { nesting_level--; }
  PG_END_TRY();
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
  PG_FINALLY();
  { nesting_level--; }
  PG_END_TRY();
}

static void PschExecutorEnd(QueryDesc* query_desc) {
  // Skip if disabled, parallel worker, or no valid queryId
  if (!psch_enabled || IsParallelWorker() || query_desc->plannedstmt->queryId == UINT64CONST(0)) {
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

  // Compute CPU time delta from getrusage
  int64 cpu_user_us = 0;
  int64 cpu_sys_us = 0;
  struct rusage rusage_end;
  if (getrusage(RUSAGE_SELF, &rusage_end) == 0) {
    cpu_user_us = TimeDiffMicrosec(rusage_end.ru_utime, rusage_start.ru_utime);
    cpu_sys_us = TimeDiffMicrosec(rusage_end.ru_stime, rusage_start.ru_stime);
  }

  // Build and enqueue the event
  PschEvent event;
  BuildEventFromQueryDesc(query_desc, &event, cpu_user_us, cpu_sys_us);
  PschEnqueueEvent(&event);

  // Call previous hook or standard function
  if (prev_executor_end != nullptr) {
    prev_executor_end(query_desc);
  } else {
    standard_ExecutorEnd(query_desc);
  }
}

// Build a PschEvent for utility statements (no QueryDesc available)
static void BuildEventForUtility(PschEvent* event, const char* queryString, TimestampTz start_ts,
                                 uint64 duration_us, bool is_top_level, uint64 rows,
                                 BufferUsage* bufusage, WalUsage* walusage, int64 cpu_user_us,
                                 int64 cpu_sys_us) {
  MemSet(event, 0, sizeof(*event));

  // Basic identification
  event->ts_start = start_ts;
  event->duration_us = duration_us;
  event->dbid = MyDatabaseId;
  event->userid = GetUserId();
  ResolveNames(event);
  event->pid = MyProcPid;
  event->queryid = 0;  // Utility statements don't have queryId
  event->top_level = is_top_level;
  event->cmd_type = PSCH_CMD_UTILITY;
  event->rows = rows;
  event->cpu_user_time_us = cpu_user_us;
  event->cpu_sys_time_us = cpu_sys_us;

  CopyBufferUsage(event, bufusage);
  CopyIoTiming(event, bufusage);
  CopyWalUsage(event, walusage);
  CopyClientContext(event);
  CopyQueryText(event, queryString);
}

// Helper macro to call ProcessUtility (previous hook or standard)
#if PG_VERSION_NUM >= 140000
#define CALL_PROCESS_UTILITY()                                                                     \
  do {                                                                                             \
    if (prev_process_utility) {                                                                    \
      prev_process_utility(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, qc); \
    } else {                                                                                       \
      standard_ProcessUtility(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest,   \
                              qc);                                                                 \
    }                                                                                              \
  } while (0)
#else
#define CALL_PROCESS_UTILITY()                                                          \
  do {                                                                                  \
    if (prev_process_utility) {                                                         \
      prev_process_utility(pstmt, queryString, context, params, queryEnv, dest, qc);    \
    } else {                                                                            \
      standard_ProcessUtility(pstmt, queryString, context, params, queryEnv, dest, qc); \
    }                                                                                   \
  } while (0)
#endif

// Check if we should track this utility statement
static bool ShouldTrackUtility(Node* parsetree) {
  if (!psch_enabled || IsParallelWorker()) {
    return false;
  }
  // Skip EXECUTE/PREPARE/DEALLOCATE to avoid double-counting
  if (IsA(parsetree, ExecuteStmt) || IsA(parsetree, PrepareStmt) ||
      IsA(parsetree, DeallocateStmt)) {
    return false;
  }
  return true;
}

// Get row count from QueryCompletion for utility statements
static uint64 GetUtilityRowCount(QueryCompletion* qc) {
  if (qc == nullptr) {
    return 0;
  }
  switch (qc->commandTag) {
    case CMDTAG_COPY:
    case CMDTAG_FETCH:
    case CMDTAG_SELECT:
    case CMDTAG_REFRESH_MATERIALIZED_VIEW:
      return qc->nprocessed;
    default:
      return 0;
  }
}

// Execute utility with nesting level tracking
static void ExecuteUtilityWithNesting(PlannedStmt* pstmt, const char* queryString,
#if PG_VERSION_NUM >= 140000
                                      bool readOnlyTree,
#endif
                                      ProcessUtilityContext context, ParamListInfo params,
                                      QueryEnvironment* queryEnv, DestReceiver* dest,
                                      QueryCompletion* qc) {
  nesting_level++;
  PG_TRY();
  { CALL_PROCESS_UTILITY(); }
  PG_FINALLY();
  { nesting_level--; }
  PG_END_TRY();
}

// ProcessUtility hook - captures DDL and utility statements
#if PG_VERSION_NUM >= 140000
static void PschProcessUtility(PlannedStmt* pstmt, const char* queryString, bool readOnlyTree,
                               ProcessUtilityContext context, ParamListInfo params,
                               QueryEnvironment* queryEnv, DestReceiver* dest,
                               QueryCompletion* qc) {
#else
static void PschProcessUtility(PlannedStmt* pstmt, const char* queryString,
                               ProcessUtilityContext context, ParamListInfo params,
                               QueryEnvironment* queryEnv, DestReceiver* dest,
                               QueryCompletion* qc) {
#endif
  if (!ShouldTrackUtility(pstmt->utilityStmt)) {
    CALL_PROCESS_UTILITY();
    return;
  }

  // Capture state before execution
  bool is_top_level = (nesting_level == 0);
  TimestampTz start_ts = GetCurrentTimestamp();
  BufferUsage bufusage_start = pgBufferUsage;
  WalUsage walusage_start = pgWalUsage;
  struct rusage rusage_util_start;
  getrusage(RUSAGE_SELF, &rusage_util_start);
  instr_time start_time;
  INSTR_TIME_SET_CURRENT(start_time);

  // Execute the utility
#if PG_VERSION_NUM >= 140000
  ExecuteUtilityWithNesting(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, qc);
#else
  ExecuteUtilityWithNesting(pstmt, queryString, context, params, queryEnv, dest, qc);
#endif

  // Calculate duration
  instr_time duration;
  INSTR_TIME_SET_CURRENT(duration);
  INSTR_TIME_SUBTRACT(duration, start_time);

  // Calculate buffer/WAL deltas
  BufferUsage bufusage_delta;
  WalUsage walusage_delta;
  memset(&bufusage_delta, 0, sizeof(BufferUsage));
  memset(&walusage_delta, 0, sizeof(WalUsage));
  BufferUsageAccumDiff(&bufusage_delta, &pgBufferUsage, &bufusage_start);
  WalUsageAccumDiff(&walusage_delta, &pgWalUsage, &walusage_start);

  // Calculate CPU time delta
  int64 cpu_user_us = 0;
  int64 cpu_sys_us = 0;
  struct rusage rusage_util_end;
  if (getrusage(RUSAGE_SELF, &rusage_util_end) == 0) {
    cpu_user_us = TimeDiffMicrosec(rusage_util_end.ru_utime, rusage_util_start.ru_utime);
    cpu_sys_us = TimeDiffMicrosec(rusage_util_end.ru_stime, rusage_util_start.ru_stime);
  }

  // Build and enqueue event
  PschEvent event;
  BuildEventForUtility(&event, queryString, start_ts, INSTR_TIME_GET_MICROSEC(duration),
                       is_top_level, GetUtilityRowCount(qc), &bufusage_delta, &walusage_delta,
                       cpu_user_us, cpu_sys_us);
  PschEnqueueEvent(&event);
}

#undef CALL_PROCESS_UTILITY

// Check if log capture should occur for this error.
// Returns false during early initialization, in background workers, or when disabled.
static bool ShouldCaptureLog(ErrorData* edata) {
  // Basic preconditions
  if (edata == nullptr || !system_init || !psch_enabled || disable_error_capture) {
    return false;
  }

  // Check error level threshold
  if (edata->elevel < psch_log_min_elevel) {
    return false;
  }

  // PostgreSQL bootstrapping checks - MyProc indicates PGPROC allocation complete
  if (MyProc == nullptr || IsParallelWorker()) {
    return false;
  }

  // Session initialization checks (critical for debug5 safety):
  // - MyDatabaseId: database must be assigned
  // - IsUnderPostmaster: not single-user mode or bootstrap
  // - psch_shared_state: shared memory must be ready
  // - MyBgworkerEntry: skip background workers (not user queries)
  if (MyDatabaseId == InvalidOid || !IsUnderPostmaster || psch_shared_state == nullptr ||
      MyBgworkerEntry != nullptr) {
    return false;
  }

  return true;
}

// Build and enqueue an error event from ErrorData
static void CaptureLogEvent(ErrorData* edata) {
  const char* query = (debug_query_string != nullptr) ? debug_query_string : "";

  PschEvent event;
  MemSet(&event, 0, sizeof(event));

  // Basic fields
  event.ts_start = GetCurrentTimestamp();
  event.dbid = MyDatabaseId;
  event.userid = GetUserId();
  event.pid = MyProcPid;
  event.top_level = (nesting_level == 0);
  event.cmd_type = PSCH_CMD_UNKNOWN;

  // Error details
  UnpackSqlState(edata->sqlerrcode, event.err_sqlstate);
  event.err_elevel = static_cast<uint8>(edata->elevel);

  // Error message
  if (edata->message != nullptr) {
    size_t len = strlen(edata->message);
    if (len >= PSCH_MAX_ERR_MSG_LEN) {
      len = PSCH_MAX_ERR_MSG_LEN - 1;
    }
    memcpy(event.err_message, edata->message, len);
    event.err_message[len] = '\0';
    event.err_message_len = static_cast<uint16>(TrimWhitespace(event.err_message, len));
  }

  // Query text
  if (query[0] != '\0') {
    size_t len = strlen(query);
    if (len >= PSCH_MAX_QUERY_LEN) {
      len = PSCH_MAX_QUERY_LEN - 1;
    }
    memcpy(event.query, query, len);
    event.query[len] = '\0';
    event.query_len = static_cast<uint16>(TrimWhitespace(event.query, len));
  }

  // Resolve names and client context
  ResolveNames(&event);
  event.application_name_len = static_cast<uint8>(
      GetApplicationName(event.application_name, sizeof(event.application_name)));
  event.client_addr_len =
      static_cast<uint8>(GetClientAddress(event.client_addr, sizeof(event.client_addr)));

  // Enqueue with recursion guard
  disable_error_capture = true;
  PschEnqueueEvent(&event);
  disable_error_capture = false;
}

// emit_log_hook - captures log messages at configured level and above
static void PschEmitLogHook(ErrorData* edata) {
  if (ShouldCaptureLog(edata)) {
    CaptureLogEvent(edata);
  }

  // Reset recursion guard on ERROR+ (transaction will abort)
  if (edata != nullptr && edata->elevel >= ERROR) {
    disable_error_capture = false;
  }

  // Chain to previous hook
  if (prev_emit_log_hook != nullptr) {
    prev_emit_log_hook(edata);
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

  prev_emit_log_hook = emit_log_hook;
  emit_log_hook = PschEmitLogHook;

  // Mark system as initialized - emit_log_hook will now capture messages
  system_init = true;
}

}  // extern "C"
