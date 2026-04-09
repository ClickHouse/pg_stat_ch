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

#include "parser/analyze.h"

#if PG_VERSION_NUM >= 140000
#include "nodes/queryjumble.h"
#endif

#if PG_VERSION_NUM >= 150000
#include "jit/jit.h"
#endif
}

#include "config/guc.h"
#include "hooks/hooks.h"
#include "hooks/query_normalize.h"
#include "hooks/query_normalize_state.h"
#include "queue/event.h"
#include "queue/shmem.h"

// Previous hook values for chaining
static post_parse_analyze_hook_type prev_post_parse_analyze = nullptr;
static ExecutorStart_hook_type prev_executor_start = nullptr;
static ExecutorRun_hook_type prev_executor_run = nullptr;
static ExecutorFinish_hook_type prev_executor_finish = nullptr;
static ExecutorEnd_hook_type prev_executor_end = nullptr;
static ProcessUtility_hook_type prev_process_utility = nullptr;
static emit_log_hook_type prev_emit_log_hook = nullptr;

// Per-query frame pushed at ExecutorStart / PschProcessUtility and popped at
// ExecutorEnd / end of PschProcessUtility. The stack naturally tracks nesting
// depth and provides each query with its own rusage baseline and start time,
// fixing the prior single-static design that produced overlapping CPU deltas
// for nested SPI calls.
//
// Fixed-size to avoid heap allocation: std::vector::push_back throws
// std::bad_alloc on OOM, which is incompatible with PostgreSQL's longjmp-based
// error handling. Real-world nesting depth is small; 64 levels is a generous
// ceiling even for deeply recursive plpgsql.
struct PschQueryFrame {
  struct rusage rusage_start;
  TimestampTz query_start_ts;
  uint64 queryid;  // used as parent_query_id by the next inner query
};
constexpr int kMaxQueryNestingDepth = 64;
static std::array<PschQueryFrame, kMaxQueryNestingDepth> query_stack;
static int query_stack_depth = 0;

// Deadlock prevention for emit_log_hook
static bool disable_error_capture = false;

// System initialization flag - set after hooks are installed and shmem is ready
static bool system_init = false;

static int GetClientAddress(char* buf, int buf_size);
static void ResolveNames(PschEvent* event);

static uint8 CopyName(char* dst, size_t dst_size, const char* src) {
  size_t len = strlcpy(dst, src, dst_size);
  return static_cast<uint8>(Min(len, dst_size - 1));
}

// Cache for session-stable values to avoid repeated catalog lookups on every query.
// Following pg_stat_monitor's pattern of caching client IP (pg_stat_monitor.c:73-96).
// Database name and client address never change within a session. Username is
// re-resolved when userid changes (handles SET ROLE). This also carries the
// session-local registry of normalized statements waiting to be consumed by
// ExecutorEnd, ProcessUtility, or emit_log_hook.
struct PschBackendState {
  bool initialized;
  char datname[NAMEDATALEN];
  uint8 datname_len;
  Oid cached_userid;
  char username[NAMEDATALEN];
  uint8 username_len;
  char client_addr[46];  // INET6_ADDRSTRLEN
  uint8 client_addr_len;
  PschNormalizedQueryState normalized_queries;
};
static PschBackendState backend_state = {};

// Resolve and cache the current username. On initial resolve, falls back to
// "<unknown>" if resolution fails. On SET ROLE re-resolve, keeps the existing
// cached value on failure (better to show the old name than "<unknown>") and
// leaves cached_userid unchanged so future calls can retry resolution.
static void CacheUsername(Oid userid, bool fallback_on_null) {
  const char* username = GetUserNameFromId(userid, true);
  if (username != nullptr) {
    backend_state.username_len =
        CopyName(backend_state.username, sizeof(backend_state.username), username);
    backend_state.cached_userid = userid;
  } else if (fallback_on_null) {
    backend_state.username_len =
        CopyName(backend_state.username, sizeof(backend_state.username), "<unknown>");
    backend_state.cached_userid = userid;
  }
}

// Ensure the backend cache is populated. Called on each query; the first call
// resolves datname and client_addr (session-stable), and all calls check whether
// userid changed (SET ROLE) to re-resolve username.
static void EnsureBackendCache(void) {
  Oid userid = GetUserId();

  if (!backend_state.initialized) {
    // Can't resolve catalog names outside a transaction
    if (!IsTransactionState()) {
      return;
    }

    // Database name (session-stable)
    const char* datname = get_database_name(MyDatabaseId);
    backend_state.datname_len = CopyName(backend_state.datname, sizeof(backend_state.datname),
                                         datname != nullptr ? datname : "<unknown>");

    // Client address (session-stable)
    backend_state.client_addr_len = static_cast<uint8>(
        GetClientAddress(backend_state.client_addr, sizeof(backend_state.client_addr)));

    // Username (may change via SET ROLE)
    CacheUsername(userid, true);

    backend_state.initialized = true;
    return;
  }

  // Re-resolve username if userid changed (SET ROLE)
  if (backend_state.cached_userid != userid) {
    if (IsTransactionState()) {
      CacheUsername(userid, false);
    }
  }
}

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

static size_t TrimTrailing(char* str, size_t len) {
  while (len > 0 && isspace(static_cast<unsigned char>(str[len - 1]))) {
    len--;
  }
  str[len] = '\0';
  return len;
}

// Copy string to destination, skipping leading whitespace and trimming trailing.
// This avoids memmove by skipping leading whitespace BEFORE the copy.
// Returns the final length of the trimmed string in dst.
static size_t CopyTrimmed(char* dst, size_t dst_size, const char* src) {
  if (src == nullptr || dst_size == 0) {
    if (dst_size > 0)
      dst[0] = '\0';
    return 0;
  }
  while (*src != '\0' && isspace(static_cast<unsigned char>(*src)))
    src++;
  size_t src_len = strlcpy(dst, src, dst_size);
  size_t len = Min(src_len, dst_size - 1);
  return TrimTrailing(dst, len);
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

static int GetApplicationName(char* buf, int buf_size) {
  // Try application_name GUC first (always up-to-date)
  if (application_name != nullptr && application_name[0] != '\0') {
    return static_cast<int>(CopyTrimmed(buf, buf_size, application_name));
  }

  PgBackendStatus* beentry = GetBackendStatus();
  if (beentry != nullptr && beentry->st_appname != nullptr) {
    return static_cast<int>(CopyTrimmed(buf, buf_size, beentry->st_appname));
  }

  buf[0] = '\0';
  return 0;
}

static int GetClientAddress(char* buf, int buf_size) {
  buf[0] = '\0';

  PgBackendStatus* beentry = GetBackendStatus();
  if (beentry == nullptr) {
    return 0;
  }

  std::array<char, NI_MAXHOST> remote_host{};

  int ret = pg_getnameinfo_all(&beentry->st_clientaddr.addr, beentry->st_clientaddr.salen,
                               remote_host.data(), remote_host.size(), nullptr, 0,
                               NI_NUMERICHOST | NI_NUMERICSERV);

  if (ret != 0 || remote_host[0] == '\0') {
    return 0;
  }

  // Handle local connections
  if (strcmp(remote_host.data(), "[local]") == 0) {
    size_t src_len = strlcpy(buf, "127.0.0.1", buf_size);
    return static_cast<int>(Min(src_len, static_cast<size_t>(buf_size - 1)));
  }

  size_t src_len = strlcpy(buf, remote_host.data(), buf_size);
  return static_cast<int>(Min(src_len, static_cast<size_t>(buf_size - 1)));
}

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

static void CopyWalUsage(PschEvent* event, const WalUsage* wal) {
  event->wal_records = wal->wal_records;
  event->wal_fpi = wal->wal_fpi;
  event->wal_bytes = wal->wal_bytes;
}

// Initialize PschEvent by zeroing only fixed-size fields (~550 bytes) instead of
// the full struct (~4.5KB). The two large text buffers (err_message: 2KB, query: 2KB)
// are just null-terminated since they'll be overwritten by CopyTrimmed/strlcpy or
// left at len=0.
static void InitEventPartial(PschEvent* event) {
  // Zero header: all fixed fields before err_message (~430 bytes)
  const size_t header_size = offsetof(PschEvent, err_message);
  memset(event, 0, header_size);
  event->err_message[0] = '\0';

  // Zero mid section: application_name through query_len (~114 bytes)
  const size_t mid_offset = offsetof(PschEvent, application_name);
  const size_t mid_size = offsetof(PschEvent, query) - mid_offset;
  memset(reinterpret_cast<char*>(event) + mid_offset, 0, mid_size);
  event->query[0] = '\0';
}

static void InitBaseEvent(PschEvent* event, TimestampTz ts_start, uint64 parent_query_id,
                          PschCmdType cmd_type) {
  InitEventPartial(event);
  event->ts_start = ts_start;
  event->dbid = MyDatabaseId;
  event->userid = GetUserId();
  event->pid = MyProcPid;
  event->parent_query_id = parent_query_id;
  event->cmd_type = cmd_type;
  ResolveNames(event);
}

static void CopyClientContext(PschEvent* event) {
  event->application_name_len = static_cast<uint8>(
      GetApplicationName(event->application_name, sizeof(event->application_name)));

  EnsureBackendCache();
  if (backend_state.initialized) {
    memcpy(event->client_addr, backend_state.client_addr, backend_state.client_addr_len + 1);
    event->client_addr_len = backend_state.client_addr_len;
  } else {
    event->client_addr_len =
        static_cast<uint8>(GetClientAddress(event->client_addr, sizeof(event->client_addr)));
  }
}

// Copy the original SQL text for one statement into the event buffer.
//
// PostgreSQL can hand us a multi-statement source string plus stmt_location /
// stmt_len. CleanQuerytext trims that down to just the current statement, but
// it does not parameterize literals. This helper is the raw-text fallback when
// we have no normalized entry for the statement.
static void CopyRawStatementText(PschEvent* event, const PschStatementKey& statement_key) {
  if (statement_key.source_text == nullptr) {
    return;
  }

  const char* query_text = statement_key.source_text;
  if (statement_key.stmt_location >= 0) {
    int query_loc = statement_key.stmt_location;
    int query_len = statement_key.stmt_len;
    query_text = CleanQuerytext(query_text, &query_loc, &query_len);
  }

  event->query_len = static_cast<uint16>(CopyTrimmed(event->query, PSCH_MAX_QUERY_LEN, query_text));
}

// Copy query text into the event buffer, preferring a previously normalized
// form from post_parse_analyze_hook.
//
// The normalized registry is keyed by statement identity and reused across
// repeated executions of cached plans, so this helper first looks up the
// normalized entry stashed at parse time. If no match exists, it falls back to
// CopyRawStatementText, which preserves the literal SQL text for the current
// statement only.
static void CopyQueryText(PschEvent* event, const PschStatementKey& statement_key) {
  if (PschCopyNormalizedQueryForStatement(&backend_state.normalized_queries, event->query,
                                          sizeof(event->query), &event->query_len, statement_key,
                                          false)) {
    return;
  }

  CopyRawStatementText(event, statement_key);
}

// Resolve database and user names, using the session cache when available.
// Falls back to catalog lookups if cache hasn't been initialized yet (e.g.,
// emit_log_hook fires before the first executor hook).
static void ResolveNames(PschEvent* event) {
  EnsureBackendCache();

  if (backend_state.initialized) {
    memcpy(event->datname, backend_state.datname, backend_state.datname_len + 1);
    event->datname_len = backend_state.datname_len;
    memcpy(event->username, backend_state.username, backend_state.username_len + 1);
    event->username_len = backend_state.username_len;
    return;
  }

  // Fallback: resolve fresh (cache not yet initialized, e.g. emit_log_hook early)
  const char* datname = nullptr;
  const char* username = nullptr;
  if (IsTransactionState()) {
    datname = get_database_name(event->dbid);
    username = GetUserNameFromId(event->userid, true);
  }

  event->datname_len =
      CopyName(event->datname, sizeof(event->datname), datname != nullptr ? datname : "<unknown>");
  event->username_len = CopyName(event->username, sizeof(event->username),
                                 username != nullptr ? username : "<unknown>");
}

// Copy JIT instrumentation to event (PG15+)
static void CopyJitInstrumentation([[maybe_unused]] PschEvent* event,
                                   [[maybe_unused]] QueryDesc* query_desc) {
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
#endif
}

// Copy parallel worker info to event (PG18+)
static void CopyParallelWorkerInfo([[maybe_unused]] PschEvent* event,
                                   [[maybe_unused]] QueryDesc* query_desc) {
#if PG_VERSION_NUM >= 180000
  if (query_desc->estate != nullptr) {
    event->parallel_workers_planned =
        static_cast<int16>(query_desc->estate->es_parallel_workers_to_launch);
    event->parallel_workers_launched =
        static_cast<int16>(query_desc->estate->es_parallel_workers_launched);
  }
#endif
}

static void BuildEventFromQueryDesc(QueryDesc* query_desc, PschEvent* event,
                                    TimestampTz query_start_ts, int64 cpu_user_us,
                                    int64 cpu_sys_us, uint64 parent_query_id) {
  InitBaseEvent(event, query_start_ts, parent_query_id, ConvertCmdType(query_desc->operation));
  event->queryid = query_desc->plannedstmt->queryId;
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
  const PschStatementKey statement_key =
      PschMakeStatementKey(query_desc->sourceText, query_desc->plannedstmt->stmt_location,
                           query_desc->plannedstmt->stmt_len);
  CopyQueryText(event, statement_key);
}

extern "C" {

// Remove a pending normalized entry for one statement when execution exits
// without building a normal executor/utility event from it.
static void ForgetNormalizedStatement(const char* source_text, int stmt_location, int stmt_len) {
  const PschStatementKey statement_key = PschMakeStatementKey(source_text, stmt_location, stmt_len);
  PschForgetNormalizedQueryForStatement(&backend_state.normalized_queries, statement_key);
}

// post_parse_analyze_hook — normalize query text at parse time.
// The JumbleState (with constant locations) is only available here, so we
// must generate the normalized text now and stash it for ExecutorEnd.
static void PschPostParseAnalyze(ParseState* pstate, Query* query, JumbleState* jstate) {
  if (prev_post_parse_analyze != nullptr) {
    prev_post_parse_analyze(pstate, query, jstate);
  }

  // Only normalize if enabled and the query has constants to replace.
  if (!psch_enabled || IsParallelWorker() || jstate == nullptr || jstate->clocations_count <= 0) {
    return;
  }

  const char* source_text = pstate->p_sourcetext;
  const int stmt_location = query->stmt_location;
  const int stmt_len = query->stmt_len;
  const char* query_text = source_text;
  int query_loc = stmt_location;
  int query_len = stmt_len;

  // CleanQuerytext slices a multi-statement source string down to the current
  // statement before we replace literal constants with placeholders.
  query_text = CleanQuerytext(query_text, &query_loc, &query_len);

  // Allocate in TopMemoryContext so the normalized text survives until
  // ExecutorEnd or ProcessUtility copies it into the exported event.
  MemoryContext oldcxt = MemoryContextSwitchTo(TopMemoryContext);
  char* normalized_query = PschNormalizeQuery(query_text, query_loc, &query_len, jstate);
  if (normalized_query != nullptr) {
    const PschStatementKey statement_key =
        PschMakeStatementKey(source_text, stmt_location, stmt_len);
    PschRememberNormalizedQuery(&backend_state.normalized_queries, statement_key, normalized_query,
                                query_len);
  }
  MemoryContextSwitchTo(oldcxt);
}

static void PschExecutorStart(QueryDesc* query_desc, int eflags) {
  if (IsParallelWorker()) {
    if (prev_executor_start != nullptr) {
      prev_executor_start(query_desc, eflags);
    } else {
      standard_ExecutorStart(query_desc, eflags);
    }
    return;
  }

  // Push a per-query frame. Each frame captures its own rusage baseline so
  // that nested SPI calls each get an accurate, non-overlapping CPU delta.
  // The stack depth implicitly tracks nesting; the frame below us on the stack
  // supplies the parent_query_id for this query's event.
  PschQueryFrame frame;
  frame.queryid = query_desc->plannedstmt->queryId;
  frame.query_start_ts = GetCurrentTimestamp();
  getrusage(RUSAGE_SELF, &frame.rusage_start);
  if (query_stack_depth < kMaxQueryNestingDepth) {
    query_stack[query_stack_depth] = frame;
  }
  query_stack_depth++;

  if (prev_executor_start != nullptr) {
    prev_executor_start(query_desc, eflags);
  } else {
    standard_ExecutorStart(query_desc, eflags);
  }

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
  if (prev_executor_run != nullptr) {
    prev_executor_run(query_desc, direction, count);
  } else {
    standard_ExecutorRun(query_desc, direction, count);
  }
}
#else
static void PschExecutorRun(QueryDesc* query_desc, ScanDirection direction, uint64 count,
                            bool execute_once) {
  if (prev_executor_run != nullptr) {
    prev_executor_run(query_desc, direction, count, execute_once);
  } else {
    standard_ExecutorRun(query_desc, direction, count, execute_once);
  }
}
#endif

static void PschExecutorFinish(QueryDesc* query_desc) {
  if (prev_executor_finish != nullptr) {
    prev_executor_finish(query_desc);
  } else {
    standard_ExecutorFinish(query_desc);
  }
}

static void PschExecutorEnd(QueryDesc* query_desc) {
  // Parallel workers never push a frame — handle them separately.
  if (IsParallelWorker()) {
    ForgetNormalizedStatement(query_desc->sourceText, query_desc->plannedstmt->stmt_location,
                              query_desc->plannedstmt->stmt_len);
    if (prev_executor_end != nullptr) {
      prev_executor_end(query_desc);
    } else {
      standard_ExecutorEnd(query_desc);
    }
    return;
  }

  // Pop the frame pushed in ExecutorStart. If depth exceeded the array cap,
  // the frame is zeroed — the event is still emitted but with zero CPU.
  PschQueryFrame frame = {};
  if (query_stack_depth > 0) {
    query_stack_depth--;
    if (query_stack_depth < kMaxQueryNestingDepth) {
      frame = query_stack[query_stack_depth];
    }
  }

  if (!psch_enabled || query_desc->plannedstmt->queryId == UINT64CONST(0)) {
    ForgetNormalizedStatement(query_desc->sourceText, query_desc->plannedstmt->stmt_location,
                              query_desc->plannedstmt->stmt_len);
    if (prev_executor_end != nullptr) {
      prev_executor_end(query_desc);
    } else {
      standard_ExecutorEnd(query_desc);
    }
    return;
  }

  if (query_desc->totaltime != nullptr) {
    InstrEndLoop(query_desc->totaltime);
  }

  int64 cpu_user_us = 0;
  int64 cpu_sys_us = 0;
  struct rusage rusage_end;
  if (getrusage(RUSAGE_SELF, &rusage_end) == 0) {
    cpu_user_us = TimeDiffMicrosec(rusage_end.ru_utime, frame.rusage_start.ru_utime);
    cpu_sys_us = TimeDiffMicrosec(rusage_end.ru_stime, frame.rusage_start.ru_stime);
  }

  // The frame below us on the stack is our caller; its queryid is our parent.
  int parent_idx = query_stack_depth - 1;
  uint64 parent_query_id =
      (parent_idx >= 0 && parent_idx < kMaxQueryNestingDepth) ? query_stack[parent_idx].queryid : 0;

  PschEvent event;
  BuildEventFromQueryDesc(query_desc, &event, frame.query_start_ts, cpu_user_us, cpu_sys_us,
                          parent_query_id);
  PschEnqueueEvent(&event);

  if (prev_executor_end != nullptr) {
    prev_executor_end(query_desc);
  } else {
    standard_ExecutorEnd(query_desc);
  }
}

// Build a PschEvent for utility statements (no QueryDesc available)
static void BuildEventForUtility(PschEvent* event, const char* queryString, TimestampTz start_ts,
                                 int stmt_location, int stmt_len, uint64 duration_us,
                                 uint64 parent_query_id, uint64 rows, BufferUsage* bufusage,
                                 WalUsage* walusage, int64 cpu_user_us, int64 cpu_sys_us) {
  InitBaseEvent(event, start_ts, parent_query_id, PSCH_CMD_UTILITY);
  event->duration_us = duration_us;
  event->rows = rows;
  event->cpu_user_time_us = cpu_user_us;
  event->cpu_sys_time_us = cpu_sys_us;

  CopyBufferUsage(event, bufusage);
  CopyIoTiming(event, bufusage);
  CopyWalUsage(event, walusage);
  CopyClientContext(event);
  const PschStatementKey statement_key = PschMakeStatementKey(queryString, stmt_location, stmt_len);
  CopyQueryText(event, statement_key);
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
    int stmt_location = pstmt->stmt_location;
    int stmt_len = pstmt->stmt_len;
    CALL_PROCESS_UTILITY();
    ForgetNormalizedStatement(queryString, stmt_location, stmt_len);
    return;
  }

  // Capture parent before pushing our own frame, then push so that any
  // executor hooks fired from within this utility (e.g. CREATE TABLE AS SELECT)
  // see us on the stack and correctly link themselves as our children.
  int parent_idx = query_stack_depth - 1;
  uint64 parent_query_id =
      (parent_idx >= 0 && parent_idx < kMaxQueryNestingDepth) ? query_stack[parent_idx].queryid : 0;

  PschQueryFrame frame;
  frame.queryid = 0;  // utility statements have no queryId
  frame.query_start_ts = GetCurrentTimestamp();
  getrusage(RUSAGE_SELF, &frame.rusage_start);
  if (query_stack_depth < kMaxQueryNestingDepth) {
    query_stack[query_stack_depth] = frame;
  }
  query_stack_depth++;

  int stmt_location = pstmt->stmt_location;
  int stmt_len = pstmt->stmt_len;
  BufferUsage bufusage_start = pgBufferUsage;
  WalUsage walusage_start = pgWalUsage;
  instr_time start_time;
  INSTR_TIME_SET_CURRENT(start_time);

  CALL_PROCESS_UTILITY();

  instr_time duration;
  INSTR_TIME_SET_CURRENT(duration);
  INSTR_TIME_SUBTRACT(duration, start_time);

  // Pop our frame and compute CPU delta. Zeroed if depth exceeded the cap.
  PschQueryFrame popped = {};
  if (query_stack_depth > 0) {
    query_stack_depth--;
    if (query_stack_depth < kMaxQueryNestingDepth) {
      popped = query_stack[query_stack_depth];
    }
  }


  BufferUsage bufusage_delta;
  WalUsage walusage_delta;
  MemSet(&bufusage_delta, 0, sizeof(BufferUsage));
  MemSet(&walusage_delta, 0, sizeof(WalUsage));
  BufferUsageAccumDiff(&bufusage_delta, &pgBufferUsage, &bufusage_start);
  WalUsageAccumDiff(&walusage_delta, &pgWalUsage, &walusage_start);

  int64 cpu_user_us = 0;
  int64 cpu_sys_us = 0;
  struct rusage rusage_end;
  if (getrusage(RUSAGE_SELF, &rusage_end) == 0) {
    cpu_user_us = TimeDiffMicrosec(rusage_end.ru_utime, popped.rusage_start.ru_utime);
    cpu_sys_us = TimeDiffMicrosec(rusage_end.ru_stime, popped.rusage_start.ru_stime);
  }

  PschEvent event;
  BuildEventForUtility(&event, queryString, popped.query_start_ts, stmt_location, stmt_len,
                       INSTR_TIME_GET_MICROSEC(duration), parent_query_id, GetUtilityRowCount(qc),
                       &bufusage_delta, &walusage_delta, cpu_user_us, cpu_sys_us);
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

// Build and enqueue an error event from ErrorData.
//
// We intentionally leave event.query empty here. emit_log_hook only exposes
// debug_query_string and cursor position, not the exact statement identity used
// by ExecutorEnd/ProcessUtility, so reconstructing normalized SQL required
// fuzzy matching and extra backend-local state. Error events still carry the
// message, SQLSTATE, and client/session metadata.
static void CaptureLogEvent(ErrorData* edata) {
  PschEvent event;
  uint64 parent_query_id = query_stack_depth > 0 ? query_stack[query_stack_depth - 1].queryid : 0;
  InitBaseEvent(&event, GetCurrentTimestamp(), parent_query_id, PSCH_CMD_UNKNOWN);

  UnpackSqlState(edata->sqlerrcode, event.err_sqlstate);
  event.err_elevel = static_cast<uint8>(edata->elevel);

  if (edata->message != nullptr) {
    event.err_message_len =
        static_cast<uint16>(CopyTrimmed(event.err_message, PSCH_MAX_ERR_MSG_LEN, edata->message));
  }

  CopyClientContext(&event);

  // Enqueue with recursion guard
  disable_error_capture = true;
  PschEnqueueEvent(&event);
  disable_error_capture = false;
}

// emit_log_hook - captures log messages at configured level and above
//
// CHAINING ORDER: We chain to the previous hook FIRST to allow other extensions
// (e.g., log formatters, filters) to transform ErrorData before we capture it.
// This ensures we capture the final, potentially modified log message.
static void PschEmitLogHook(ErrorData* edata) {
  // Chain to previous hook first (allows log transformation by other extensions)
  if (prev_emit_log_hook != nullptr) {
    prev_emit_log_hook(edata);
  }

  // Capture the (potentially transformed) event
  if (ShouldCaptureLog(edata)) {
    CaptureLogEvent(edata);
  }

  // Reset recursion guard on ERROR+ (transaction will abort)
  if (edata != nullptr && edata->elevel >= ERROR) {
    disable_error_capture = false;
  }
}

void PschSuppressErrorCapture(bool suppress) {
  disable_error_capture = suppress;
}

void PschInstallHooks(void) {
#if PG_VERSION_NUM >= 140000
  EnableQueryId();
#endif

  prev_post_parse_analyze = post_parse_analyze_hook;
  post_parse_analyze_hook = PschPostParseAnalyze;

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
