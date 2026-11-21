// pg_stat_ch executor hooks implementation

#include <sys/resource.h>

#include "postgres.h"

#include "access/parallel.h"
#include "access/xact.h"
#include "commands/dbcommands.h"
#include "common/ip.h"
#include "common/pg_prng.h"
#include "executor/executor.h"
#include "executor/instrument.h"
#include "jit/jit.h"
#include "libpq/auth.h"
#include "libpq/libpq-be.h"
#include "miscadmin.h"
#include "nodes/queryjumble.h"
#include "parser/analyze.h"
#include "postmaster/bgworker.h"
#include "storage/proc.h"
#include "tcop/utility.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

#include "compat.h"
#include "config/guc.h"
#include "hooks/hooks.h"
#include "hooks/query_normalize.h"
#include "hooks/query_normalize_state.h"
#include "hooks/string_utils.h"
#include "queue/event.h"
#include "queue/local_batch.h"
#include "queue/psch_dsa.h"
#include "queue/shmem.h"

// Previous hook values for chaining
static post_parse_analyze_hook_type prev_post_parse_analyze = NULL;
static ExecutorStart_hook_type prev_executor_start = NULL;
static ExecutorRun_hook_type prev_executor_run = NULL;
static ExecutorFinish_hook_type prev_executor_finish = NULL;
static ExecutorEnd_hook_type prev_executor_end = NULL;
static ProcessUtility_hook_type prev_process_utility = NULL;
static emit_log_hook_type prev_emit_log_hook = NULL;
static ClientAuthentication_hook_type prev_client_authentication = NULL;
static fmgr_hook_type prev_fmgr = NULL;

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

static int GetClientAddress(char* buf, int buf_size);

// Cache for session-stable values to avoid repeated catalog lookups on every query.
// Following pg_stat_monitor's pattern of caching client IP (pg_stat_monitor.c:73-96).
// Database name and client address never change within a session. Usernames
// are cached per userid (handles SET ROLE). This also carries the
// session-local registry of parse-time query text looked up later by
// ExecutorEnd or ProcessUtility.
typedef struct PschCachedUser {
  Oid userid;  // hash key
  uint8 name_len;
  char name[NAMEDATALEN];
} PschCachedUser;

typedef struct PschBackendState {
  bool initialized;
  char datname[NAMEDATALEN];
  uint8 datname_len;
  // Every role seen at a safe point, never evicted, since transaction end can
  // restore an earlier role with no hook running before next error. Grows
  // like Postgres's own role syscache
  HTAB* users;
  // Set on role rename or drop, names refreshed at next safe point
  bool users_stale;
  char client_addr[PSCH_MAX_CLIENT_ADDR_LEN];
  uint8 client_addr_len;
  PschNormalizedQueryCache normalize_cache;
} PschBackendState;
static PschBackendState backend_state = {0};

static void PschRoleInval(Datum arg pg_attribute_unused(),
                          PschSysCacheId cacheid pg_attribute_unused(),
                          uint32 hashvalue pg_attribute_unused()) {
  backend_state.users_stale = true;
}

// Failed resolve caches nothing so later calls retry
static void CacheUsername(Oid userid) {
  if (backend_state.users == NULL) {
    HASHCTL ctl = {.keysize = sizeof(Oid), .entrysize = sizeof(PschCachedUser)};
    backend_state.users = hash_create("pg_stat_ch usernames", 8, &ctl, HASH_ELEM | HASH_BLOBS);
    CacheRegisterSyscacheCallback(AUTHOID, PschRoleInval, (Datum)0);
  }
  if (backend_state.users_stale) {
    // Clear first so inval during lookup marks stale again
    backend_state.users_stale = false;
    HASH_SEQ_STATUS scan;
    hash_seq_init(&scan, backend_state.users);
    PschCachedUser* user;
    while ((user = hash_seq_search(&scan)) != NULL) {
      const char* username = GetUserNameFromId(user->userid, true);
      if (username != NULL) {
        user->name_len = PschCopyName(user->name, sizeof(user->name), username);
      }
    }
  }
  if (hash_search(backend_state.users, &userid, HASH_FIND, NULL) != NULL) {
    return;
  }
  const char* username = GetUserNameFromId(userid, true);
  if (username == NULL) {
    return;
  }
  PschCachedUser* user = hash_search(backend_state.users, &userid, HASH_ENTER, NULL);
  user->name_len = PschCopyName(user->name, sizeof(user->name), username);
}

// Populate backend cache and cache current user's name, which SET ROLE or
// SECURITY DEFINER may change
static void EnsureBackendCache(void) {
  // Can't resolve catalog names outside a transaction
  if (!IsTransactionState()) {
    return;
  }
  if (!backend_state.initialized) {
    // datname and client_addr are session-stable
    const char* datname = get_database_name(MyDatabaseId);
    backend_state.datname_len = PschCopyName(backend_state.datname, sizeof(backend_state.datname),
                                             datname != NULL ? datname : "<unknown>");
    backend_state.client_addr_len =
        (uint8)(GetClientAddress(backend_state.client_addr, sizeof(backend_state.client_addr)));
    backend_state.initialized = true;
  }
  CacheUsername(GetUserId());
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
    case CMD_MERGE:
      return PSCH_CMD_MERGE;
    case CMD_UTILITY:
      return PSCH_CMD_UTILITY;
    case CMD_NOTHING:
      return PSCH_CMD_NOTHING;
    default:
      return PSCH_CMD_UNKNOWN;
  }
}

static int64 TimeDiffMicrosec(struct timeval end, struct timeval start) {
  return ((int64)(end.tv_sec - start.tv_sec) * 1000000LL) + (int64)(end.tv_usec - start.tv_usec);
}

// Unpack SQLSTATE code from PostgreSQL's packed format to 5 unterminated chars
static void UnpackSqlState(int sql_state, char* buf) {
  for (int i = 0; i < 5; i++) {
    buf[i] = PGUNSIXBIT(sql_state);
    sql_state >>= 6;
  }
}

// The application_name GUC is the authoritative live value (the beentry's
// st_appname is populated from it, never the other way around).
static int GetApplicationName(char* buf) {
  return (int)(PschCopyTrimmed(buf, PSCH_MAX_APP_NAME_LEN, application_name));
}

static int GetClientAddress(char* buf, int buf_size) {
  // MyProcPort->raddr is what pgstat_bestart copies into the beentry's
  // st_clientaddr; it is immutable for the life of the session. NULL in
  // background/aux processes, which have no client connection.
  if (MyProcPort == NULL) {
    return 0;
  }

  char remote_host[NI_MAXHOST] = {0};

  int ret = pg_getnameinfo_all(&MyProcPort->raddr.addr, MyProcPort->raddr.salen, remote_host,
                               sizeof(remote_host), NULL, 0, NI_NUMERICHOST | NI_NUMERICSERV);

  if (ret != 0 || remote_host[0] == '\0') {
    return 0;
  }

  // Handle local connections
  if (strcmp(remote_host, "[local]") == 0) {
    return PschCopyName(buf, buf_size, "127.0.0.1");
  }

  return PschCopyName(buf, buf_size, remote_host);
}

// Check whether an event should be captured based on duration thresholds
// and sampling rate. Queries at or above min_duration_us are always captured;
// faster queries are randomly sampled at the configured rate.
static bool ShouldSampleEvent(uint64 duration_us) {
  if (psch_min_duration_us == 0 && psch_sample_rate >= 1.0) {
    return true;
  }
  if (duration_us >= (uint64)(psch_min_duration_us)) {
    return true;
  }
  if (psch_sample_rate <= 0.0) {
    return false;
  }
  if (psch_sample_rate >= 1.0) {
    return true;
  }
  return pg_prng_double(&pg_global_prng_state) < psch_sample_rate;
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
  event->temp_blk_read_time_us = INSTR_TIME_GET_MICROSEC(buf->temp_blk_read_time);
  event->temp_blk_write_time_us = INSTR_TIME_GET_MICROSEC(buf->temp_blk_write_time);
}

static void CopyWalUsage(PschEvent* event, const WalUsage* wal) {
  event->wal_records = wal->wal_records;
  event->wal_fpi = wal->wal_fpi;
  event->wal_bytes = wal->wal_bytes;
}

// Initialize PschEvent by zeroing only the fixed-size prefix instead of the full
// struct (~4.5KB).  After the field reorder in event.h, everything before
// err_message is fixed-size, so one memset covers the numeric fields, names,
// client context, and stored lengths. Text buffers are read by length alone.
static void InitEventPartial(PschEvent* event) {
  const size_t fixed_prefix_size = offsetof(PschEvent, err_message);
  memset(event, 0, fixed_prefix_size);
}

// Copy names and client context from backend cache. No catalog access, so
// safe inside emit_log_hook, which runs before the cache may be warm
static void CopySessionContext(PschEvent* event) {
  if (backend_state.initialized) {
    memcpy(event->datname, backend_state.datname, backend_state.datname_len);
    event->datname_len = backend_state.datname_len;
    memcpy(event->client_addr, backend_state.client_addr, backend_state.client_addr_len);
    event->client_addr_len = backend_state.client_addr_len;
  } else {
    event->datname_len = PschCopyName(event->datname, sizeof(event->datname), "<unknown>");
    event->client_addr_len =
        (uint8)(GetClientAddress(event->client_addr, sizeof(event->client_addr)));
  }

  // HASH_FIND never allocates
  const PschCachedUser* user =
      backend_state.users != NULL
          ? hash_search(backend_state.users, &event->userid, HASH_FIND, NULL)
          : NULL;
  if (user != NULL) {
    memcpy(event->username, user->name, user->name_len);
    event->username_len = user->name_len;
  } else {
    event->username_len = PschCopyName(event->username, sizeof(event->username), "<unknown>");
  }

  event->application_name_len = (uint8)(GetApplicationName(event->application_name));
}

static void InitBaseEvent(PschEvent* event, TimestampTz ts_start, bool top_level,
                          PschCmdType cmd_type) {
  InitEventPartial(event);
  event->ts_start = ts_start;
  event->dbid = MyDatabaseId;
  event->userid = GetUserId();
  event->pid = MyProcPid;
  event->top_level = top_level;
  event->cmd_type = cmd_type;
  CopySessionContext(event);
}

// Copy query text into the event buffer from the parse-time LRU cache.
//
// We only export text that was stashed during post_parse_analyze_hook:
// - constant-bearing statements export normalized text
// - constant-free statements export the unchanged statement slice
// - on a cache miss, query text is left empty instead of falling back to
//   raw SQL at execution time
static void CopyQueryText(PschEvent* event, uint64 query_id) {
  PschLookupNormalizedQuery(&backend_state.normalize_cache, query_id, event->query,
                            sizeof(event->query), &event->query_len);
}

// Copy JIT instrumentation to event
static void CopyJitInstrumentation(PschEvent* event, QueryDesc* query_desc) {
  if (query_desc->estate->es_jit != NULL) {
    JitInstrumentation* jit = &query_desc->estate->es_jit->instr;
    event->jit_functions = (int32)(jit->created_functions);
    event->jit_generation_time_us = (int32)(INSTR_TIME_GET_MICROSEC(jit->generation_counter));
    event->jit_inlining_time_us = (int32)(INSTR_TIME_GET_MICROSEC(jit->inlining_counter));
    event->jit_optimization_time_us = (int32)(INSTR_TIME_GET_MICROSEC(jit->optimization_counter));
    event->jit_emission_time_us = (int32)(INSTR_TIME_GET_MICROSEC(jit->emission_counter));
#if PG_VERSION_NUM >= 170000
    event->jit_deform_time_us = (int32)(INSTR_TIME_GET_MICROSEC(jit->deform_counter));
#endif
  }
}

// Copy parallel worker info to event (PG18+)
static void CopyParallelWorkerInfo(PschEvent* event pg_attribute_unused(),
                                   QueryDesc* query_desc pg_attribute_unused()) {
#if PG_VERSION_NUM >= 180000
  if (query_desc->estate != NULL) {
    event->parallel_workers_planned = (int16)(query_desc->estate->es_parallel_workers_to_launch);
    event->parallel_workers_launched = (int16)(query_desc->estate->es_parallel_workers_launched);
  }
#endif
}

static uint64 QueryDurationUs(QueryDesc* query_desc) {
  Instrumentation* instr = PschQueryInstr(query_desc);
  if (instr == NULL) {
    return (uint64)(GetCurrentTimestamp() - query_start_ts);
  }
#if PG_VERSION_NUM >= 190000
  return (uint64)(INSTR_TIME_GET_MICROSEC(instr->total));
#else
  return (uint64)(instr->total * 1000000.0);
#endif
}

static void BuildEventFromQueryDesc(QueryDesc* query_desc, PschEvent* event, int64 cpu_user_us,
                                    int64 cpu_sys_us) {
  InitBaseEvent(event, query_start_ts, current_query_is_top_level,
                ConvertCmdType(query_desc->operation));
  event->queryid = query_desc->plannedstmt->queryId;
  event->rows = query_desc->estate->es_processed;
  event->cpu_user_time_us = cpu_user_us;
  event->cpu_sys_time_us = cpu_sys_us;

  // Instrumentation data (duration, buffer, WAL)
  event->duration_us = QueryDurationUs(query_desc);
  Instrumentation* instr = PschQueryInstr(query_desc);
  if (instr != NULL) {
    CopyBufferUsage(event, &instr->bufusage);
    CopyIoTiming(event, &instr->bufusage);
    CopyWalUsage(event, &instr->walusage);
  }

  CopyJitInstrumentation(event, query_desc);
  CopyParallelWorkerInfo(event, query_desc);
  CopyQueryText(event, query_desc->plannedstmt->queryId);
}

// Work that may allocate or throw, so must stay out of emit_log_hook. Also
// clears capture guard left set by an error escaping capture
static void AtSafePoint(void) {
  disable_error_capture = false;
  PschDsaAttach();
  PschLocalBatchInit();
  EnsureBackendCache();
}

// Startup transaction commits after database, session user, and role settings
// from startup packet or pg_db_role_setting apply
static void PschStartupXactCallback(XactEvent event, void* arg pg_attribute_unused()) {
  if (event == XACT_EVENT_PRE_COMMIT) {
    EnsureBackendCache();
  }
  UnregisterXactCallback(PschStartupXactCallback, NULL);
}

// Gives errors from a session's first statement strings and names, which
// emit_log_hook can't set up itself. Runs in connection startup transaction
static void PschClientAuthentication(Port* port, int status) {
  if (prev_client_authentication != NULL) {
    prev_client_authentication(port, status);
  }
  // Ignore pg_stat_ch.enabled, which may be turned on by reload before first statement
  if (status != STATUS_OK) {
    return;
  }
  PschDsaAttach();
  PschLocalBatchInit();
  RegisterXactCallback(PschStartupXactCallback, NULL);
}

// Postgres calls this for SECURITY DEFINER functions after switching to owner,
// whose messages may come before any hook sees owner
static void PschFmgr(FmgrHookEventType event, FmgrInfo* flinfo, Datum* arg) {
  if (event == FHET_START) {
    EnsureBackendCache();
  }
  if (prev_fmgr != NULL) {
    prev_fmgr(event, flinfo, arg);
  }
}

// post_parse_analyze_hook — decide query text at parse time.
// The JumbleState (with constant locations) is only available here, so we
// must generate any normalized form now and stash the final exported text for
// ExecutorEnd.
static void PschPostParseAnalyze(ParseState* pstate, Query* query, PschJumbleState* jstate) {
  if (prev_post_parse_analyze != NULL) {
    prev_post_parse_analyze(pstate, query, jstate);
  }

  if (!psch_enabled || IsParallelWorker()) {
    return;
  }
  AtSafePoint();

  // Nothing to cache without a queryId (the executor/utility paths also skip
  // queryId == 0, so there is no consumer for this text).
  if (query->queryId == UINT64CONST(0)) {
    return;
  }

  const char* query_text = pstate->p_sourcetext;
  int query_loc = query->stmt_location;
  int query_len = query->stmt_len;

  // CleanQuerytext slices a multi-statement source string down to the current
  // statement before we either normalize literals or store the unchanged text.
  query_text = CleanQuerytext(query_text, &query_loc, &query_len);

  // PschRememberNormalizedQuery copies into the cache's own long-lived context,
  // so normalized text can be allocated in the short-lived query context
  if (jstate != NULL && jstate->clocations_count > 0) {
    char* normalized = PschNormalizeQuery(query_text, query_loc, &query_len, jstate);
    if (normalized != NULL) {
      PschRememberNormalizedQuery(&backend_state.normalize_cache, query->queryId, normalized,
                                  query_len);
      pfree(normalized);
    }
  } else if (query_text != NULL && query_len > 0) {
    PschTrimStatement(&query_text, &query_len);
    PschRememberNormalizedQuery(&backend_state.normalize_cache, query->queryId, query_text,
                                query_len);
  }
}

static void PschExecutorStart(QueryDesc* query_desc, int eflags) {
  if (IsParallelWorker()) {
    if (prev_executor_start != NULL) {
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
  if (psch_enabled) {
    AtSafePoint();
  }

  bool track = psch_enabled && query_desc->plannedstmt->queryId != UINT64CONST(0);
#if PG_VERSION_NUM >= 190000
  if (track) {
    query_desc->query_instr_options |= INSTRUMENT_ALL;
  }
#endif

  if (prev_executor_start != NULL) {
    prev_executor_start(query_desc, eflags);
  } else {
    standard_ExecutorStart(query_desc, eflags);
  }

#if PG_VERSION_NUM < 190000
  if (track && query_desc->totaltime == NULL) {
    MemoryContext oldcxt = MemoryContextSwitchTo(query_desc->estate->es_query_cxt);
    query_desc->totaltime = InstrAlloc(1, INSTRUMENT_ALL, false);
    MemoryContextSwitchTo(oldcxt);
  }
#endif
}

#if PG_VERSION_NUM >= 180000
static void PschExecutorRun(QueryDesc* query_desc, ScanDirection direction, uint64 count) {
#else
static void PschExecutorRun(QueryDesc* query_desc, ScanDirection direction, uint64 count,
                            bool execute_once) {
#endif
  if (IsParallelWorker()) {
#if PG_VERSION_NUM >= 180000
    if (prev_executor_run != NULL) {
      prev_executor_run(query_desc, direction, count);
    } else {
      standard_ExecutorRun(query_desc, direction, count);
    }
#else
    if (prev_executor_run != NULL) {
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
    if (prev_executor_run != NULL) {
      prev_executor_run(query_desc, direction, count);
    } else {
      standard_ExecutorRun(query_desc, direction, count);
    }
#else
    if (prev_executor_run != NULL) {
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
  if (IsParallelWorker()) {
    if (prev_executor_finish != NULL) {
      prev_executor_finish(query_desc);
    } else {
      standard_ExecutorFinish(query_desc);
    }
    return;
  }

  nesting_level++;
  PG_TRY();
  {
    if (prev_executor_finish != NULL) {
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
  if (!psch_enabled || IsParallelWorker() || query_desc->plannedstmt->queryId == UINT64CONST(0)) {
    if (prev_executor_end != NULL) {
      prev_executor_end(query_desc);
    } else {
      standard_ExecutorEnd(query_desc);
    }
    return;
  }

#if PG_VERSION_NUM < 190000
  if (query_desc->totaltime != NULL) {
    InstrEndLoop(query_desc->totaltime);
  }
#endif

  // Before sampling, so errors after an unsampled role change see new name
  EnsureBackendCache();

  // Compute duration early for sampling filter
  uint64 duration_us = QueryDurationUs(query_desc);

  if (!ShouldSampleEvent(duration_us)) {
    if (prev_executor_end != NULL) {
      prev_executor_end(query_desc);
    } else {
      standard_ExecutorEnd(query_desc);
    }
    return;
  }

  // Compute CPU time delta from getrusage
  int64 cpu_user_us = 0;
  int64 cpu_sys_us = 0;
  struct rusage rusage_end;
  if (getrusage(RUSAGE_SELF, &rusage_end) == 0) {
    cpu_user_us = TimeDiffMicrosec(rusage_end.ru_utime, rusage_start.ru_utime);
    cpu_sys_us = TimeDiffMicrosec(rusage_end.ru_stime, rusage_start.ru_stime);
  }

  PschEvent event;
  BuildEventFromQueryDesc(query_desc, &event, cpu_user_us, cpu_sys_us);
  PschEnqueueEvent(&event);

  if (prev_executor_end != NULL) {
    prev_executor_end(query_desc);
  } else {
    standard_ExecutorEnd(query_desc);
  }
}

// Build a PschEvent for utility statements (no QueryDesc available)
static void BuildEventForUtility(PschEvent* event, uint64 query_id, TimestampTz start_ts,
                                 uint64 duration_us, bool is_top_level, uint64 rows,
                                 BufferUsage* bufusage, WalUsage* walusage, int64 cpu_user_us,
                                 int64 cpu_sys_us) {
  InitBaseEvent(event, start_ts, is_top_level, PSCH_CMD_UTILITY);
  event->queryid = query_id;
  event->duration_us = duration_us;
  event->rows = rows;
  event->cpu_user_time_us = cpu_user_us;
  event->cpu_sys_time_us = cpu_sys_us;

  CopyBufferUsage(event, bufusage);
  CopyIoTiming(event, bufusage);
  CopyWalUsage(event, walusage);
  CopyQueryText(event, query_id);
}

// Helper macro to call ProcessUtility (previous hook or standard)
#define CALL_PROCESS_UTILITY()                                                                     \
  do {                                                                                             \
    if (prev_process_utility) {                                                                    \
      prev_process_utility(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, qc); \
    } else {                                                                                       \
      standard_ProcessUtility(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest,   \
                              qc);                                                                 \
    }                                                                                              \
  } while (0)

static bool ShouldTrackUtility(Node* parsetree) {
  if (!psch_enabled || IsParallelWorker()) {
    return false;
  }
  // Skip EXECUTE/PREPARE/DEALLOCATE to avoid double-counting
  if (IsA(parsetree, ExecuteStmt) || IsA(parsetree, PrepareStmt) ||
      IsA(parsetree, DeallocateStmt)) {
    return false;
  }
  // Skip transaction control statements (BEGIN, COMMIT, ROLLBACK, SAVEPOINT,
  // etc.).  They carry no meaningful telemetry — no query text, no buffer
  // stats, no duration — and at high TPS they consume a significant share of
  // queue capacity (2 out of 7 events per pgbench TPC-B transaction).
  if (IsA(parsetree, TransactionStmt)) {
    return false;
  }
  return true;
}

static uint64 GetUtilityRowCount(QueryCompletion* qc) {
  if (qc == NULL) {
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

static void ExecuteUtilityWithNesting(PlannedStmt* pstmt, const char* queryString,
                                      bool readOnlyTree, ProcessUtilityContext context,
                                      ParamListInfo params, QueryEnvironment* queryEnv,
                                      DestReceiver* dest, QueryCompletion* qc) {
  nesting_level++;
  PG_TRY();
  { CALL_PROCESS_UTILITY(); }
  PG_FINALLY();
  { nesting_level--; }
  PG_END_TRY();
}

// ProcessUtility hook - captures DDL and utility statements
static void PschProcessUtility(PlannedStmt* pstmt, const char* queryString, bool readOnlyTree,
                               ProcessUtilityContext context, ParamListInfo params,
                               QueryEnvironment* queryEnv, DestReceiver* dest,
                               QueryCompletion* qc) {
  if (!ShouldTrackUtility(pstmt->utilityStmt)) {
    CALL_PROCESS_UTILITY();
    return;
  }
  AtSafePoint();

  // Capture state before execution
  bool is_top_level = (nesting_level == 0);
  TimestampTz start_ts = GetCurrentTimestamp();
  uint64 query_id = pstmt->queryId;
  BufferUsage bufusage_start = pgBufferUsage;
  WalUsage walusage_start = pgWalUsage;
  struct rusage rusage_util_start;
  getrusage(RUSAGE_SELF, &rusage_util_start);
  instr_time start_time;
  INSTR_TIME_SET_CURRENT(start_time);

  ExecuteUtilityWithNesting(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, qc);

  instr_time duration;
  INSTR_TIME_SET_CURRENT(duration);
  INSTR_TIME_SUBTRACT(duration, start_time);

  uint64 duration_us = INSTR_TIME_GET_MICROSEC(duration);
  // Before sampling, so errors after an unsampled SET ROLE see new name
  EnsureBackendCache();
  if (!ShouldSampleEvent(duration_us)) {
    return;
  }

  BufferUsage bufusage_delta;
  WalUsage walusage_delta;
  MemSet(&bufusage_delta, 0, sizeof(BufferUsage));
  MemSet(&walusage_delta, 0, sizeof(WalUsage));
  BufferUsageAccumDiff(&bufusage_delta, &pgBufferUsage, &bufusage_start);
  WalUsageAccumDiff(&walusage_delta, &pgWalUsage, &walusage_start);

  int64 cpu_user_us = 0;
  int64 cpu_sys_us = 0;
  struct rusage rusage_util_end;
  if (getrusage(RUSAGE_SELF, &rusage_util_end) == 0) {
    cpu_user_us = TimeDiffMicrosec(rusage_util_end.ru_utime, rusage_util_start.ru_utime);
    cpu_sys_us = TimeDiffMicrosec(rusage_util_end.ru_stime, rusage_util_start.ru_stime);
  }

  PschEvent event;
  BuildEventForUtility(&event, query_id, start_ts, duration_us, is_top_level,
                       GetUtilityRowCount(qc), &bufusage_delta, &walusage_delta, cpu_user_us,
                       cpu_sys_us);
  PschEnqueueEvent(&event);
}

#undef CALL_PROCESS_UTILITY

// Check if log capture should occur for this error.
// Returns false during early initialization, in background workers, or when disabled.
static bool ShouldCaptureLog(ErrorData* edata) {
  // Basic preconditions
  if (edata == NULL || !system_init || !psch_enabled || disable_error_capture) {
    return false;
  }

  // Check error level threshold
  if (edata->elevel < psch_log_min_elevel) {
    return false;
  }

  // PostgreSQL bootstrapping checks - MyProc indicates PGPROC allocation complete
  if (MyProc == NULL || IsParallelWorker()) {
    return false;
  }

  // Session initialization checks (critical for debug5 safety):
  // - MyDatabaseId: database must be assigned
  // - IsUnderPostmaster: not single-user mode or bootstrap
  // - psch_shared_state: shared memory must be ready
  // - MyBgworkerEntry: skip background workers (not user queries)
  if (MyDatabaseId == InvalidOid || !IsUnderPostmaster || psch_shared_state == NULL ||
      MyBgworkerEntry != NULL) {
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
  InitBaseEvent(&event, GetCurrentTimestamp(), (nesting_level == 0), PSCH_CMD_UNKNOWN);

  UnpackSqlState(edata->sqlerrcode, event.err_sqlstate);
  event.err_elevel = (uint8)(edata->elevel);

  if (edata->message != NULL) {
    event.err_message_len =
        (uint16)(PschCopyTrimmed(event.err_message, PSCH_MAX_ERR_MSG_LEN, edata->message));
  }

  PschEnqueueEvent(&event);
}

// emit_log_hook - captures log messages at configured level and above
//
// CHAINING ORDER: We chain to the previous hook FIRST to allow other extensions
// (e.g., log formatters, filters) to transform ErrorData before we capture it.
// This ensures we capture the final, potentially modified log message.
static void PschEmitLogHook(ErrorData* edata) {
  // Chain to previous hook first (allows log transformation by other extensions)
  if (prev_emit_log_hook != NULL) {
    prev_emit_log_hook(edata);
  }

  if (!ShouldCaptureLog(edata)) {
    return;
  }

  // Capture must not throw: a nested ERROR here resets ErrorContext, freeing
  // the report being emitted, so control can never return to errfinish.
  // Holding interrupts keeps a pending cancel from firing inside capture.
  // Should something throw anyway, let it propagate as Postgres does for an
  // error during error processing. Guard stays set so nested report skips
  // capture instead of recursing, AtSafePoint clears it
  disable_error_capture = true;
  HOLD_INTERRUPTS();
  CaptureLogEvent(edata);
  RESUME_INTERRUPTS();
  disable_error_capture = false;
}

// Set or clear the error-capture guard, returning the previous value so
// callers nest correctly (e.g. PschEnqueueEvent suppressing capture while
// already inside the emit_log_hook capture, which holds the guard).
bool PschSuppressErrorCapture(bool suppress) {
  bool prev = disable_error_capture;
  disable_error_capture = suppress;
  return prev;
}

void PschInstallHooks(void) {
  EnableQueryId();

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

  prev_client_authentication = ClientAuthentication_hook;
  ClientAuthentication_hook = PschClientAuthentication;

  prev_fmgr = fmgr_hook;
  fmgr_hook = PschFmgr;

  // Mark system as initialized - emit_log_hook will now capture messages
  system_init = true;
}
