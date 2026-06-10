// pg_stat_ch ClickHouse exporter — C port of the former C++ implementation.
//
// Contract (src/export/exporter.h): no function here may ereport(ERROR) or
// otherwise longjmp; WARNING/LOG/DEBUG1 only.  All column buffers are
// preallocated in PschClickHouseExporterCreate, sized for the driver's
// staging chunk, so the steady-state export path performs zero heap
// allocation outside the two reset-per-batch memory contexts that
// clickhouse-c draws from (via NO_OOM allocators that return NULL).

#include "postgres.h"

#include "miscadmin.h"           // ProcDiePending
#include "storage/procsignal.h"  // ProcSignalBarrierPending
#include "utils/memutils.h"
#include "utils/palloc.h"

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

#include "clickhouse-client.h"
#include "clickhouse-compression.h"
#include "clickhouse-openssl.h"
#include "clickhouse-posix-io.h"
#include "clickhouse.h"

#include "config/guc.h"
#include "config/memory_budget.h"
#include "export/exporter.h"
#include "queue/shmem.h"  // PschRecordExportFailure / PschRecordExportSuccess

static const int kSocketTimeoutSec = 30;

// Bound packets consumed while awaiting response
static const int kMaxRecvPackets = 4096;

// PostgreSQL epoch is 2000-01-01, Unix epoch is 1970-01-01.
static const int64 kPostgresEpochOffsetUs = INT64CONST(946684800000000);

// ---------------------------------------------------------------------------
// Static schema.  Exactly the columns the old driver registered, in the same
// order (the old stats_exporter.cc:236-286 registration sequence).  This
// table is the single source of truth for names, ClickHouse types, buffer
// sizing, and the INSERT column list.
// ---------------------------------------------------------------------------

typedef enum ChTypeId {
  CH_TYPE_INT16 = 0,
  CH_TYPE_INT32,
  CH_TYPE_INT64,
  CH_TYPE_UINT8,
  CH_TYPE_UINT64,
  CH_TYPE_DATETIME64_6,
  CH_TYPE_FIXEDSTRING_5,
  CH_NUM_TYPES,
  CH_TYPE_NONE = -1,  // String columns carry no parsed type on the wire
} ChTypeId;

static const char* const kChTypeNames[CH_NUM_TYPES] = {
    "Int16", "Int32", "Int64", "UInt8", "UInt64", "DateTime64(6)", "FixedString(5)",
};

typedef enum ChColId {
  CH_COL_TS_START = 0,
  CH_COL_DURATION_US,
  CH_COL_DB,
  CH_COL_USERNAME,
  CH_COL_PID,
  CH_COL_QUERY_ID,
  CH_COL_CMD_TYPE,
  CH_COL_ROWS,
  CH_COL_QUERY,
  CH_COL_SHARED_BLKS_HIT,
  CH_COL_SHARED_BLKS_READ,
  CH_COL_SHARED_BLKS_DIRTIED,
  CH_COL_SHARED_BLKS_WRITTEN,
  CH_COL_LOCAL_BLKS_HIT,
  CH_COL_LOCAL_BLKS_READ,
  CH_COL_LOCAL_BLKS_DIRTIED,
  CH_COL_LOCAL_BLKS_WRITTEN,
  CH_COL_TEMP_BLKS_READ,
  CH_COL_TEMP_BLKS_WRITTEN,
  CH_COL_SHARED_BLK_READ_TIME_US,
  CH_COL_SHARED_BLK_WRITE_TIME_US,
  CH_COL_LOCAL_BLK_READ_TIME_US,
  CH_COL_LOCAL_BLK_WRITE_TIME_US,
  CH_COL_TEMP_BLK_READ_TIME_US,
  CH_COL_TEMP_BLK_WRITE_TIME_US,
  CH_COL_WAL_RECORDS,
  CH_COL_WAL_FPI,
  CH_COL_WAL_BYTES,
  CH_COL_CPU_USER_TIME_US,
  CH_COL_CPU_SYS_TIME_US,
  CH_COL_JIT_FUNCTIONS,
  CH_COL_JIT_GENERATION_TIME_US,
  CH_COL_JIT_DEFORM_TIME_US,
  CH_COL_JIT_INLINING_TIME_US,
  CH_COL_JIT_OPTIMIZATION_TIME_US,
  CH_COL_JIT_EMISSION_TIME_US,
  CH_COL_PARALLEL_WORKERS_PLANNED,
  CH_COL_PARALLEL_WORKERS_LAUNCHED,
  CH_COL_ERR_SQLSTATE,
  CH_COL_ERR_ELEVEL,
  CH_COL_ERR_MESSAGE,
  CH_COL_APP,
  CH_COL_CLIENT_ADDR,
  CH_NUM_COLS,
} ChColId;

typedef enum ChColKind {
  CH_KIND_FIXED = 0,    // n_rows * elem_size slab via append_fixed
  CH_KIND_STRING,       // bytes slab + cumulative uint64 end-offsets via append_string
  CH_KIND_FIXEDSTRING,  // n_rows * elem_size slab via append_fixed (FixedString type)
} ChColKind;

typedef struct ChColDesc {
  const char* name;
  ChColKind kind;
  ChTypeId type_id;  // CH_TYPE_NONE for CH_KIND_STRING
  uint16 elem_size;  // FIXED: element bytes; FIXEDSTRING: width
  uint16 max_len;    // STRING: clamp limit == per-row buffer reservation
} ChColDesc;

#define CH_FIXED(nm, tid, sz) {(nm), CH_KIND_FIXED, (tid), (sz), 0}
#define CH_STRING(nm, maxlen) {(nm), CH_KIND_STRING, CH_TYPE_NONE, 0, (maxlen)}

// cmd_type values come from PschCmdTypeToString: longest is "UNKNOWN" (7).
#define CH_CMD_TYPE_MAX_LEN 8

static const ChColDesc kChCols[CH_NUM_COLS] = {
    [CH_COL_TS_START] = CH_FIXED("ts_start", CH_TYPE_DATETIME64_6, 8),
    [CH_COL_DURATION_US] = CH_FIXED("duration_us", CH_TYPE_UINT64, 8),
    [CH_COL_DB] = CH_STRING("db", NAMEDATALEN),
    [CH_COL_USERNAME] = CH_STRING("username", NAMEDATALEN),
    [CH_COL_PID] = CH_FIXED("pid", CH_TYPE_INT32, 4),
    [CH_COL_QUERY_ID] = CH_FIXED("query_id", CH_TYPE_INT64, 8),
    [CH_COL_CMD_TYPE] = CH_STRING("cmd_type", CH_CMD_TYPE_MAX_LEN),
    [CH_COL_ROWS] = CH_FIXED("rows", CH_TYPE_UINT64, 8),
    [CH_COL_QUERY] = CH_STRING("query", PSCH_MAX_QUERY_LEN),
    [CH_COL_SHARED_BLKS_HIT] = CH_FIXED("shared_blks_hit", CH_TYPE_INT64, 8),
    [CH_COL_SHARED_BLKS_READ] = CH_FIXED("shared_blks_read", CH_TYPE_INT64, 8),
    [CH_COL_SHARED_BLKS_DIRTIED] = CH_FIXED("shared_blks_dirtied", CH_TYPE_INT64, 8),
    [CH_COL_SHARED_BLKS_WRITTEN] = CH_FIXED("shared_blks_written", CH_TYPE_INT64, 8),
    [CH_COL_LOCAL_BLKS_HIT] = CH_FIXED("local_blks_hit", CH_TYPE_INT64, 8),
    [CH_COL_LOCAL_BLKS_READ] = CH_FIXED("local_blks_read", CH_TYPE_INT64, 8),
    [CH_COL_LOCAL_BLKS_DIRTIED] = CH_FIXED("local_blks_dirtied", CH_TYPE_INT64, 8),
    [CH_COL_LOCAL_BLKS_WRITTEN] = CH_FIXED("local_blks_written", CH_TYPE_INT64, 8),
    [CH_COL_TEMP_BLKS_READ] = CH_FIXED("temp_blks_read", CH_TYPE_INT64, 8),
    [CH_COL_TEMP_BLKS_WRITTEN] = CH_FIXED("temp_blks_written", CH_TYPE_INT64, 8),
    [CH_COL_SHARED_BLK_READ_TIME_US] = CH_FIXED("shared_blk_read_time_us", CH_TYPE_INT64, 8),
    [CH_COL_SHARED_BLK_WRITE_TIME_US] = CH_FIXED("shared_blk_write_time_us", CH_TYPE_INT64, 8),
    [CH_COL_LOCAL_BLK_READ_TIME_US] = CH_FIXED("local_blk_read_time_us", CH_TYPE_INT64, 8),
    [CH_COL_LOCAL_BLK_WRITE_TIME_US] = CH_FIXED("local_blk_write_time_us", CH_TYPE_INT64, 8),
    [CH_COL_TEMP_BLK_READ_TIME_US] = CH_FIXED("temp_blk_read_time_us", CH_TYPE_INT64, 8),
    [CH_COL_TEMP_BLK_WRITE_TIME_US] = CH_FIXED("temp_blk_write_time_us", CH_TYPE_INT64, 8),
    [CH_COL_WAL_RECORDS] = CH_FIXED("wal_records", CH_TYPE_INT64, 8),
    [CH_COL_WAL_FPI] = CH_FIXED("wal_fpi", CH_TYPE_INT64, 8),
    [CH_COL_WAL_BYTES] = CH_FIXED("wal_bytes", CH_TYPE_UINT64, 8),
    [CH_COL_CPU_USER_TIME_US] = CH_FIXED("cpu_user_time_us", CH_TYPE_INT64, 8),
    [CH_COL_CPU_SYS_TIME_US] = CH_FIXED("cpu_sys_time_us", CH_TYPE_INT64, 8),
    [CH_COL_JIT_FUNCTIONS] = CH_FIXED("jit_functions", CH_TYPE_INT32, 4),
    [CH_COL_JIT_GENERATION_TIME_US] = CH_FIXED("jit_generation_time_us", CH_TYPE_INT32, 4),
    [CH_COL_JIT_DEFORM_TIME_US] = CH_FIXED("jit_deform_time_us", CH_TYPE_INT32, 4),
    [CH_COL_JIT_INLINING_TIME_US] = CH_FIXED("jit_inlining_time_us", CH_TYPE_INT32, 4),
    [CH_COL_JIT_OPTIMIZATION_TIME_US] = CH_FIXED("jit_optimization_time_us", CH_TYPE_INT32, 4),
    [CH_COL_JIT_EMISSION_TIME_US] = CH_FIXED("jit_emission_time_us", CH_TYPE_INT32, 4),
    [CH_COL_PARALLEL_WORKERS_PLANNED] = CH_FIXED("parallel_workers_planned", CH_TYPE_INT16, 2),
    [CH_COL_PARALLEL_WORKERS_LAUNCHED] = CH_FIXED("parallel_workers_launched", CH_TYPE_INT16, 2),
    [CH_COL_ERR_SQLSTATE] = {"err_sqlstate", CH_KIND_FIXEDSTRING, CH_TYPE_FIXEDSTRING_5, 5, 0},
    [CH_COL_ERR_ELEVEL] = CH_FIXED("err_elevel", CH_TYPE_UINT8, 1),
    [CH_COL_ERR_MESSAGE] = CH_STRING("err_message", PSCH_MAX_ERR_MSG_LEN),
    [CH_COL_APP] = CH_STRING("app", PSCH_MAX_APP_NAME_LEN),
    [CH_COL_CLIENT_ADDR] = CH_STRING("client_addr", PSCH_MAX_CLIENT_ADDR_LEN),
};

// Preallocated per-column storage.  Fixed columns: chunk_rows * elem_size.
// String columns keep the cumulative uint64 end-offset layout that
// chc_block_builder_append_string consumes directly.
typedef struct ChColData {
  char* data;
  uint64_t* offsets;  // CH_KIND_STRING only; uint64_t (not PG uint64) — consumed by clickhouse-c
  size_t bytes_used;  // CH_KIND_STRING fill cursor, reset per chunk
} ChColData;

typedef struct ClickHouseExporter {
  PschExporter base;

  MemoryContext conn_cxt;   // connection-lifetime clickhouse-c allocations
  MemoryContext batch_cxt;  // reset after every commit attempt
  MemoryContext col_cxt;    // create-time column buffers, never reset
  chc_alloc conn_al;
  chc_alloc batch_al;

  chc_client* client;
  int fd;
  SSL_CTX* ssl_ctx;
  SSL* ssl;
  chc_posix_io posix_io;
  chc_openssl_io openssl_io;
  chc_io io;
  chc_codec codec;

  chc_block_builder* bb;  // live only inside ChCommitChunk
  chc_err build_err;
  chc_type* types[CH_NUM_TYPES];  // parsed eagerly at connect, conn_cxt-owned

  int chunk_rows;  // rows per INSERT == driver staging chunk capacity
  ChColData cols[CH_NUM_COLS];
  char* insert_query;
  size_t insert_query_len;

  // True while a wire exchange is outstanding.  Still set on entry means a
  // longjmp cut the previous INSERT mid-protocol: the server is wedged
  // waiting for data, so the connection must be dropped, not reused.
  bool in_flight;

  int consecutive_failures;
  uint64 mem_used;  // preallocated bytes, for pg_stat_ch_memory()
} ClickHouseExporter;

// ---------------------------------------------------------------------------
// Forward declarations
// ---------------------------------------------------------------------------

static bool ChConnect(PschExporter* self, char* errbuf, size_t errlen);
static bool ChIsConnected(const PschExporter* self);
static PschExportStatus ChExportEvents(PschExporter* self, const PschEvent* events, int nevents,
                                       int* exported_out);
static int ChConsecutiveFailures(const PschExporter* self);
static void ChResetFailures(PschExporter* self);
static uint64 ChMemUsed(const PschExporter* self);
static void ChDestroy(PschExporter* self);

static bool EstablishNewConnection(ClickHouseExporter* exp);
static void CloseConnection(ClickHouseExporter* exp);
static void ResetBatchContext(ClickHouseExporter* exp);

static const PschExporterOps kClickHouseOps = {
    .connect = ChConnect,
    .is_connected = ChIsConnected,
    .export_events = ChExportEvents,
    .send_arrow = NULL,  // ClickHouse backend does not speak Arrow IPC
    .consecutive_failures = ChConsecutiveFailures,
    .reset_failures = ChResetFailures,
    .mem_used = ChMemUsed,
    .destroy = ChDestroy,
};

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

// Abort blocking reads when the bgworker receives SIGTERM (ProcDiePending) or
// a procsignal barrier is pending (ProcSignalBarrierPending — e.g. DROP
// DATABASE/TABLESPACE).  clickhouse-c checks this before each socket refill
// (clickhouse.h:708,758), so returning true here surfaces as CHC_ERR_CANCELLED
// -> ERR_SEND -> the batch is requeued and the bgworker loop promptly runs
// ProcessProcSignalBarrier.  This bounds barrier-ack latency to one read while
// data is flowing; a hard-stalled connection is still bounded by the socket
// deadline (kSocketTimeoutSec), since clickhouse-c does not poll the cancel
// callback while blocked inside poll().
static bool PschChcCheckCancel(void* ud) {
  (void)ud;
  return ProcDiePending != 0 || ProcSignalBarrierPending;
}

// palloc ereport(ERROR)s on requests beyond MaxAllocHugeSize even with
// NO_OOM; refuse them here so the whole clickhouse-c call tree stays
// longjmp-free (OOM and oversize both surface as NULL -> CHC_ERR_OOM).
static void* PschChcAlloc(void* ud, size_t bytes) {
  if (bytes > MaxAllocHugeSize)
    return NULL;
  return MemoryContextAllocExtended((MemoryContext)ud, bytes, MCXT_ALLOC_HUGE | MCXT_ALLOC_NO_OOM);
}

static void* PschChcRealloc(void* ud, void* p, size_t old_bytes, size_t new_bytes) {
  (void)old_bytes;
  if (p == NULL)
    return PschChcAlloc(ud, new_bytes);
  if (new_bytes > MaxAllocHugeSize)
    return NULL;
  return repalloc_extended(p, new_bytes, MCXT_ALLOC_HUGE | MCXT_ALLOC_NO_OOM);
}

static void PschChcFree(void* ud, void* p, size_t bytes) {
  (void)ud;
  (void)bytes;
  if (p != NULL)
    pfree(p);
}

static chc_alloc MakePschChcAlloc(MemoryContext cxt) {
  return (chc_alloc){cxt, PschChcAlloc, PschChcRealloc, PschChcFree};
}

static int64 MonotonicNowUs(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ((int64)ts.tv_sec * 1000000) + (ts.tv_nsec / 1000);
}

// msg need not be NUL-terminated (server-controlled exception text); always
// truncates and NUL-terminates within errlen.
static void ErrBufSetN(char* errbuf, size_t errlen, const char* msg, size_t msg_len) {
  size_t n;

  if (errbuf == NULL || errlen == 0)
    return;
  n = Min(msg_len, errlen - 1);
  if (n > 0)
    memcpy(errbuf, msg, n);
  errbuf[n] = '\0';
}

static void ErrBufSet(char* errbuf, size_t errlen, const char* msg) {
  ErrBufSetN(errbuf, errlen, msg, strlen(msg));
}

// Clamp a field length to its buffer maximum, warning on overflow.  Must be
// applied BEFORE any memcpy into the preallocated column buffers.
static inline uint32 ClampFieldLen(uint32 len, uint32 max, const char* field_name) {
  if (len <= max)
    return len;
  // Rate-limit to at most one WARNING/sec: a corrupt or oversized field could
  // otherwise flood the log at the event rate.
  static time_t last_log = 0;
  time_t now = time(NULL);
  if (now - last_log >= 1) {
    elog(WARNING, "pg_stat_ch: invalid %s %u, clamping", field_name, (unsigned)len);
    last_log = now;
  }
  return max;
}

// ---------------------------------------------------------------------------
// Connection plumbing (near-verbatim port of the C++ implementation)
// ---------------------------------------------------------------------------

// Restore blocking mode after bounded connect, clickhouse-c I/O expects it
static bool ConnectWithTimeout(int fd, const struct sockaddr* sa, socklen_t slen, int timeout_sec) {
  int flags = fcntl(fd, F_GETFL, 0);
  bool ok = false;
  int save_errno = 0;

  if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)
    return false;

  if (connect(fd, sa, slen) == 0) {
    ok = true;
  } else if (errno == EINPROGRESS) {
    const int64 deadline = MonotonicNowUs() + (int64)timeout_sec * 1000000;
    for (;;) {
      const int64 remaining_us = deadline - MonotonicNowUs();
      struct pollfd pfd;
      int pr;
      int soerr = 0;
      socklen_t l = sizeof soerr;

      if (remaining_us <= 0) {
        save_errno = ETIMEDOUT;
        break;
      }
      pfd.fd = fd;
      pfd.events = POLLOUT;
      pfd.revents = 0;
      pr = poll(&pfd, 1, (int)((remaining_us + 999) / 1000));
      if (pr < 0) {
        if (errno == EINTR)
          continue;  // signal (eg bgworker latch); retry within deadline
        save_errno = errno;
        break;
      }
      if (pr == 0) {
        save_errno = ETIMEDOUT;
        break;
      }
      if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &soerr, &l) < 0) {
        save_errno = errno;
        break;
      }
      save_errno = soerr;
      ok = soerr == 0;
      break;
    }
  } else {
    save_errno = errno;
  }

  // Always restore blocking mode, even on failure: keeps the helper safe to
  // reuse and matches clickhouse-c I/O expectations
  if (fcntl(fd, F_SETFL, flags) < 0)
    return false;
  if (!ok)
    errno = save_errno;
  return ok;
}

static bool MemoryContextsReady(const ClickHouseExporter* exp) {
  return exp->conn_cxt != NULL && exp->batch_cxt != NULL;
}

// Sole longjmp source in the connection path: AllocSetContextCreate ereports on
// OOM. Swallow it here so EstablishNewConnection stays a pure bool — callers
// then need no PG_TRY frame around it. elog.c leaves CurrentMemoryContext at
// ErrorContext after a caught error, so restore it before returning.
static bool EnsureMemoryContexts(ClickHouseExporter* exp) {
  MemoryContext oldcontext;

  if (MemoryContextsReady(exp) && exp->col_cxt != NULL)
    return true;

  oldcontext = CurrentMemoryContext;
  PG_TRY();
  {
    if (exp->conn_cxt == NULL)
      exp->conn_cxt = AllocSetContextCreate(TopMemoryContext, "pg_stat_ch clickhouse-c",
                                            ALLOCSET_DEFAULT_SIZES);
    if (exp->batch_cxt == NULL)
      exp->batch_cxt = AllocSetContextCreate(TopMemoryContext, "pg_stat_ch clickhouse-c batch",
                                             ALLOCSET_DEFAULT_SIZES);
    if (exp->col_cxt == NULL)
      exp->col_cxt = AllocSetContextCreate(TopMemoryContext, "pg_stat_ch clickhouse-c columns",
                                           ALLOCSET_DEFAULT_SIZES);
  }
  PG_CATCH();
  {
    MemoryContextSwitchTo(oldcontext);
    if (exp->col_cxt != NULL) {
      MemoryContextDelete(exp->col_cxt);
      exp->col_cxt = NULL;
    }
    if (exp->batch_cxt != NULL) {
      MemoryContextDelete(exp->batch_cxt);
      exp->batch_cxt = NULL;
    }
    if (exp->conn_cxt != NULL) {
      MemoryContextDelete(exp->conn_cxt);
      exp->conn_cxt = NULL;
    }
    exp->conn_al = (chc_alloc){0};
    exp->batch_al = (chc_alloc){0};
    EmitErrorReport();
    FlushErrorState();
    return false;
  }
  PG_END_TRY();

  exp->conn_al = MakePschChcAlloc(exp->conn_cxt);
  exp->batch_al = MakePschChcAlloc(exp->batch_cxt);
  return true;
}

static void ClearTypes(ClickHouseExporter* exp) {
  for (int t = 0; t < CH_NUM_TYPES; t++) {
    if (exp->types[t] != NULL) {
      chc_type_destroy(exp->types[t], &exp->conn_al);
      exp->types[t] = NULL;
    }
  }
}

static void ResetBatchContext(ClickHouseExporter* exp) {
  if (exp->batch_cxt != NULL) {
    MemoryContextReset(exp->batch_cxt);
    exp->batch_al = MakePschChcAlloc(exp->batch_cxt);
  }
}

static void ResetConnectionContext(ClickHouseExporter* exp) {
  ClearTypes(exp);
  if (exp->conn_cxt != NULL) {
    MemoryContextReset(exp->conn_cxt);
    exp->conn_al = MakePschChcAlloc(exp->conn_cxt);
  }
}

// WARNING + consecutive-failure increment (+ optional close).  Shmem failure
// stats are recorded by the driver (stats_exporter.c) from the returned
// status, so the backend must NOT call PschRecordExportFailure here (it would
// double-count send_failures); the detailed message lives in this WARNING.
static void RecordFailure(ClickHouseExporter* exp, const char* context, const char* message,
                          bool close_conn) {
  const char* m = (message != NULL && message[0] != '\0') ? message : "unknown failure";

  elog(WARNING, "pg_stat_ch: %s: %s", context, m);
  exp->consecutive_failures++;
  if (close_conn)
    CloseConnection(exp);
}

static void SetReadDeadline(ClickHouseExporter* exp) {
  const int64 dl = MonotonicNowUs() + (int64)kSocketTimeoutSec * 1000000;

  if (psch_clickhouse_use_tls)
    chc_openssl_io_set_deadline(&exp->openssl_io, dl);
  else
    chc_posix_io_set_deadline(&exp->posix_io, dl);
}

static bool TcpConnect(ClickHouseExporter* exp, const char* host, int port) {
  struct addrinfo hints;
  struct addrinfo* res = NULL;
  char port_s[16];
  int fd = -1;
  int save_errno = ECONNREFUSED;
  int rc;
  const int one = 1;
  struct timeval tv;

  memset(&hints, 0, sizeof hints);
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  snprintf(port_s, sizeof port_s, "%d", port);

  rc = getaddrinfo(host, port_s, &hints, &res);
  if (rc != 0) {
    elog(WARNING, "pg_stat_ch: getaddrinfo(%s:%d): %s", host, port, gai_strerror(rc));
    return false;
  }

  for (struct addrinfo* ai = res; ai != NULL; ai = ai->ai_next) {
    fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (fd < 0) {
      save_errno = errno;
      continue;
    }
    if (ConnectWithTimeout(fd, ai->ai_addr, ai->ai_addrlen, kSocketTimeoutSec))
      break;
    save_errno = errno;
    close(fd);
    fd = -1;
  }
  freeaddrinfo(res);
  if (fd < 0) {
    elog(WARNING, "pg_stat_ch: connect(%s:%d): %s", host, port, strerror(save_errno));
    return false;
  }

  (void)setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
  (void)setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof one);
  tv.tv_sec = kSocketTimeoutSec;
  tv.tv_usec = 0;
  (void)setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
  (void)setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);
  exp->fd = fd;
  return true;
}

static bool TlsConnect(ClickHouseExporter* exp, const char* host) {
  // OpenSSL 1.1.0+ auto-initializes on first use; no explicit library init.
  exp->ssl_ctx = SSL_CTX_new(TLS_client_method());
  if (exp->ssl_ctx == NULL) {
    elog(WARNING, "pg_stat_ch: SSL_CTX_new failed");
    return false;
  }
  if (!psch_clickhouse_skip_tls_verify) {
    SSL_CTX_set_verify(exp->ssl_ctx, SSL_VERIFY_PEER, NULL);
    if (SSL_CTX_set_default_verify_paths(exp->ssl_ctx) != 1) {
      elog(WARNING, "pg_stat_ch: could not load default CA certificates");
      return false;
    }
  }

  exp->ssl = SSL_new(exp->ssl_ctx);
  if (exp->ssl == NULL) {
    elog(WARNING, "pg_stat_ch: SSL_new failed");
    return false;
  }
  if (SSL_set_tlsext_host_name(exp->ssl, host) != 1) {  // SNI
    elog(WARNING, "pg_stat_ch: could not set TLS SNI host name");
    return false;
  }
  if (!psch_clickhouse_skip_tls_verify) {
    SSL_set_hostflags(exp->ssl, X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS);
    if (SSL_set1_host(exp->ssl, host) != 1) {
      elog(WARNING, "pg_stat_ch: could not set TLS verification host");
      return false;
    }
  }
  if (SSL_set_fd(exp->ssl, exp->fd) != 1) {
    elog(WARNING, "pg_stat_ch: SSL_set_fd failed");
    return false;
  }
  ERR_clear_error();  // queue is per-thread, drop any residue from a prior retry
  if (SSL_connect(exp->ssl) != 1) {
    // cert-verify failures leave the error queue empty, so check vr first
    const long vr = SSL_get_verify_result(exp->ssl);
    char ebuf[256];

    if (vr != X509_V_OK) {
      snprintf(ebuf, sizeof ebuf, "certificate verify failed: %s",
               X509_verify_cert_error_string(vr));
    } else {
      const unsigned long e = ERR_get_error();
      if (e != 0)
        ERR_error_string_n(e, ebuf, sizeof ebuf);  // cipher / protocol mismatch etc
      else
        snprintf(ebuf, sizeof ebuf, "SSL_connect failed");
    }
    elog(WARNING, "pg_stat_ch: TLS handshake failed: %s", ebuf);
    return false;
  }
  return true;
}

static void CloseConnection(ClickHouseExporter* exp) {
  if (exp->client != NULL) {
    chc_client_close(exp->client);
    exp->client = NULL;
  }
  if (exp->ssl != NULL) {
    SSL_shutdown(exp->ssl);
    SSL_free(exp->ssl);
    exp->ssl = NULL;
  }
  if (exp->ssl_ctx != NULL) {
    SSL_CTX_free(exp->ssl_ctx);
    exp->ssl_ctx = NULL;
  }
  if (exp->fd >= 0) {
    close(exp->fd);
    exp->fd = -1;
  }
  ResetConnectionContext(exp);
}

static bool EstablishNewConnection(ClickHouseExporter* exp) {
  const char* host;
  int port;
  chc_client_opts opts;
  chc_err err;

  if (!EnsureMemoryContexts(exp))
    return false;
  CloseConnection(exp);

  host = psch_clickhouse_host != NULL ? psch_clickhouse_host : "localhost";
  port = psch_clickhouse_port;

  if (!TcpConnect(exp, host, port))
    return false;

  if (psch_clickhouse_use_tls) {
    if (!TlsConnect(exp, host)) {
      CloseConnection(exp);
      return false;
    }
    chc_openssl_io_init(&exp->openssl_io, &exp->io, exp->ssl, PschChcCheckCancel, NULL);
  } else {
    chc_posix_io_init(&exp->posix_io, &exp->io, exp->fd, PschChcCheckCancel, NULL);
  }

  chc_lz4_codec_init(&exp->codec);

  // GUC string pointers are consumed synchronously inside chc_client_init;
  // never stash them (SIGHUP reload frees the old values).
  memset(&opts, 0, sizeof opts);
  opts.client_name = "pg_stat_ch";
  opts.database = psch_clickhouse_database != NULL ? psch_clickhouse_database : "pg_stat_ch";
  opts.user = psch_clickhouse_user != NULL ? psch_clickhouse_user : "default";
  opts.password = psch_clickhouse_password != NULL ? psch_clickhouse_password : "";
  opts.compression = CHC_COMP_LZ4;
  opts.codec = &exp->codec;

  SetReadDeadline(exp);  // bound the Hello / Ping handshake reads
  memset(&err, 0, sizeof err);
  if (chc_client_init(&exp->client, &opts, &exp->conn_al, &exp->io, &err) != CHC_OK) {
    elog(WARNING, "pg_stat_ch: failed to connect to ClickHouse: %s",
         err.msg[0] ? err.msg : "init failed");
    if (exp->client != NULL) {
      chc_client_close(exp->client);
      exp->client = NULL;
    }
    CloseConnection(exp);
    return false;
  }

  // Parse all column types eagerly (replaces the old lazy ResolveType map):
  // the append path must never allocate type ASTs mid-batch.
  for (int t = 0; t < CH_NUM_TYPES; t++) {
    chc_err terr = {0};
    if (chc_type_parse(kChTypeNames[t], strlen(kChTypeNames[t]), &exp->conn_al, &exp->types[t],
                       &terr) != CHC_OK) {
      elog(WARNING, "pg_stat_ch: failed to parse ClickHouse type %s: %s", kChTypeNames[t],
           terr.msg[0] ? terr.msg : "parse failed");
      CloseConnection(exp);
      return false;
    }
  }

  elog(LOG, "pg_stat_ch: connected to ClickHouse at %s:%d%s", host, port,
       psch_clickhouse_use_tls ? " (TLS)" : "");
  return true;
}

// ---------------------------------------------------------------------------
// INSERT protocol loop
// ---------------------------------------------------------------------------

// chc_packet_clear runs on EVERY exit path (replaces the old PacketGuard):
// a missed clear leaks server packets — exception payloads included — into
// conn_cxt until the next reconnect.
static bool RecvUntil(ClickHouseExporter* exp, chc_packet_kind target, char* errbuf,
                      size_t errlen) {
  for (int i = 0; i < kMaxRecvPackets; i++) {
    chc_packet pkt = {0};
    chc_err err = {0};
    chc_packet_kind kind;

    if (chc_client_recv_packet(exp->client, &pkt, &err) != CHC_OK) {
      chc_packet_clear(exp->client, &pkt);
      ErrBufSet(errbuf, errlen, err.msg[0] ? err.msg : "recv_packet failed");
      return false;
    }
    kind = pkt.kind;
    if (kind == CHC_PKT_EXCEPTION) {
      // display_text is server-controlled and not NUL-terminated; bounded
      // copy only — never strcpy/strlcpy from it.
      if (pkt.exception != NULL && pkt.exception->display_text != NULL)
        ErrBufSetN(errbuf, errlen, pkt.exception->display_text, pkt.exception->display_text_len);
      else
        ErrBufSet(errbuf, errlen, "server exception");
      chc_packet_clear(exp->client, &pkt);
      return false;
    }
    chc_packet_clear(exp->client, &pkt);
    if (kind == target)
      return true;
  }
  ErrBufSet(errbuf, errlen, "too many packets awaiting response");
  return false;
}

static bool SendInsert(ClickHouseExporter* exp, const chc_block_builder* bb, char* errbuf,
                       size_t errlen) {
  chc_err err = {0};

  SetReadDeadline(exp);

  if (chc_client_send_query(exp->client, exp->insert_query, exp->insert_query_len, "", 0, &err) !=
      CHC_OK) {
    ErrBufSet(errbuf, errlen, err.msg[0] ? err.msg : "send_query failed");
    return false;
  }
  // Server echoes a 0-row Data block describing the target columns.
  if (!RecvUntil(exp, CHC_PKT_DATA, errbuf, errlen))
    return false;

  if (chc_client_send_data(exp->client, bb, &err) != CHC_OK) {
    ErrBufSet(errbuf, errlen, err.msg[0] ? err.msg : "send_data failed");
    return false;
  }
  // Empty trailing block terminates the INSERT stream.
  if (chc_client_send_data(exp->client, NULL, &err) != CHC_OK) {
    ErrBufSet(errbuf, errlen, err.msg[0] ? err.msg : "send_data terminator failed");
    return false;
  }

  SetReadDeadline(exp);
  return RecvUntil(exp, CHC_PKT_END_OF_STREAM, errbuf, errlen);
}

// ---------------------------------------------------------------------------
// Column fill (pure memcpy into preallocated buffers; infallible by
// construction: every length is clamped to the buffer reservation first)
// ---------------------------------------------------------------------------

static inline void PutI16(ChColData* cd, int row, int16 v) {
  ((int16*)cd->data)[row] = v;
}
static inline void PutI32(ChColData* cd, int row, int32 v) {
  ((int32*)cd->data)[row] = v;
}
static inline void PutI64(ChColData* cd, int row, int64 v) {
  ((int64*)cd->data)[row] = v;
}
static inline void PutU8(ChColData* cd, int row, uint8 v) {
  ((uint8*)cd->data)[row] = v;
}
static inline void PutU64(ChColData* cd, int row, uint64 v) {
  ((uint64*)cd->data)[row] = v;
}

// Caller guarantees len <= the column's max_len reservation (clamped).
static inline void PutStr(ChColData* cd, int row, const char* src, uint32 len) {
  if (len > 0)
    memcpy(cd->data + cd->bytes_used, src, len);
  cd->bytes_used += len;
  cd->offsets[row] = cd->bytes_used;
}

static void FillColumns(ClickHouseExporter* exp, const PschEvent* events, int n) {
  ChColData* cols = exp->cols;

  for (int c = 0; c < CH_NUM_COLS; c++)
    cols[c].bytes_used = 0;

  for (int i = 0; i < n; i++) {
    const PschEvent* ev = &events[i];
    const char* cmd = PschCmdTypeToString(ev->cmd_type);
    uint32 len;

    PutI64(&cols[CH_COL_TS_START], i, ev->ts_start + kPostgresEpochOffsetUs);
    PutU64(&cols[CH_COL_DURATION_US], i, ev->duration_us);

    len = ClampFieldLen(ev->datname_len, sizeof(ev->datname), "datname_len");
    PutStr(&cols[CH_COL_DB], i, ev->datname, len);
    len = ClampFieldLen(ev->username_len, sizeof(ev->username), "username_len");
    PutStr(&cols[CH_COL_USERNAME], i, ev->username, len);

    PutI32(&cols[CH_COL_PID], i, ev->pid);
    PutI64(&cols[CH_COL_QUERY_ID], i, (int64)ev->queryid);
    PutStr(&cols[CH_COL_CMD_TYPE], i, cmd, (uint32)strlen(cmd));
    PutU64(&cols[CH_COL_ROWS], i, ev->rows);

    len = ClampFieldLen(ev->query_len, PSCH_MAX_QUERY_LEN, "query_len");
    PutStr(&cols[CH_COL_QUERY], i, ev->query, len);

    PutI64(&cols[CH_COL_SHARED_BLKS_HIT], i, ev->shared_blks_hit);
    PutI64(&cols[CH_COL_SHARED_BLKS_READ], i, ev->shared_blks_read);
    PutI64(&cols[CH_COL_SHARED_BLKS_DIRTIED], i, ev->shared_blks_dirtied);
    PutI64(&cols[CH_COL_SHARED_BLKS_WRITTEN], i, ev->shared_blks_written);
    PutI64(&cols[CH_COL_LOCAL_BLKS_HIT], i, ev->local_blks_hit);
    PutI64(&cols[CH_COL_LOCAL_BLKS_READ], i, ev->local_blks_read);
    PutI64(&cols[CH_COL_LOCAL_BLKS_DIRTIED], i, ev->local_blks_dirtied);
    PutI64(&cols[CH_COL_LOCAL_BLKS_WRITTEN], i, ev->local_blks_written);
    PutI64(&cols[CH_COL_TEMP_BLKS_READ], i, ev->temp_blks_read);
    PutI64(&cols[CH_COL_TEMP_BLKS_WRITTEN], i, ev->temp_blks_written);

    PutI64(&cols[CH_COL_SHARED_BLK_READ_TIME_US], i, ev->shared_blk_read_time_us);
    PutI64(&cols[CH_COL_SHARED_BLK_WRITE_TIME_US], i, ev->shared_blk_write_time_us);
    PutI64(&cols[CH_COL_LOCAL_BLK_READ_TIME_US], i, ev->local_blk_read_time_us);
    PutI64(&cols[CH_COL_LOCAL_BLK_WRITE_TIME_US], i, ev->local_blk_write_time_us);
    PutI64(&cols[CH_COL_TEMP_BLK_READ_TIME_US], i, ev->temp_blk_read_time_us);
    PutI64(&cols[CH_COL_TEMP_BLK_WRITE_TIME_US], i, ev->temp_blk_write_time_us);

    PutI64(&cols[CH_COL_WAL_RECORDS], i, ev->wal_records);
    PutI64(&cols[CH_COL_WAL_FPI], i, ev->wal_fpi);
    PutU64(&cols[CH_COL_WAL_BYTES], i, ev->wal_bytes);

    PutI64(&cols[CH_COL_CPU_USER_TIME_US], i, ev->cpu_user_time_us);
    PutI64(&cols[CH_COL_CPU_SYS_TIME_US], i, ev->cpu_sys_time_us);

    PutI32(&cols[CH_COL_JIT_FUNCTIONS], i, ev->jit_functions);
    PutI32(&cols[CH_COL_JIT_GENERATION_TIME_US], i, ev->jit_generation_time_us);
    PutI32(&cols[CH_COL_JIT_DEFORM_TIME_US], i, ev->jit_deform_time_us);
    PutI32(&cols[CH_COL_JIT_INLINING_TIME_US], i, ev->jit_inlining_time_us);
    PutI32(&cols[CH_COL_JIT_OPTIMIZATION_TIME_US], i, ev->jit_optimization_time_us);
    PutI32(&cols[CH_COL_JIT_EMISSION_TIME_US], i, ev->jit_emission_time_us);

    PutI16(&cols[CH_COL_PARALLEL_WORKERS_PLANNED], i, ev->parallel_workers_planned);
    PutI16(&cols[CH_COL_PARALLEL_WORKERS_LAUNCHED], i, ev->parallel_workers_launched);

    // FixedString(5): err_sqlstate[6] always holds >= 5 bytes, NUL-filled
    // when shorter — a verbatim 5-byte copy matches the old NUL-padded slab.
    memcpy(cols[CH_COL_ERR_SQLSTATE].data + (size_t)i * 5, ev->err_sqlstate, 5);
    PutU8(&cols[CH_COL_ERR_ELEVEL], i, ev->err_elevel);
    len = ClampFieldLen(ev->err_message_len, PSCH_MAX_ERR_MSG_LEN, "err_message_len");
    PutStr(&cols[CH_COL_ERR_MESSAGE], i, ev->err_message, len);

    len = ClampFieldLen(ev->application_name_len, PSCH_MAX_APP_NAME_LEN, "app_name_len");
    PutStr(&cols[CH_COL_APP], i, ev->application_name, len);
    len = ClampFieldLen(ev->client_addr_len, PSCH_MAX_CLIENT_ADDR_LEN, "client_addr_len");
    PutStr(&cols[CH_COL_CLIENT_ADDR], i, ev->client_addr, len);
  }
}

static void AppendColumn(ClickHouseExporter* exp, int col, size_t n_rows) {
  const ChColDesc* d = &kChCols[col];
  const ChColData* cd = &exp->cols[col];

  if (exp->build_err.code != CHC_OK)
    return;

  if (d->kind == CH_KIND_STRING) {
    chc_block_builder_append_string(exp->bb, d->name, strlen(d->name), cd->offsets,
                                    (const uint8_t*)cd->data, n_rows, &exp->build_err);
    return;
  }

  // Eager parse at connect guarantees the type; unreachable unless the
  // commit path ran without a connection.
  if (exp->types[d->type_id] == NULL) {
    exp->build_err.code = CHC_ERR_USAGE;
    snprintf(exp->build_err.msg, sizeof exp->build_err.msg, "type %s not parsed",
             kChTypeNames[d->type_id]);
    return;
  }
  chc_block_builder_append_fixed(exp->bb, d->name, strlen(d->name), exp->types[d->type_id],
                                 cd->data, n_rows, &exp->build_err);
}

// One INSERT for up to chunk_rows events.  The goto tail replaces the three
// C++ RAII guards: destroy the live builder, clear the slot, and ALWAYS
// reset the batch context — a missed reset is unbounded TopMemoryContext
// growth (the original SIGABRT failure class).
static PschExportStatus ChCommitChunk(ClickHouseExporter* exp, const PschEvent* events, int n) {
  PschExportStatus status;
  chc_block_builder* bb = NULL;
  chc_err err = {0};
  char errbuf[256];
  bool sent;

  if (!MemoryContextsReady(exp)) {
    RecordFailure(exp, "ClickHouse commit failed", "allocator not initialized", false);
    return PSCH_EXPORT_ERR_NOMEM;
  }

  if (chc_block_builder_init(&bb, &exp->batch_al, &err) != CHC_OK) {
    RecordFailure(exp, "block builder init failed", err.msg[0] ? err.msg : "OOM", false);
    status = PSCH_EXPORT_ERR_NOMEM;
    goto done;
  }
  exp->bb = bb;

  if (exp->client == NULL && (!EstablishNewConnection(exp) || exp->client == NULL)) {
    RecordFailure(exp, "ClickHouse connection failed", "connection not established", false);
    status = PSCH_EXPORT_ERR_CONN;
    goto done;
  }

  FillColumns(exp, events, n);

  chc_err_reset(&exp->build_err);
  for (int c = 0; c < CH_NUM_COLS; c++)
    AppendColumn(exp, c, (size_t)n);
  if (exp->build_err.code != CHC_OK) {
    RecordFailure(exp, "failed to build ClickHouse block",
                  exp->build_err.msg[0] ? exp->build_err.msg : "block build failed", false);
    status = PSCH_EXPORT_ERR_INTERNAL;
    goto done;
  }

  elog(DEBUG1, "pg_stat_ch: Inserting Block to ClickHouse");
  exp->in_flight = true;  // survives a longjmp; forces reconnect on next entry
  sent = SendInsert(exp, exp->bb, errbuf, sizeof errbuf);
  exp->in_flight = false;  // a returned failure closes the connection below
  if (!sent) {
    RecordFailure(exp, "failed to insert to ClickHouse", errbuf, true);
    status = PSCH_EXPORT_ERR_SEND;
    goto done;
  }

  elog(DEBUG1, "pg_stat_ch: exported %d events to ClickHouse", n);
  status = PSCH_EXPORT_OK;

done:
  if (exp->bb != NULL) {
    chc_block_builder_destroy(exp->bb);
    exp->bb = NULL;
  }
  ResetBatchContext(exp);
  return status;
}

// ---------------------------------------------------------------------------
// PschExporterOps implementation
// ---------------------------------------------------------------------------

static bool ChConnect(PschExporter* self, char* errbuf, size_t errlen) {
  ClickHouseExporter* exp = (ClickHouseExporter*)self;

  if (exp->in_flight) {
    CloseConnection(exp);
    exp->in_flight = false;
  }
  if (!EstablishNewConnection(exp)) {
    ErrBufSet(errbuf, errlen, "could not connect to ClickHouse");
    exp->consecutive_failures++;
    return false;
  }
  return true;
}

static bool ChIsConnected(const PschExporter* self) {
  const ClickHouseExporter* exp = (const ClickHouseExporter*)self;
  return exp->client != NULL;
}

static PschExportStatus ChExportEvents(PschExporter* self, const PschEvent* events, int nevents,
                                       int* exported_out) {
  ClickHouseExporter* exp = (ClickHouseExporter*)self;
  int done = 0;

  if (exported_out != NULL)
    *exported_out = 0;

  // Recover from a longjmp that interrupted the previous call: drop any live
  // builder (the context reset reclaims its memory) and force a reconnect if
  // a wire exchange was cut mid-protocol — the server would still be waiting
  // for INSERT data on the old stream.
  exp->bb = NULL;
  ResetBatchContext(exp);
  if (exp->in_flight) {
    elog(WARNING, "pg_stat_ch: previous ClickHouse exchange was interrupted; reconnecting");
    CloseConnection(exp);
    exp->in_flight = false;
  }

  if (events == NULL || nevents <= 0)
    return PSCH_EXPORT_OK;

  // The driver stages at most chunk_rows (== PschExportArenaPlan.staging_events)
  // per call, so this loop normally runs once; the split only guards against
  // a caller exceeding the preallocated column capacity.
  while (done < nevents) {
    int n = Min(nevents - done, exp->chunk_rows);
    PschExportStatus status = ChCommitChunk(exp, events + done, n);

    if (status != PSCH_EXPORT_OK)
      return status;
    done += n;
  }

  exp->consecutive_failures = 0;
  if (exported_out != NULL)
    *exported_out = nevents;
  return PSCH_EXPORT_OK;
}

static int ChConsecutiveFailures(const PschExporter* self) {
  return ((const ClickHouseExporter*)self)->consecutive_failures;
}

static void ChResetFailures(PschExporter* self) {
  ((ClickHouseExporter*)self)->consecutive_failures = 0;
}

static uint64 ChMemUsed(const PschExporter* self) {
  return ((const ClickHouseExporter*)self)->mem_used;
}

static void ChDestroy(PschExporter* self) {
  ClickHouseExporter* exp = (ClickHouseExporter*)self;

  if (exp == NULL)
    return;
  // A longjmp may have left a builder live inside batch_cxt; the context
  // delete below reclaims its memory, so only the pointer is dropped here.
  exp->bb = NULL;
  CloseConnection(exp);
  if (exp->batch_cxt != NULL) {
    MemoryContextDelete(exp->batch_cxt);
    exp->batch_cxt = NULL;
  }
  if (exp->col_cxt != NULL) {
    MemoryContextDelete(exp->col_cxt);
    exp->col_cxt = NULL;
  }
  if (exp->conn_cxt != NULL) {
    MemoryContextDelete(exp->conn_cxt);
    exp->conn_cxt = NULL;
  }
  pfree(exp);
}

// ---------------------------------------------------------------------------
// Creation
// ---------------------------------------------------------------------------

static char* BuildInsertQuery(ClickHouseExporter* exp, size_t* len_out) {
  static const char kPrefix[] = "INSERT INTO events_raw (";
  static const char kSuffix[] = ") VALUES";
  size_t need = (sizeof kPrefix - 1) + (sizeof kSuffix - 1);
  char* q;
  char* p;

  for (int c = 0; c < CH_NUM_COLS; c++)
    need += strlen(kChCols[c].name) + (c != 0 ? 2 : 0);

  q = (char*)MemoryContextAllocExtended(exp->col_cxt, need + 1, MCXT_ALLOC_NO_OOM);
  if (q == NULL)
    return NULL;

  p = q;
  memcpy(p, kPrefix, sizeof kPrefix - 1);
  p += sizeof kPrefix - 1;
  for (int c = 0; c < CH_NUM_COLS; c++) {
    size_t l = strlen(kChCols[c].name);

    if (c != 0) {
      *p++ = ',';
      *p++ = ' ';
    }
    memcpy(p, kChCols[c].name, l);
    p += l;
  }
  memcpy(p, kSuffix, sizeof kSuffix - 1);
  p += sizeof kSuffix - 1;
  *p = '\0';

  Assert((size_t)(p - q) == need);
  *len_out = need;
  return q;
}

PschExporter* PschClickHouseExporterCreate(char* errbuf, size_t errlen) {
  ClickHouseExporter* exp;
  const PschMemoryBudget* budget;
  PschExportArenaPlan plan;

  exp = (ClickHouseExporter*)MemoryContextAllocExtended(
      TopMemoryContext, sizeof(ClickHouseExporter), MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO);
  if (exp == NULL) {
    ErrBufSet(errbuf, errlen, "out of memory allocating ClickHouse exporter");
    return NULL;
  }
  exp->base.ops = &kClickHouseOps;
  exp->fd = -1;
  exp->mem_used = sizeof(ClickHouseExporter);

  if (!EnsureMemoryContexts(exp)) {
    ErrBufSet(errbuf, errlen, "out of memory creating exporter memory contexts");
    goto fail;
  }

  // Buffer sizes are init-time constants: the budget is resolved once per
  // process, deliberately independent of SIGHUP-reloadable GUCs.
  budget = PschMemoryBudgetGet();
  PschExportArenaSplit(budget->export_arena_bytes, &plan);
  exp->chunk_rows = plan.staging_events > 0 ? plan.staging_events : 256;

  for (int c = 0; c < CH_NUM_COLS; c++) {
    const ChColDesc* d = &kChCols[c];
    size_t data_bytes;

    Assert(d->name != NULL);
    Assert(d->kind == CH_KIND_STRING ? d->max_len > 0 : d->elem_size > 0);

    if (d->kind == CH_KIND_STRING) {
      size_t off_bytes = (size_t)exp->chunk_rows * sizeof(uint64_t);

      data_bytes = (size_t)exp->chunk_rows * d->max_len;
      exp->cols[c].offsets = (uint64_t*)MemoryContextAllocExtended(
          exp->col_cxt, off_bytes, MCXT_ALLOC_NO_OOM | MCXT_ALLOC_HUGE);
      if (exp->cols[c].offsets == NULL)
        goto oom;
      exp->mem_used += off_bytes;
    } else {
      data_bytes = (size_t)exp->chunk_rows * d->elem_size;
    }

    exp->cols[c].data = (char*)MemoryContextAllocExtended(exp->col_cxt, data_bytes,
                                                          MCXT_ALLOC_NO_OOM | MCXT_ALLOC_HUGE);
    if (exp->cols[c].data == NULL)
      goto oom;
    exp->mem_used += data_bytes;
  }

  exp->insert_query = BuildInsertQuery(exp, &exp->insert_query_len);
  if (exp->insert_query == NULL)
    goto oom;
  exp->mem_used += exp->insert_query_len + 1;

  return &exp->base;

oom:
  ErrBufSet(errbuf, errlen, "out of memory preallocating ClickHouse column buffers");
fail:
  ChDestroy(&exp->base);
  return NULL;
}
