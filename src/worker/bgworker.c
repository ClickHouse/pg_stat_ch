// pg_stat_ch background worker implementation

#include "postgres.h"

#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/procsignal.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/wait_event.h"

#include "queue/psch_dsa.h"
#include "queue/shmem.h"

#include <signal.h>
#ifdef __GLIBC__
#include <malloc.h>
#endif

#include "config/guc.h"
#include "export/psch_exporter.h"
#include "worker/bgworker.h"
#include "worker/exporter_bridge.h"

// Exponential backoff after exporter failures
static const int kBaseDelayMs = 1000;
static const int kMaxDelayMs = 60000;
static const int kMaxConsecutiveFailures = 10;

// Custom wait event for pg_stat_activity visibility
static uint32 psch_wait_event_main = 0;

// Exporter handle, owned here and destroyed by PschBgworkerShutdown
static PschExporter* exporter = NULL;
static bool exporter_config_stale = false;
static bool exporter_connected = false;
static int consecutive_failures = 0;

// Dequeued events and their exporter views, reset after every batch
static MemoryContext batch_cxt = NULL;

static void HandleFlushSignal(SIGNAL_ARGS) {
  (void)postgres_signal_arg;
  int save_errno = errno;
  SetLatch(MyLatch);
  errno = save_errno;
}

// Reserve SIGUSR1 for PostgreSQL barriers, including DROP DATABASE
static void SetupSignalHandlers(void) {
  pqsignal(SIGHUP, SignalHandlerForConfigReload);
  pqsignal(SIGTERM, die);
  pqsignal(SIGUSR1, procsignal_sigusr1_handler);
  pqsignal(SIGUSR2, HandleFlushSignal);
  pqsignal(SIGPIPE, SIG_IGN);
}

// Handle SIGHUP config reload
static void HandleConfigReload(void) {
  if (ConfigReloadPending != 0) {
    ConfigReloadPending = 0;
    ProcessConfigFile(PGC_SIGHUP);
    exporter_config_stale = true;
    elog(DEBUG1, "pg_stat_ch: configuration reloaded");
  }
}

// Clear published PID before teardown to prevent flush signals reaching a recycled PID
static void PschBgworkerShutdown(int code pg_attribute_unused(), Datum arg pg_attribute_unused()) {
  PschSetBgworkerPid(0);
  PschExporterDestroy(exporter);
  exporter = NULL;
  elog(LOG, "pg_stat_ch: statistics exporter shutdown");
}

static void ProcessPendingSignals(void) {
  if (ProcSignalBarrierPending) {
    ProcessProcSignalBarrier();
  }
  CHECK_FOR_INTERRUPTS();
  HandleConfigReload();
}

static void LogConnected(void) {
  if (psch_use_otel) {
    elog(LOG, "pg_stat_ch: connected to OTLP endpoint %s", psch_otel_endpoint);
  } else {
    elog(LOG, "pg_stat_ch: connected to ClickHouse at %s:%d%s", psch_clickhouse_host,
         psch_clickhouse_port, psch_clickhouse_use_tls ? " (TLS)" : "");
  }
}

// Track connection state reported by exporter, log transitions to connected
static void NoteConnection(const PschExporterResult* result) {
  if (result->connected && !exporter_connected) {
    LogConnected();
  }
  exporter_connected = result->connected;
}

// Account a failed exporter call: diagnostics, shared statistics, backoff
static void RecordFailure(const char* operation, PschExporterResult* result) {
  PschLogExporterResult(operation, result);
  if (result->status == PSCH_EXPORTER_FAILED) {
    PschRecordExportFailure(result->error);
    consecutive_failures++;
  }
}

static void CreateExporter(void) {
  PschExporterConfig config;
  PschExporterResult result;

  PschBuildExporterConfig(&config);
  if (PschExporterCreate(&config, &exporter, &result) != PSCH_EXPORTER_OK) {
    PschLogExporterResult("create exporter", &result);
    ereport(FATAL, (errmsg("pg_stat_ch: exporter unavailable, bgworker will be restarted")));
  }
  exporter_config_stale = false;
}

// Apply reloaded GUCs, previous configuration stays in effect on rejection
static void ReconfigureExporter(void) {
  PschExporterConfig config;
  PschExporterResult result;

  PschBuildExporterConfig(&config);
  PschExporterConfigure(exporter, &config, &result);
  PschLogExporterResult("configure exporter", &result);
  exporter_config_stale = false;
}

// Connect if needed. False means back off without dequeuing
static bool ConnectExporter(void) {
  PschExporterResult result;
  PschExporterStatus status = PschExporterConnect(exporter, &result);

  NoteConnection(&result);
  if (status != PSCH_EXPORTER_OK) {
    RecordFailure("connect", &result);
    return false;
  }
  PschLogExporterResult("connect", &result);
  return true;
}

// Dequeue one batch and export it. True when a full batch was exported, so
// queue may hold more
static bool ExportBatch(void) {
  int batch_max = PschEffectiveBatchMax();
  int capacity = Min(batch_max, (int)PschQueueDepth());
  if (capacity == 0) {
    return false;
  }

  MemoryContext oldcxt = MemoryContextSwitchTo(batch_cxt);
  PschEvent* events = palloc(capacity * sizeof(PschEvent));
  PschExportEvent* views = palloc(capacity * sizeof(PschExportEvent));
  MemoryContextSwitchTo(oldcxt);

  int count = 0;
  while (count < capacity && PschDequeueEvent(&events[count])) {
    count++;
  }
  for (int i = 0; i < count; i++) {
    PschToExportEvent(&events[i], &views[i]);
  }

  bool full_batch_exported = false;
  if (count > 0) {
    PschExporterResult result;
    PschExporterStatus status = PschExporterExport(exporter, views, count, &result);

    NoteConnection(&result);
    if (status == PSCH_EXPORTER_OK) {
      PschLogExporterResult("export", &result);
      if (psch_shared_state != NULL) {
        pg_atomic_fetch_add_u64(&psch_shared_state->exported, result.exported);
      }
      PschRecordExportSuccess();
      consecutive_failures = 0;
      full_batch_exported = count == batch_max;
    } else {
      RecordFailure("export", &result);
      elog(DEBUG1, "pg_stat_ch: %u of %d events accepted before failure", result.exported, count);
    }
  }

  MemoryContextReset(batch_cxt);
  return full_batch_exported;
}

// Drain queue in bounded batches until it runs short, export fails, or worker
// is disabled. A PostgreSQL error inside one batch is reported and ends this pass
static void DrainQueue(void) {
  pgstat_report_activity(STATE_RUNNING, "exporting to ClickHouse");

  for (;;) {
    MemoryContext oldcxt = CurrentMemoryContext;
    volatile bool more = false;

    PG_TRY();
    {
      if (exporter_config_stale) {
        ReconfigureExporter();
      }
      more = ConnectExporter() && ExportBatch();
    }
    PG_CATCH();
    {
      MemoryContextSwitchTo(oldcxt);
      EmitErrorReport();
      FlushErrorState();
      MemoryContextReset(batch_cxt);
      elog(WARNING, "pg_stat_ch: export error, will retry");
    }
    PG_END_TRY();

    if (!more) {
      break;
    }
    ProcessPendingSignals();
    if (!psch_enabled) {
      break;
    }
  }

  pgstat_report_activity(STATE_IDLE, NULL);
}

// Initialize wait event for pg_stat_activity visibility
static uint32 InitializeWaitEvent(void) {
#if PG_VERSION_NUM >= 170000
  return WaitEventExtensionNew("PgStatChExporter");
#else
  return PG_WAIT_EXTENSION;
#endif
}

// Exponential backoff: base * 2^(failures-1), capped
static int RetryDelayMs(void) {
  if (consecutive_failures <= 0) {
    return 0;
  }
  int capped_failures = Min(consecutive_failures, kMaxConsecutiveFailures);
  return Min(kBaseDelayMs * (1 << (capped_failures - 1)), kMaxDelayMs);
}

static int CalculateSleepMs(void) {
  int sleep_ms = psch_flush_interval_ms;
  if (consecutive_failures > 0) {
    sleep_ms = Max(RetryDelayMs(), sleep_ms);
    elog(DEBUG1, "pg_stat_ch: %d consecutive failures, sleeping %d ms", consecutive_failures,
         sleep_ms);
  }
  return sleep_ms;
}

// Process PostgreSQL signals between synchronous exporter calls
static void RunExportCycle(uint32 wait_event) {
  int sleep_ms = CalculateSleepMs();
  (void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, sleep_ms, wait_event);
  ResetLatch(MyLatch);

  ProcessPendingSignals();

  if (psch_enabled) {
    DrainQueue();
  }
}

void PschBgworkerMain(Datum main_arg pg_attribute_unused()) {
#ifdef __GLIBC__
  // Cap glibc arenas before gRPC/Arrow threads allocate
  if (mallopt(M_ARENA_MAX, 4) == 0) {
    elog(DEBUG1, "pg_stat_ch: mallopt(M_ARENA_MAX, 4) failed");
  }
#endif
  SetupSignalHandlers();
  BackgroundWorkerUnblockSignals();
  BackgroundWorkerInitializeConnection("postgres", NULL, 0);

  // Register cleanup before creating exporter
  on_proc_exit(PschBgworkerShutdown, 0);

  // Store our PID for signaling (used by pg_stat_ch_flush())
  PschSetBgworkerPid(MyProcPid);

  // Attach to DSA area eagerly so the first dequeue doesn't hit lazy init
  PschDsaAttach();

  batch_cxt =
      AllocSetContextCreate(TopMemoryContext, "pg_stat_ch export batch", ALLOCSET_DEFAULT_SIZES);

  elog(LOG, "pg_stat_ch: background worker started (pid=%d)", MyProcPid);

  // Register custom wait event for pg_stat_activity visibility
  if (psch_wait_event_main == 0) {
    psch_wait_event_main = InitializeWaitEvent();
  }

  pgstat_report_activity(STATE_RUNNING, "initializing exporter");
  CreateExporter();
  if (ConnectExporter()) {
    elog(LOG, "pg_stat_ch: exporter connectivity verified on startup");
  } else {
    elog(WARNING, "pg_stat_ch: failed to connect on startup, will retry on first export");
  }
  pgstat_report_activity(STATE_IDLE, NULL);

  // Main loop (pattern from worker_spi.c:206-290)
  for (;;) {
    RunExportCycle(psch_wait_event_main);
  }
}

// Signal the background worker to flush immediately.
// Called from pg_stat_ch_flush() SQL function.
// Uses SIGUSR2 since SIGUSR1 is reserved for PostgreSQL's procsignal mechanism.
void PschSignalFlush(void) {
  pid_t bgworker_pid = PschGetBgworkerPid();
  if (bgworker_pid == 0) {
    ereport(WARNING, (errmsg("pg_stat_ch: background worker not running")));
  } else if (kill(bgworker_pid, SIGUSR2) != 0) {
    // Stale pid: worker died without running on_proc_exit. Clear shmem so we
    // don't keep signaling a recycled pid; postmaster restart will repopulate.
    // Only clear when shmem still holds the stale pid we observed.
    if (errno == ESRCH) {
      uint32 expected = (uint32)bgworker_pid;
      pg_atomic_compare_exchange_u32(&psch_shared_state->bgworker_pid, &expected, 0);
    }
    ereport(WARNING, (errmsg("pg_stat_ch: failed to signal background worker: %m")));
  }
}

void PschRegisterBgworker(void) {
  BackgroundWorker worker;

  MemSet(&worker, 0, sizeof(worker));
  strlcpy(worker.bgw_name, "pg_stat_ch exporter", BGW_MAXLEN);
  strlcpy(worker.bgw_type, "pg_stat_ch exporter", BGW_MAXLEN);
  worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
  worker.bgw_start_time = BgWorkerStart_ConsistentState;
  worker.bgw_restart_time = 10;  // Restart after 10 seconds on crash
  strlcpy(worker.bgw_library_name, "pg_stat_ch", BGW_MAXLEN);
  strlcpy(worker.bgw_function_name, "PschBgworkerMain", BGW_MAXLEN);
  worker.bgw_main_arg = (Datum)0;
  worker.bgw_notify_pid = 0;

  RegisterBackgroundWorker(&worker);
}
