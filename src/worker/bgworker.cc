// pg_stat_ch background worker implementation
//
// Follows PostgreSQL's official patterns from worker_spi.c and autovacuum.c:
// - Signal handlers set up BEFORE BackgroundWorkerUnblockSignals()
// - Uses die() for SIGTERM (standard PostgreSQL handler)
// - Uses SignalHandlerForConfigReload for SIGHUP with ConfigReloadPending flag
// - CHECK_FOR_INTERRUPTS() in main loop to process pending signals
// - PG_TRY/PG_CATCH for error recovery

extern "C" {
#include "postgres.h"

#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/wait_event.h"
}

#include "queue/shmem.h"

#include <csignal>

#include "config/guc.h"
#include "export/clickhouse_exporter.h"
#include "worker/bgworker.h"

// Custom wait event for pg_stat_activity visibility
static uint32 psch_wait_event_main = 0;

// SIGUSR1 handler to wake up the latch for immediate flush
static void HandleFlushSignal(SIGNAL_ARGS) {
  int save_errno = errno;
  SetLatch(MyLatch);
  errno = save_errno;
}

// Set up signal handlers before unblocking (per worker_spi.c:158-163)
static void SetupSignalHandlers() {
  pqsignal(SIGHUP, SignalHandlerForConfigReload);
  pqsignal(SIGTERM, die);
  pqsignal(SIGUSR1, HandleFlushSignal);
  pqsignal(SIGPIPE, SIG_IGN);
}

// Handle SIGHUP config reload
static void HandleConfigReload() {
  if (ConfigReloadPending != 0) {
    ConfigReloadPending = 0;
    ProcessConfigFile(PGC_SIGHUP);
    elog(DEBUG1, "pg_stat_ch: configuration reloaded");
  }
}

// Callback for bgworker process exit (registered via on_proc_exit)
static void PschBgworkerShutdown([[maybe_unused]] int code, [[maybe_unused]] Datum arg) {
  PschExporterShutdown();
}

// Export batch with error recovery
static void ExportBatchWithRecovery() {
  pgstat_report_activity(STATE_RUNNING, "exporting to ClickHouse");

  PG_TRY();
  { PschExportBatch(); }
  PG_CATCH();
  {
    EmitErrorReport();
    FlushErrorState();
    elog(WARNING, "pg_stat_ch: export error, will retry");
  }
  PG_END_TRY();

  pgstat_report_activity(STATE_IDLE, nullptr);
}

// Initialize wait event for pg_stat_activity visibility
static uint32 InitializeWaitEvent() {
#if PG_VERSION_NUM >= 170000
  return WaitEventExtensionNew("PgStatChExporter");
#else
  return PG_WAIT_EXTENSION;
#endif
}

// Calculate sleep time with exponential backoff on failures
static int CalculateSleepMs() {
  int sleep_ms = psch_flush_interval_ms;
  int failures = PschGetConsecutiveFailures();
  if (failures > 0) {
    int backoff_ms = PschGetRetryDelayMs();
    sleep_ms = (backoff_ms > sleep_ms) ? backoff_ms : sleep_ms;
    elog(DEBUG1, "pg_stat_ch: %d consecutive failures, sleeping %d ms", failures, sleep_ms);
  }
  return sleep_ms;
}

// Run one export cycle: wait, check signals, and export if enabled
static void RunExportCycle(uint32 wait_event) {
  int sleep_ms = CalculateSleepMs();

  (void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, sleep_ms, wait_event);
  ResetLatch(MyLatch);

  CHECK_FOR_INTERRUPTS();
  HandleConfigReload();

  if (psch_enabled) {
    ExportBatchWithRecovery();
  }
}

extern "C" {

void PschBgworkerMain([[maybe_unused]] Datum main_arg) {
  SetupSignalHandlers();
  BackgroundWorkerUnblockSignals();
  BackgroundWorkerInitializeConnection("postgres", nullptr, 0);

  // Register cleanup callback for graceful shutdown
  on_proc_exit(PschBgworkerShutdown, 0);

  // Store our PID for signaling
  PschSetBgworkerPid(MyProcPid);

  elog(LOG, "pg_stat_ch: background worker started (pid=%d)", MyProcPid);

  // Register custom wait event for pg_stat_activity visibility
  if (psch_wait_event_main == 0) {
    psch_wait_event_main = InitializeWaitEvent();
  }

  // Initialize ClickHouse exporter and verify connectivity
  pgstat_report_activity(STATE_RUNNING, "initializing ClickHouse exporter");
  if (PschExporterInit()) {
    elog(LOG, "pg_stat_ch: ClickHouse connectivity verified on startup");
  } else {
    elog(WARNING,
         "pg_stat_ch: failed to connect to ClickHouse on startup, will retry on first export");
  }
  pgstat_report_activity(STATE_IDLE, nullptr);

  // Main loop (pattern from worker_spi.c:206-290)
  for (;;) {
    RunExportCycle(psch_wait_event_main);
  }
}

void PschSignalFlush(void) {
  int bgworker_pid = PschGetBgworkerPid();
  if (bgworker_pid == 0) {
    ereport(WARNING, (errmsg("pg_stat_ch: background worker not running")));
    return;
  }

  // Send SIGUSR1 to wake up the bgworker
  if (kill(bgworker_pid, SIGUSR1) != 0) {
    ereport(WARNING, (errmsg("pg_stat_ch: failed to signal background worker")));
  }
}

void PschRegisterBgworker(void) {
  BackgroundWorker worker;

  MemSet(&worker, 0, sizeof(worker));
  strlcpy(worker.bgw_name, "pg_stat_ch exporter", BGW_MAXLEN);
  strlcpy(worker.bgw_type, "pg_stat_ch exporter", BGW_MAXLEN);
  worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
  worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
  worker.bgw_restart_time = 10;  // Restart after 10 seconds on crash
  strlcpy(worker.bgw_library_name, "pg_stat_ch", BGW_MAXLEN);
  strlcpy(worker.bgw_function_name, "PschBgworkerMain", BGW_MAXLEN);
  worker.bgw_main_arg = static_cast<Datum>(0);
  worker.bgw_notify_pid = 0;

  RegisterBackgroundWorker(&worker);
}

}  // extern "C"
