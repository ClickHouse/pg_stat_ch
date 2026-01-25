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
#include "storage/latch.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/wait_event.h"
}

#include "queue/shmem.h"

#include <csignal>

#include "worker/bgworker.h"
#include "export/clickhouse_exporter.h"
#include "config/guc.h"

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

// Export batch with error recovery
static void ExportBatchWithRecovery() {
  pgstat_report_activity(STATE_RUNNING, "exporting to ClickHouse");

  PG_TRY();
  {
    PschExportBatch();
  }
  PG_CATCH();
  {
    EmitErrorReport();
    FlushErrorState();
    elog(WARNING, "pg_stat_ch: export error, will retry");
  }
  PG_END_TRY();

  pgstat_report_activity(STATE_IDLE, nullptr);
}

extern "C" {

void PschBgworkerMain([[maybe_unused]] Datum main_arg) {
  SetupSignalHandlers();
  BackgroundWorkerUnblockSignals();
  BackgroundWorkerInitializeConnection("postgres", nullptr, 0);

  // Store our PID for signaling
  PschSetBgworkerPid(MyProcPid);

  elog(LOG, "pg_stat_ch: background worker started (pid=%d)", MyProcPid);

  // Register custom wait event for pg_stat_activity visibility
  if (psch_wait_event_main == 0) {
#if PG_VERSION_NUM >= 170000
    psch_wait_event_main = WaitEventExtensionNew("PgStatChExporter");
#else
    // PG16: Use generic extension wait event (custom names not supported)
    psch_wait_event_main = PG_WAIT_EXTENSION;
#endif
  }

  // Initialize ClickHouse exporter
  pgstat_report_activity(STATE_RUNNING, "initializing ClickHouse exporter");
  if (!PschExporterInit()) {
    elog(WARNING, "pg_stat_ch: failed to initialize ClickHouse exporter");
  }
  pgstat_report_activity(STATE_IDLE, nullptr);

  // Main loop (pattern from worker_spi.c:206-290)
  for (;;) {
    // Use exponential backoff sleep if we have consecutive failures
    int sleep_ms = psch_flush_interval_ms;
    int failures = PschGetConsecutiveFailures();
    if (failures > 0) {
      int backoff_ms = PschGetRetryDelayMs();
      sleep_ms = (backoff_ms > sleep_ms) ? backoff_ms : sleep_ms;
      elog(DEBUG1, "pg_stat_ch: %d consecutive failures, sleeping %d ms",
           failures, sleep_ms);
    }

    (void)WaitLatch(MyLatch,
                    WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                    sleep_ms,
                    psch_wait_event_main);
    ResetLatch(MyLatch);

    CHECK_FOR_INTERRUPTS();  // Handles SIGTERM via die()
    HandleConfigReload();

    if (psch_enabled) {
      ExportBatchWithRecovery();
    }
  }
}

void PschSignalFlush(void) {
  int bgworker_pid = PschGetBgworkerPid();
  if (bgworker_pid == 0) {
    ereport(WARNING,
            (errmsg("pg_stat_ch: background worker not running")));
    return;
  }

  // Send SIGUSR1 to wake up the bgworker
  if (kill(bgworker_pid, SIGUSR1) != 0) {
    ereport(WARNING,
            (errmsg("pg_stat_ch: failed to signal background worker")));
  }
}

void PschRegisterBgworker(void) {
  BackgroundWorker worker;

  MemSet(&worker, 0, sizeof(worker));
  snprintf(worker.bgw_name, BGW_MAXLEN, "pg_stat_ch exporter");
  snprintf(worker.bgw_type, BGW_MAXLEN, "pg_stat_ch exporter");
  worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
  worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
  worker.bgw_restart_time = 10;  // Restart after 10 seconds on crash
  snprintf(worker.bgw_library_name, BGW_MAXLEN, "pg_stat_ch");
  snprintf(worker.bgw_function_name, BGW_MAXLEN, "PschBgworkerMain");
  worker.bgw_main_arg = static_cast<Datum>(0);
  worker.bgw_notify_pid = 0;

  RegisterBackgroundWorker(&worker);
}

}  // extern "C"
