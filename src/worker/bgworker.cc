// pg_stat_ch background worker implementation

extern "C" {
#include "postgres.h"

#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "utils/guc.h"
}

#include "worker/bgworker.h"
#include "export/clickhouse_exporter.h"
#include "config/guc.h"

// Flag indicating shutdown was requested
static volatile sig_atomic_t got_sigterm = 0;

extern "C" {

static void PschBgworkerSigterm(SIGNAL_ARGS) {
  int save_errno = errno;
  got_sigterm = 1;
  SetLatch(MyLatch);
  errno = save_errno;
}

static void PschBgworkerSighup(SIGNAL_ARGS) {
  int save_errno = errno;
  ProcessConfigFile(PGC_SIGHUP);
  SetLatch(MyLatch);
  errno = save_errno;
}

void PschBgworkerMain([[maybe_unused]] Datum main_arg) {
  // Set up signal handlers
  pqsignal(SIGTERM, PschBgworkerSigterm);
  pqsignal(SIGHUP, PschBgworkerSighup);

  // We're now ready to receive signals
  BackgroundWorkerUnblockSignals();

  // Connect to the "postgres" database for OID resolution
  BackgroundWorkerInitializeConnection("postgres", nullptr, 0);

  elog(LOG, "pg_stat_ch: background worker started");

  // Initialize ClickHouse exporter
  if (!PschExporterInit()) {
    elog(WARNING, "pg_stat_ch: failed to initialize ClickHouse exporter");
  }

  // Main loop
  while (got_sigterm == 0) {
    int rc;

    // Wait for latch or timeout
    rc = WaitLatch(MyLatch,
                   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                   psch_flush_interval_ms,
                   PG_WAIT_EXTENSION);

    ResetLatch(MyLatch);

    // Check for shutdown
    if (got_sigterm != 0) {
      break;
    }

    // Check for config reload
    if ((rc & WL_LATCH_SET) != 0) {
      // Config might have changed, re-read GUC values
    }

    // Export a batch of events to ClickHouse
    if (psch_enabled) {
      PschExportBatch();
    }
  }

  // Cleanup
  PschExporterShutdown();

  elog(LOG, "pg_stat_ch: background worker shutting down");

  proc_exit(0);
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
