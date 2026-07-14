// pg_stat_ch background worker implementation
//
// Signal handling notes:
//
// PostgreSQL uses SIGUSR1 for inter-process signaling via the "procsignal"
// mechanism (storage/procsignal.h). Operations like DROP DATABASE use
// ProcSignalBarrier to coordinate across all backends - they send SIGUSR1
// and wait for each backend to acknowledge by calling ProcessProcSignalBarrier().
//
// We MUST use procsignal_sigusr1_handler for SIGUSR1. A previous bug used a
// custom handler which prevented barrier acknowledgment, causing DROP DATABASE
// to hang indefinitely. The fix:
//   1. Use procsignal_sigusr1_handler for SIGUSR1 (handles barriers)
//   2. Use SIGUSR2 for extension-specific immediate flush requests
//   3. Add socket timeouts to ClickHouse connections as a safety net
//
// Signal assignments:
//   SIGHUP  -> SignalHandlerForConfigReload (reload postgresql.conf)
//   SIGTERM -> die() (graceful shutdown)
//   SIGUSR1 -> procsignal_sigusr1_handler (PostgreSQL internal - barriers, etc.)
//   SIGUSR2 -> HandleFlushSignal (extension-specific - immediate flush)
//   SIGPIPE -> SIG_IGN (ignore broken pipe from network)

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
#include "utils/wait_event.h"

#include "queue/psch_dsa.h"
#include "queue/shmem.h"

#include <signal.h>
#ifdef __GLIBC__
#include <malloc.h>
#endif

#include "config/guc.h"
#include "export/stats_exporter.h"
#include "worker/bgworker.h"

// Custom wait event for pg_stat_activity visibility
static uint32 psch_wait_event_main = 0;

// SIGUSR2 handler: wake the worker for immediate flush.
// Note: SIGUSR1 is reserved for PostgreSQL's procsignal mechanism.
static void HandleFlushSignal(SIGNAL_ARGS) {
  (void)postgres_signal_arg;
  int save_errno = errno;
  SetLatch(MyLatch);
  errno = save_errno;
}

// Set up signal handlers before unblocking signals.
// Pattern from worker_spi.c:158-163 and autovacuum.c.
//
// CRITICAL: We MUST use procsignal_sigusr1_handler for SIGUSR1.
// This handler processes PostgreSQL's internal signals including:
//   - PROCSIG_BARRIER: Global barrier for DROP DATABASE, DROP TABLESPACE, etc.
//   - PROCSIG_CATCHUP_INTERRUPT: Shared invalidation catchup
//   - PROCSIG_NOTIFY_INTERRUPT: LISTEN/NOTIFY
//   - PROCSIG_LOG_MEMORY_CONTEXT: Memory context logging
//   - PROCSIG_RECOVERY_CONFLICT_*: Standby recovery conflicts
//
// If we don't use this handler, operations that require barrier acknowledgment
// (like DROP DATABASE) will hang indefinitely waiting for this worker.
static void SetupSignalHandlers(void) {
  pqsignal(SIGHUP, SignalHandlerForConfigReload);
  pqsignal(SIGTERM, die);
  pqsignal(SIGUSR1, procsignal_sigusr1_handler);  // REQUIRED for barriers
  pqsignal(SIGUSR2, HandleFlushSignal);           // Extension-specific flush
  pqsignal(SIGPIPE, SIG_IGN);
}

// Handle SIGHUP config reload
static void HandleConfigReload(void) {
  if (ConfigReloadPending != 0) {
    ConfigReloadPending = 0;
    ProcessConfigFile(PGC_SIGHUP);
    elog(DEBUG1, "pg_stat_ch: configuration reloaded");
  }
}

// Callback for bgworker process exit (registered via on_proc_exit).
// Clear bgworker_pid before exporter teardown so a concurrent
// pg_stat_ch_flush() cannot race a SIGUSR2 to a recycled PID.
static void PschBgworkerShutdown(int code pg_attribute_unused(),
                                 Datum arg pg_attribute_unused()) {
  PschSetBgworkerPid(0);
  PschExporterShutdown();
}

// Process pending signals: barriers, interrupts (SIGTERM/SIGINT), config reload.
// Called after WaitLatch wakes and between batches in the drain loop.
static void ProcessPendingSignals(void) {
  // Barriers first: ProcSignalBarrierPending is set when operations like
  // DROP DATABASE need all backends to acknowledge. Failing to process
  // causes those operations to hang.
  if (ProcSignalBarrierPending) {
    ProcessProcSignalBarrier();
  }
  CHECK_FOR_INTERRUPTS();
  HandleConfigReload();
}

// Drain the queue: loop exporting batches until a partial batch (< batch_max)
// indicates the queue is nearly empty. Each batch gets its own PG_TRY/PG_CATCH
// so an error on batch N+1 doesn't lose batches 1..N. Signals are processed
// between batches to stay responsive to SIGTERM, barriers, and config reload.
static void ExportBatchWithRecovery(void) {
  pgstat_report_activity(STATE_RUNNING, "exporting to ClickHouse");

  for (;;) {
    // volatile: required because PG_TRY/PG_CATCH uses setjmp/longjmp.
    // Without it, the compiler may keep 'exported' in a register that
    // gets clobbered on longjmp, making the value undefined in PG_CATCH.
    volatile int exported = 0;

    PG_TRY();
    { exported = PschExportBatch(); }
    PG_CATCH();
    {
      EmitErrorReport();
      FlushErrorState();
      elog(WARNING, "pg_stat_ch: export error, will retry");
    }
    PG_END_TRY();

    if (exported < psch_batch_max) {
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

// Calculate sleep time with exponential backoff on failures
static int CalculateSleepMs(void) {
  int sleep_ms = psch_flush_interval_ms;
  int failures = PschGetConsecutiveFailures();
  if (failures > 0) {
    int backoff_ms = PschGetRetryDelayMs();
    sleep_ms = (backoff_ms > sleep_ms) ? backoff_ms : sleep_ms;
    elog(DEBUG1, "pg_stat_ch: %d consecutive failures, sleeping %d ms", failures, sleep_ms);
  }
  return sleep_ms;
}

// Run one export cycle: sleep, process signals, then drain queue if enabled.
//
// Note on blocking: If we're blocked in ClickHouse network I/O when a barrier
// signal arrives, we can't process it until the I/O completes. The socket
// timeouts configured in stats_exporter.cc (30 seconds) bound this delay.
static void RunExportCycle(uint32 wait_event) {
  int sleep_ms = CalculateSleepMs();
  (void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, sleep_ms, wait_event);
  ResetLatch(MyLatch);

  ProcessPendingSignals();

  if (psch_enabled) {
    ExportBatchWithRecovery();
  }
}

void PschBgworkerMain(Datum main_arg pg_attribute_unused()) {
#ifdef __GLIBC__
  // Set before any gRPC/Arrow threads are created; cap glibc malloc arenas to reduce virtual memory usage.
  if (mallopt(M_ARENA_MAX, 4) == 0) {
    elog(DEBUG1, "pg_stat_ch: mallopt(M_ARENA_MAX, 4) failed");
  }
#endif
  SetupSignalHandlers();
  BackgroundWorkerUnblockSignals();
  BackgroundWorkerInitializeConnection("postgres", NULL, 0);

  // Register cleanup callback for graceful shutdown
  on_proc_exit(PschBgworkerShutdown, 0);

  // Store our PID for signaling (used by pg_stat_ch_flush())
  PschSetBgworkerPid(MyProcPid);

  // Attach to DSA area eagerly so the first dequeue doesn't hit lazy init
  PschDsaAttach();

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
