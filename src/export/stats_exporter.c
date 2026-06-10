// pg_stat_ch statistics export driver.
//
// Owns the bgworker-side export pipeline: peek a chunk of events from the
// shmem ring into a preallocated staging buffer, dispatch it to the selected
// backend (ClickHouse native or OTLP/HTTP, optionally as Arrow IPC), then
// consume or requeue per OTEL_REWRITE_DESIGN.md §5a.
//
// Memory discipline: the staging chunk and the Arrow builder are preallocated
// in PschExporterInit (the only place allowed to ereport(FATAL)); the export
// path performs zero heap allocation and never ereport(ERROR)s.  A longjmp
// can still arrive from elsewhere (bgworker.c PG_CATCHes around us), so batch
// state is reset on entry and no non-reclaimable resource is held across PG
// calls — every buffer here lives for the worker's lifetime.

#include "postgres.h"

#include "utils/timestamp.h"

#include "pg_stat_ch/pg_stat_ch.h"
#include "config/guc.h"
#include "config/memory_budget.h"
#include "export/arrow_batch.h"
#include "export/exporter.h"
#include "export/stats_exporter.h"
#include "queue/event.h"
#include "queue/shmem.h"

// PostgreSQL epoch is 2000-01-01, Unix epoch is 1970-01-01:
// 946684800 seconds = 946684800000000 microseconds.
static const int64 kPostgresEpochOffsetUs = INT64CONST(946684800000000);

// Exponential backoff: 1s * 2^(n-1), capped at 60s.  kMaxConsecutiveFailures
// both caps the exponent (shift stays well under 31 bits) and arms the
// poison-batch valve in PschExportBatch.
static const int kBaseDelayMs = 1000;
static const int kMaxDelayMs = 60000;
static const int kMaxConsecutiveFailures = 10;

// Bound on the psch_extra_attributes change-detection snapshot.  Matches the
// builder's own preallocated attribute storage order of magnitude; a change
// beyond this bound is not detected, which is the same truncation the builder
// applies anyway (arrow_batch.h).
#define PSCH_ATTRS_SNAPSHOT_CAP 2048

// Bgworker-local exporter state.  Everything is preallocated in
// PschExporterInit and released only in PschExporterShutdown.
static PschExporter* g_exporter = NULL;
static PschEvent* g_staging = NULL;
static int g_staging_cap = 0;
static PschArrowBuilder* g_arrow = NULL;
static char g_attrs_snapshot[PSCH_ATTRS_SNAPSHOT_CAP];

static void MaybeDumpArrowBatch(const uint8* data, size_t len) {
  unsigned long long unix_now_ns;
  char path[MAXPGPATH];
  char tmp_path[MAXPGPATH];
  FILE* file;
  size_t written;

  if (data == NULL || len == 0 || psch_debug_arrow_dump_dir == NULL ||
      *psch_debug_arrow_dump_dir == '\0') {
    return;
  }

  unix_now_ns =
      (unsigned long long)((GetCurrentTimestamp() + kPostgresEpochOffsetUs) * INT64CONST(1000));
  snprintf(path, sizeof(path), "%s/arrow_%llu.ipc", psch_debug_arrow_dump_dir, unix_now_ns);
  snprintf(tmp_path, sizeof(tmp_path), "%s/arrow_%llu.ipc.tmp", psch_debug_arrow_dump_dir,
           unix_now_ns);

  file = fopen(tmp_path, "wb");
  if (file == NULL) {
    elog(WARNING, "pg_stat_ch: failed to open Arrow dump file '%s'", tmp_path);
    return;
  }

  written = fwrite(data, 1, len, file);
  fclose(file);
  if (written != len) {
    remove(tmp_path);
    elog(WARNING, "pg_stat_ch: short Arrow dump write to '%s' (%zu/%zu bytes)", tmp_path, written,
         len);
    return;
  }

  // tmp + rename keeps test readers (t/026_arrow_dump.pl) from ever seeing a
  // partially-written payload.
  if (rename(tmp_path, path) != 0) {
    remove(tmp_path);
    elog(WARNING, "pg_stat_ch: failed to finalize Arrow dump file '%s'", path);
  }
}

// Re-parse extra_attributes after a SIGHUP changed the GUC.  The snapshot is
// updated even when the re-parse fails so a persistently-bad value warns once
// per change, not once per batch (the 2/sec flood class this rewrite kills).
static void MaybeRefreshArrowAttributes(void) {
  const char* attrs = (psch_extra_attributes != NULL) ? psch_extra_attributes : "";

  if (strncmp(attrs, g_attrs_snapshot, sizeof(g_attrs_snapshot) - 1) == 0) {
    return;
  }
  if (!PschArrowBuilderSetAttributes(g_arrow, attrs)) {
    elog(WARNING,
         "pg_stat_ch: failed to apply pg_stat_ch.extra_attributes; keeping previous attributes");
  }
  strlcpy(g_attrs_snapshot, attrs, sizeof(g_attrs_snapshot));
}

// Finish + dump + send + reset the accumulated Arrow rows.  On success adds
// the sent rows to *flushed_rows.  An empty builder is a successful no-op.
static PschExportStatus FlushArrowBuilder(PschExporter* ex, int* flushed_rows) {
  const uint8* data = NULL;
  size_t len = 0;
  int rows = 0;
  PschExportStatus status;

  if (!PschArrowBuilderFinish(g_arrow, &data, &len, &rows)) {
    return PSCH_EXPORT_ERR_INTERNAL;
  }
  if (rows <= 0 || data == NULL || len == 0) {
    PschArrowBuilderReset(g_arrow);
    return PSCH_EXPORT_OK;
  }

  MaybeDumpArrowBatch(data, len);

  if (ex->ops->send_arrow == NULL) {
    return PSCH_EXPORT_ERR_INTERNAL;
  }
  status = ex->ops->send_arrow(ex, data, len, rows);
  if (status != PSCH_EXPORT_OK) {
    return status;
  }

  *flushed_rows += rows;
  PschArrowBuilderReset(g_arrow);
  return PSCH_EXPORT_OK;
}

// Arrow passthrough path: append the chunk, flushing whenever the builder
// reports FULL.  *exported_out is set to the number of rows actually delivered
// to the collector (sum of successful sub-batch flushes), on BOTH success and
// failure returns.  This lets the caller count already-delivered events as
// exported and avoid dropping or re-sending them when a later sub-batch fails
// mid-chunk.
static PschExportStatus ExportChunkAsArrow(PschExporter* ex, const PschEvent* events, int n,
                                           int* exported_out) {
  int flushed = 0;
  int i;

  *exported_out = 0;
  if (g_arrow == NULL) {
    return PSCH_EXPORT_ERR_INTERNAL;
  }

  MaybeRefreshArrowAttributes();

  // A longjmp may have interrupted the previous batch mid-build; start clean.
  PschArrowBuilderReset(g_arrow);

  for (i = 0; i < n; i++) {
    PschArrowAppendResult res = PschArrowBuilderAppend(g_arrow, &events[i]);

    if (res == PSCH_ARROW_APPEND_FULL) {
      PschExportStatus status = FlushArrowBuilder(ex, &flushed);
      if (status != PSCH_EXPORT_OK) {
        *exported_out = flushed;
        return status;
      }
      res = PschArrowBuilderAppend(g_arrow, &events[i]);
    }
    if (res != PSCH_ARROW_APPEND_OK) {
      // FULL on a fresh builder means a single un-appendable event: an
      // INTERNAL failure so the poison-batch path drops it instead of
      // wedging the ring.
      *exported_out = flushed;
      return PSCH_EXPORT_ERR_INTERNAL;
    }
  }

  if (PschArrowBuilderNumRows(g_arrow) > 0) {
    PschExportStatus status = FlushArrowBuilder(ex, &flushed);
    if (status != PSCH_EXPORT_OK) {
      *exported_out = flushed;
      return status;
    }
  }

  *exported_out = flushed;
  return PSCH_EXPORT_OK;
}

// Initialize the exporter: select the backend per psch_use_otel (POSTMASTER
// GUC, read once), preallocate the staging chunk and — for the OTel backend —
// the Arrow builder.  Both OTel paths are preallocated up front because
// otel_arrow_passthrough is SIGHUP-switchable.  Allocation/creation failure
// is FATAL (clean bgworker-only restart; bgworker.c registered the shutdown
// callback before calling us, so half-initialized state is reclaimed).
// A connect failure is NOT fatal: events stay queued and the first export
// cycle retries.
bool PschExporterInit(void) {
  const PschMemoryBudget* budget;
  PschExportArenaPlan plan;
  char errbuf[256];

  if (g_exporter != NULL || g_staging != NULL) {
    PschExporterShutdown();
  }

  budget = PschMemoryBudgetGet();
  PschExportArenaSplit(budget->export_arena_bytes, &plan);

  g_staging = malloc(plan.staging_bytes);
  if (g_staging == NULL) {
    ereport(FATAL, (errmsg("pg_stat_ch: out of memory preallocating %zu-byte export staging "
                           "buffer; bgworker will be restarted",
                           plan.staging_bytes)));
  }
  g_staging_cap = plan.staging_events;

  errbuf[0] = '\0';
  if (psch_use_otel) {
    g_exporter = PschOtelExporterCreate(errbuf, sizeof(errbuf));
  } else {
    g_exporter = PschClickHouseExporterCreate(errbuf, sizeof(errbuf));
  }
  if (g_exporter == NULL) {
    ereport(FATAL,
            (errmsg("pg_stat_ch: failed to create %s exporter: %s; bgworker will be restarted",
                    psch_use_otel ? "OTel" : "ClickHouse",
                    errbuf[0] != '\0' ? errbuf : "unknown error")));
  }

  if (psch_use_otel) {
    PschArrowBuilderConfig cfg;
    const char* attrs = (psch_extra_attributes != NULL) ? psch_extra_attributes : "";

    memset(&cfg, 0, sizeof(cfg));
    cfg.scratch_budget_bytes = plan.arrow_scratch_bytes;
    cfg.ipc_budget_bytes = plan.encode_buf_bytes;
    cfg.max_rows = plan.staging_events;
    cfg.extra_attributes = attrs;
    cfg.service_version = PG_STAT_CH_VERSION;

    errbuf[0] = '\0';
    g_arrow = PschArrowBuilderCreate(&cfg, errbuf, sizeof(errbuf));
    if (g_arrow == NULL) {
      ereport(FATAL,
              (errmsg("pg_stat_ch: failed to create Arrow batch builder: %s; bgworker will be "
                      "restarted",
                      errbuf[0] != '\0' ? errbuf : "unknown error")));
    }
    strlcpy(g_attrs_snapshot, attrs, sizeof(g_attrs_snapshot));
  }

  elog(DEBUG1, "pg_stat_ch: exporter initialized (staging=%d events, arena=" UINT64_FORMAT "B)",
       g_staging_cap, budget->export_arena_bytes);

  errbuf[0] = '\0';
  if (!g_exporter->ops->connect(g_exporter, errbuf, sizeof(errbuf))) {
    elog(DEBUG1, "pg_stat_ch: initial exporter connect failed: %s",
         errbuf[0] != '\0' ? errbuf : "unknown error");
    return false;
  }
  return true;
}

// Export one staged chunk.  Returns the number of events exported; 0 on
// empty queue or any failure (the bgworker drain loop breaks on 0 and the
// consecutive-failure state drives backoff — never return a count for a
// failed-but-requeued chunk or the loop spins hot against a dead collector).
int PschExportBatch(void) {
  PschExporter* ex = g_exporter;
  PschExportStatus status;
  int exported = 0;
  int n;

  if (ex == NULL || g_staging == NULL) {
    return 0;
  }

  // Connection check BEFORE peek: a collector/server outage leaves events
  // queued (they survive until the ring overflows, which drops newest-first
  // at the producers — preferable to losing already-captured events).
  if (!ex->ops->is_connected(ex)) {
    char errbuf[256];

    errbuf[0] = '\0';
    if (!ex->ops->connect(ex, errbuf, sizeof(errbuf))) {
      PschRecordExportFailure(errbuf[0] != '\0' ? errbuf : "failed to connect to exporter backend");
      return 0;
    }
  }

  n = PschPeekEvents(g_staging, g_staging_cap);
  if (n <= 0) {
    return 0;
  }

  if (psch_use_otel && psch_otel_arrow_passthrough) {
    status = ExportChunkAsArrow(ex, g_staging, n, &exported);
  } else {
    status = ex->ops->export_events(ex, g_staging, n, &exported);
  }

  if (status == PSCH_EXPORT_OK) {
    PschConsumeEvents(n);
    if (psch_shared_state != NULL && exported > 0) {
      pg_atomic_fetch_add_u64(&psch_shared_state->exported, (uint64)exported);
    }
    PschRecordExportSuccess();
    return exported;
  }

  // On a mid-chunk failure the Arrow path may have already delivered a prefix
  // of `exported` events (the per-record path always reports 0 on failure).
  // Those events are in ClickHouse: count them, never drop them, never re-send
  // them.  Only the undelivered remainder is requeued or dropped.  `exported`
  // is bounded by n, so `undelivered` is non-negative.
  int undelivered = n - exported;

  if (psch_shared_state != NULL && exported > 0) {
    pg_atomic_fetch_add_u64(&psch_shared_state->exported, (uint64)exported);
  }

  switch (status) {
    case PSCH_EXPORT_ERR_CONN:
      // Consume only the delivered prefix; the undelivered remainder is
      // retried once the connection recovers.
      if (exported > 0) {
        PschConsumeEvents(exported);
      }
      PschRecordExportFailure("exporter connection failed");
      break;

    case PSCH_EXPORT_ERR_SEND:
      if (ex->ops->consecutive_failures(ex) >= kMaxConsecutiveFailures) {
        // Poison-batch valve: a chunk that keeps failing to send must not
        // wedge the ring forever.  Consume the whole chunk; drop only the
        // undelivered remainder (the prefix was delivered, counted above).
        PschConsumeEvents(n);
        if (undelivered > 0) {
          PschRecordExportDrop(undelivered);
        }
        PschRecordExportFailure("send failed repeatedly; batch dropped");
        elog(WARNING, "pg_stat_ch: dropping %d events after %d consecutive send failures",
             undelivered, ex->ops->consecutive_failures(ex));
      } else {
        // Consume the delivered prefix; requeue the undelivered remainder so a
        // retry does not re-send the events already accepted by the collector.
        if (exported > 0) {
          PschConsumeEvents(exported);
        }
        PschRecordExportFailure("exporter send failed");
      }
      break;

    case PSCH_EXPORT_ERR_NOMEM:
    case PSCH_EXPORT_ERR_INTERNAL:
      // Build/encode failures are a property of the batch, not the wire:
      // retrying the identical bytes cannot succeed, so drop the undelivered
      // remainder immediately (the delivered prefix was counted above).
      PschConsumeEvents(n);
      if (undelivered > 0) {
        PschRecordExportDrop(undelivered);
      }
      PschRecordExportFailure(status == PSCH_EXPORT_ERR_NOMEM
                                  ? "exporter out of memory; batch dropped"
                                  : "exporter internal error; batch dropped");
      elog(WARNING, "pg_stat_ch: dropping %d events after %s", undelivered,
           status == PSCH_EXPORT_ERR_NOMEM ? "exporter OOM" : "internal export error");
      break;

    default:
      PschRecordExportFailure("unexpected export status");
      break;
  }

  return 0;
}

int PschGetRetryDelayMs(void) {
  int failures;
  int capped;
  int delay;

  if (g_exporter == NULL) {
    return 0;
  }
  failures = g_exporter->ops->consecutive_failures(g_exporter);
  if (failures <= 0) {
    return 0;
  }
  capped = (failures > kMaxConsecutiveFailures) ? kMaxConsecutiveFailures : failures;
  delay = kBaseDelayMs * (1 << (capped - 1));
  return (delay > kMaxDelayMs) ? kMaxDelayMs : delay;
}

int PschGetConsecutiveFailures(void) {
  if (g_exporter == NULL) {
    return 0;
  }
  return g_exporter->ops->consecutive_failures(g_exporter);
}

// Idempotent; runs from the on_proc_exit chain, possibly after a FATAL that
// interrupted PschExporterInit, so every member is NULL-guarded and backend
// destroy must tolerate half-initialized state (exporter.h contract).
void PschExporterShutdown(void) {
  if (g_exporter != NULL) {
    g_exporter->ops->destroy(g_exporter);
    g_exporter = NULL;
  }
  if (g_arrow != NULL) {
    PschArrowBuilderDestroy(g_arrow);
    g_arrow = NULL;
  }
  free(g_staging);
  g_staging = NULL;
  g_staging_cap = 0;
  g_attrs_snapshot[0] = '\0';
  elog(LOG, "pg_stat_ch: statistics exporter shutdown");
}
