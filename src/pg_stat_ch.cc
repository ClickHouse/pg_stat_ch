// pg_stat_ch - Query telemetry exporter to ClickHouse
//
// This is the main entry point for the pg_stat_ch extension.

extern "C" {
#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/builtins.h"
}

#include "pg_stat_ch/pg_stat_ch.h"

#include "config/guc.h"
#include "hooks/hooks.h"
#include "queue/shmem.h"
#include "worker/bgworker.h"

extern "C" {

PG_MODULE_MAGIC;

// Extension initialization - called when shared library is loaded
void _PG_init(void) {
  // Must be loaded via shared_preload_libraries
  if (!process_shared_preload_libraries_in_progress) {
    elog(WARNING, "pg_stat_ch must be loaded via shared_preload_libraries");
    return;
  }

  elog(LOG, "pg_stat_ch %s: initializing", PG_STAT_CH_VERSION);

  // Initialize GUC variables
  PschInitGuc();

  // Install shared memory hooks
  PschInstallShmemHooks();

  // Install executor hooks
  PschInstallHooks();

  // Register background worker
  PschRegisterBgworker();
}

// SQL function: pg_stat_ch_version()
// Returns the extension version string
PG_FUNCTION_INFO_V1(pg_stat_ch_version);
Datum pg_stat_ch_version(PG_FUNCTION_ARGS) {
  PG_RETURN_TEXT_P(cstring_to_text(PG_STAT_CH_VERSION));
}

// SQL function: pg_stat_ch_stats()
// Returns queue statistics as a single row
PG_FUNCTION_INFO_V1(pg_stat_ch_stats);
Datum pg_stat_ch_stats(PG_FUNCTION_ARGS) {
  TupleDesc tupdesc;
  Datum values[6];
  bool nulls[6];
  HeapTuple tuple;

  // Build tuple descriptor
  if (get_call_result_type(fcinfo, nullptr, &tupdesc) != TYPEFUNC_COMPOSITE) {
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("function returning record called in context that cannot "
                    "accept type record")));
  }

  tupdesc = BlessTupleDesc(tupdesc);

  // Get stats
  uint64 enqueued = 0;
  uint64 dropped = 0;
  uint64 exported = 0;
  uint32 queue_size = 0;
  uint32 queue_capacity = 0;

  PschGetStats(&enqueued, &dropped, &exported, &queue_size, &queue_capacity);

  // Fill values
  MemSet(nulls, false, sizeof(nulls));

  values[0] = Int64GetDatum(enqueued);
  values[1] = Int64GetDatum(dropped);
  values[2] = Int64GetDatum(exported);
  values[3] = Int32GetDatum(queue_size);
  values[4] = Int32GetDatum(queue_capacity);

  // Calculate usage percentage
  double usage_pct = 0.0;
  if (queue_capacity > 0) {
    usage_pct = 100.0 * queue_size / queue_capacity;
  }
  values[5] = Float8GetDatum(usage_pct);

  tuple = heap_form_tuple(tupdesc, values, nulls);

  PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

// SQL function: pg_stat_ch_reset()
// Resets all queue counters to zero
PG_FUNCTION_INFO_V1(pg_stat_ch_reset);
Datum pg_stat_ch_reset(PG_FUNCTION_ARGS) {
  PschResetStats();
  PG_RETURN_VOID();
}

}  // extern "C"
