// pg_stat_ch - Query telemetry exporter to ClickHouse
//
// This is the main entry point for the pg_stat_ch extension.

extern "C" {
#include "postgres.h"

#include "access/htup_details.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"
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
// Returns queue statistics as a single row with 10 columns:
//   1. enqueued        - Total events enqueued
//   2. dropped         - Total events dropped due to overflow
//   3. exported        - Total events exported to ClickHouse
//   4. send_failures   - Total ClickHouse send failures
//   5. last_success_ts - Timestamp of last successful export
//   6. last_error_text - Last export error message
//   7. last_error_ts   - Timestamp of last export error
//   8. queue_size      - Current number of events in queue
//   9. queue_capacity  - Maximum queue capacity
//  10. queue_pct       - Queue usage percentage
PG_FUNCTION_INFO_V1(pg_stat_ch_stats);
Datum pg_stat_ch_stats(PG_FUNCTION_ARGS) {
  TupleDesc tupdesc;
  Datum values[10];
  bool nulls[10];
  HeapTuple tuple;

  // Build tuple descriptor
  if (get_call_result_type(fcinfo, nullptr, &tupdesc) != TYPEFUNC_COMPOSITE) {
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
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
  uint64 send_failures = 0;
  TimestampTz last_success_ts = 0;
  const char* last_error_text = nullptr;
  TimestampTz last_error_ts = 0;

  PschGetStats(&enqueued, &dropped, &exported, &queue_size, &queue_capacity, &send_failures,
               &last_success_ts, &last_error_text, &last_error_ts);

  // Fill values
  MemSet(nulls, false, sizeof(nulls));

  values[0] = Int64GetDatum(enqueued);
  values[1] = Int64GetDatum(dropped);
  values[2] = Int64GetDatum(exported);
  values[3] = Int64GetDatum(send_failures);

  // last_success_ts: null if never succeeded
  if (last_success_ts == 0) {
    nulls[4] = true;
    values[4] = (Datum)0;
  } else {
    values[4] = TimestampTzGetDatum(last_success_ts);
  }

  // last_error_text: null if empty
  if (last_error_text == nullptr || last_error_text[0] == '\0') {
    nulls[5] = true;
    values[5] = (Datum)0;
  } else {
    values[5] = CStringGetTextDatum(last_error_text);
  }

  // last_error_ts: null if never failed
  if (last_error_ts == 0) {
    nulls[6] = true;
    values[6] = (Datum)0;
  } else {
    values[6] = TimestampTzGetDatum(last_error_ts);
  }

  values[7] = Int32GetDatum(queue_size);
  values[8] = Int32GetDatum(queue_capacity);

  // Calculate usage percentage
  double usage_pct = 0.0;
  if (queue_capacity > 0) {
    usage_pct = 100.0 * queue_size / queue_capacity;
  }
  values[9] = Float8GetDatum(usage_pct);

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

// SQL function: pg_stat_ch_flush()
// Signals the background worker to immediately flush pending events
PG_FUNCTION_INFO_V1(pg_stat_ch_flush);
Datum pg_stat_ch_flush(PG_FUNCTION_ARGS) {
  PschSignalFlush();
  PG_RETURN_VOID();
}

}  // extern "C"
