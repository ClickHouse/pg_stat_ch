// pg_stat_ch_memory() — SQL surface for the resolved memory budget.
// One row per budget component (total, ring_queue, intern_table, string_area,
// export_arena) plus the export_dropped counter (post-dequeue poison-batch
// drops, distinct from enqueue-overflow drops in pg_stat_ch_stats).

#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "nodes/execnodes.h"
#include "utils/builtins.h"
#include "utils/tuplestore.h"

#include "config/guc.h"
#include "config/memory_budget.h"

// Canonical declaration belongs to src/queue/shmem.h (export-driver lane);
// duplicated here so the lanes compile independently until integration.
extern uint64 PschGetExportDropped(void);

static const char* SourceText(PschBudgetSource source) {
  return source == PSCH_BUDGET_OVERRIDE ? "override" : "auto";
}

static void PutRow(ReturnSetInfo* rsinfo, const char* component, uint64 bytes, const char* source) {
  Datum values[3];
  bool nulls[3] = {false, false, false};

  values[0] = CStringGetTextDatum(component);
  values[1] = Int64GetDatum((int64)bytes);
  values[2] = CStringGetTextDatum(source);
  tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
}

// SQL function: pg_stat_ch_memory()
// RETURNS TABLE(component text, budget_bytes bigint, source text)
PG_FUNCTION_INFO_V1(pg_stat_ch_memory);
Datum pg_stat_ch_memory(PG_FUNCTION_ARGS) {
  ReturnSetInfo* rsinfo = (ReturnSetInfo*)fcinfo->resultinfo;
  const PschMemoryBudget* budget;
  const char* total_source;

  InitMaterializedSRF(fcinfo, 0);

  budget = PschMemoryBudgetGet();
  if (budget->raised) {
    total_source = "raised";
  } else {
    total_source = psch_memory_limit_mb == -1 ? "auto" : "configured";
  }

  PutRow(rsinfo, "total", budget->total_bytes, total_source);
  PutRow(rsinfo, "ring_queue", budget->ring_bytes, SourceText(budget->ring_source));
  PutRow(rsinfo, "intern_table", budget->intern_bytes, "derived");
  PutRow(rsinfo, "string_area", budget->dsa_bytes, SourceText(budget->dsa_source));
  PutRow(rsinfo, "export_arena", budget->export_arena_bytes, SourceText(budget->arena_source));

  // budget_bytes carries the count for counter rows; the column name is a
  // compromise to keep the view single-shape.
  PutRow(rsinfo, "export_dropped", PschGetExportDropped(), "counter");

  return (Datum)0;
}
