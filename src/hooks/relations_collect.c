#include "postgres.h"

#include "catalog/pg_class.h"
#include "common/hashfn.h"
#include "nodes/pathnodes.h"
#include "parser/parsetree.h"
#include "utils/lsyscache.h"

#include "config/guc.h"
#include "hooks/relations_budget.h"
#include "hooks/relations_collect.h"

// Oid set for CollectRelations' dedup below. Created fresh per call in the
// caller's es_query_cxt. Planner-generated partition/inheritance children are
// skipped before they reach this set, so it stays small: it is bounded by the
// byte budget, not by the range-table length.
typedef struct PschRelidSeenEntry {
  Oid relid;
  char status;
} PschRelidSeenEntry;

#define SH_PREFIX            psch_relid_seen
#define SH_ELEMENT_TYPE      PschRelidSeenEntry
#define SH_KEY_TYPE          Oid
#define SH_KEY               relid
#define SH_HASH_KEY(tb, key) murmurhash32((uint32)(key))
#define SH_EQUAL(tb, a, b)   ((a) == (b))
#define SH_SCOPE             static inline
#define SH_DECLARE
#define SH_DEFINE
#include "lib/simplehash.h"

// Fills parent_of_rti[child's rti] = parent's rti for every planner-generated
// partition/inheritance child; everything else stays 0 (the palloc0 default).
// Indexed by range-table index (1..nrt), so parent_of_rti must have nrt+1
// elements.
static void BuildChildMap(Index* parent_of_rti, int nrt, List* append_rel_infos) {
  (void)nrt;  // Only used by the Assert below, which compiles out in release builds.
  ListCell* alc;
  foreach (alc, append_rel_infos) {
    AppendRelInfo* appinfo = (AppendRelInfo*)lfirst(alc);
    Assert(appinfo->child_relid >= 1 && (int)appinfo->child_relid <= nrt);
    parent_of_rti[appinfo->child_relid] = appinfo->parent_relid;
  }
}

// True if the range-table entry at rti should be considered for reporting.
//
// Plain tables, plus view-expansion artifacts: the rewriter turns a
// referenced view into an RTE_SUBQUERY wrapping its definition, but stamps
// relkind='v' on that entry so it's still identifiable as a view.
// RTE_RELATION alone already covers every other relkind (matview,
// partitioned, foreign table) uniformly -- unaffected by that work.
//
// Partition and inheritance children are collapsed into their parent: the
// parent is reported, planner-generated children are not. Collapsing happens
// before deduplication, so a later explicit reference to the same partition
// is still reported.
static bool ShouldReportRte(RangeTblEntry* rte, List* rt, Index* parent_of_rti, int rti) {
  if (rte->rtekind != RTE_RELATION && rte->relkind != RELKIND_VIEW) {
    return false;
  }

  Index parent_rti = parent_of_rti[rti];
  if (parent_rti != 0 && rt_fetch(parent_rti, rt)->rtekind == RTE_RELATION) {
    return false;
  }

  return true;
}

// Appends one [ns_len][ns][name_len][name][relkind] entry to buf. Returns
// false (buf unchanged) if the entry would exceed psch_relations_max_len,
// signaling the caller to stop and flag truncation, same as query text does.
static bool AppendRelationEntry(StringInfo buf, const char* namespace_name,
                                const char* relation_name, char relkind) {
  // NAMEDATALEN-1 (63) max per Postgres identifier rules, so these always
  // fit in a uint8 length prefix.
  size_t ns_len = strlen(namespace_name);
  size_t name_len = strlen(relation_name);
  Assert(ns_len < NAMEDATALEN);
  Assert(name_len < NAMEDATALEN);

  // No separator byte to account for -- every entry is self-delimiting.
  int entry_len = (int)(1 + ns_len + 1 + name_len + 1);
  if (buf->len + entry_len > psch_relations_max_len) {
    return false;
  }

  appendStringInfoChar(buf, (char)ns_len);
  appendBinaryStringInfo(buf, namespace_name, (int)ns_len);
  appendStringInfoChar(buf, (char)name_len);
  appendBinaryStringInfo(buf, relation_name, (int)name_len);
  appendStringInfoChar(buf, (char)relkind);
  return true;
}

void CollectRelations(StringInfo buf, List* rt, List* append_rel_infos) {
  int nrt = list_length(rt);
  // Starting size only: simplehash grows on demand, and the byte budget caps
  // this set at roughly 200 relations, so don't scale it with the range table.
  psch_relid_seen_hash* seen = psch_relid_seen_create(CurrentMemoryContext, 64, NULL);
  uint8 count = 0;
  bool truncated = false;
  int rti = 0;
  ListCell* lc;

  // Defense in depth -- the GUC's own min bound (guc.c) already enforces this.
  Assert(psch_relations_max_len >= PSCH_RELATIONS_HEADER_LEN);

  Index* parent_of_rti = palloc0(sizeof(Index) * (nrt + 1));
  BuildChildMap(parent_of_rti, nrt, append_rel_infos);

  // Reserve the header now; patched with real values once known.
  appendStringInfoChar(buf, 0);
  appendStringInfoChar(buf, 0);

  foreach (lc, rt) {
    rti++;
    RangeTblEntry* rte = (RangeTblEntry*)lfirst(lc);

    if (!ShouldReportRte(rte, rt, parent_of_rti, rti)) {
      continue;
    }

    bool found;
    psch_relid_seen_insert(seen, rte->relid, &found);
    if (found) {
      continue;
    }

    const char* namespace_name = get_namespace_name(get_rel_namespace(rte->relid));
    const char* relation_name = get_rel_name(rte->relid);
    if (namespace_name == NULL || relation_name == NULL) {
      continue;  // Relation dropped concurrently; skip rather than crash.
    }

    if (!AppendRelationEntry(buf, namespace_name, relation_name, (char)rte->relkind)) {
      truncated = true;
      break;
    }
    count++;
  }

  buf->data[0] = truncated ? 1 : 0;
  buf->data[1] = (char)count;
  psch_relid_seen_destroy(seen);
}
