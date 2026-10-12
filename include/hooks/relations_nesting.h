// Pure C, no PostgreSQL headers -- kept separate from hooks.c so it's
// unit-testable in isolation (see test/unit/relations_nesting_test.cc).
#ifndef PG_STAT_CH_INCLUDE_HOOKS_RELATIONS_NESTING_H_
#define PG_STAT_CH_INCLUDE_HOOKS_RELATIONS_NESTING_H_

#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// Keyed by identity (e.g. a QueryDesc*), not by call depth -- so a level
// can be released out of order (concurrently open cursors don't close in
// the order they were opened) without handing one query's buffer to
// another. `created_level` and `active_level` (deepest level used at, see
// PschRelationsNestingTouch) are opaque caller-defined tags used by
// PschRelationsNestingAbortTo.
typedef struct {
  const void* key;
  int created_level;
  int active_level;
  bool in_use;
} PschRelationsNestingEntry;

// Returns the index of the in-use slot reserved for `key`, or -1 if none.
static inline int PschRelationsNestingFind(const PschRelationsNestingEntry* table, int max_depth,
                                           const void* key) {
  for (int i = 0; i < max_depth; i++) {
    if (table[i].in_use && table[i].key == key) {
      return i;
    }
  }
  return -1;
}

// Reserves a slot for `key`, returning its index, or -1 if all max_depth
// slots are already in use. Reclaims an existing in-use entry for the same
// key instead of adding a duplicate -- guards against a prior reservation
// for this address never having been released (see PschRelationsNestingPop).
static inline int PschRelationsNestingPush(PschRelationsNestingEntry* table, int max_depth,
                                           const void* key, int created_level) {
  int reclaim = PschRelationsNestingFind(table, max_depth, key);
  if (reclaim >= 0) {
    table[reclaim].created_level = created_level;
    table[reclaim].active_level = created_level;
    return reclaim;
  }
  for (int i = 0; i < max_depth; i++) {
    if (!table[i].in_use) {
      table[i].key = key;
      table[i].created_level = created_level;
      table[i].active_level = created_level;
      table[i].in_use = true;
      return i;
    }
  }
  return -1;
}

// Records `key` as used at `level`, returning its prior active_level (or -1
// if unknown). Never lowers active_level; call SetActiveLevel to restore it.
static inline int PschRelationsNestingTouch(PschRelationsNestingEntry* table, int max_depth,
                                            const void* key, int level) {
  int i = PschRelationsNestingFind(table, max_depth, key);
  if (i < 0) {
    return -1;
  }
  int prev = table[i].active_level;
  if (level > prev) {
    table[i].active_level = level;
  }
  return prev;
}

// Restores active_level to `level`; no-op if unknown.
static inline void PschRelationsNestingSetActiveLevel(PschRelationsNestingEntry* table,
                                                      int max_depth, const void* key, int level) {
  int i = PschRelationsNestingFind(table, max_depth, key);
  if (i >= 0) {
    table[i].active_level = level;
  }
}

// Frees and returns the slot reserved for `key`, or -1 if none is (e.g. Push
// returned -1 for it, or PschRelationsNestingAbortTo already released it).
static inline int PschRelationsNestingPop(PschRelationsNestingEntry* table, int max_depth,
                                          const void* key) {
  int i = PschRelationsNestingFind(table, max_depth, key);
  if (i >= 0) {
    table[i].in_use = false;
  }
  return i;
}

// Frees every slot created or used at `level` or deeper -- covers both
// rolled-back pushes and older portals PostgreSQL fails mid-abort.
static inline void PschRelationsNestingAbortTo(PschRelationsNestingEntry* table, int max_depth,
                                               int level) {
  for (int i = 0; i < max_depth; i++) {
    if (table[i].in_use &&
        (table[i].created_level >= level || table[i].active_level >= level)) {
      table[i].in_use = false;
    }
  }
}

// A committing (sub)transaction's surviving entries (e.g. a cursor opened
// inside it) must move to its parent's level -- otherwise a later sibling
// subtransaction reusing this same level number would wrongly sweep them on
// its own abort.
static inline void PschRelationsNestingReparentCommitted(PschRelationsNestingEntry* table,
                                                         int max_depth, int committed_level) {
  for (int i = 0; i < max_depth; i++) {
    if (!table[i].in_use) {
      continue;
    }
    if (table[i].created_level == committed_level) {
      table[i].created_level = committed_level - 1;
    }
    if (table[i].active_level == committed_level) {
      table[i].active_level = committed_level - 1;
    }
  }
}

#ifdef __cplusplus
}
#endif

#endif  // PG_STAT_CH_INCLUDE_HOOKS_RELATIONS_NESTING_H_
