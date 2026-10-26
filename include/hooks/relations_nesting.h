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
// another. `created_level` is an opaque caller-defined tag (e.g.
// transaction nesting level) used by PschRelationsNestingAbortTo.
typedef struct {
  const void* key;
  int created_level;
  bool in_use;
} PschRelationsNestingEntry;

// Reserves a slot for `key`, returning its index, or -1 if all max_depth
// slots are already in use.
static inline int PschRelationsNestingPush(PschRelationsNestingEntry* table, int max_depth,
                                           const void* key, int created_level) {
  for (int i = 0; i < max_depth; i++) {
    if (!table[i].in_use) {
      table[i].key = key;
      table[i].created_level = created_level;
      table[i].in_use = true;
      return i;
    }
  }
  return -1;
}

// Frees and returns the slot reserved for `key`, or -1 if none is (e.g. Push
// returned -1 for it, or PschRelationsNestingAbortTo already released it).
static inline int PschRelationsNestingPop(PschRelationsNestingEntry* table, int max_depth,
                                          const void* key) {
  for (int i = 0; i < max_depth; i++) {
    if (table[i].in_use && table[i].key == key) {
      table[i].in_use = false;
      return i;
    }
  }
  return -1;
}

// Frees every slot created at or after `level` -- an aborting (sub)transaction
// rolls back anything created inside it or nested deeper, so those pushes
// will never see a matching pop.
static inline void PschRelationsNestingAbortTo(PschRelationsNestingEntry* table, int max_depth,
                                               int level) {
  for (int i = 0; i < max_depth; i++) {
    if (table[i].in_use && table[i].created_level >= level) {
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
    if (table[i].in_use && table[i].created_level == committed_level) {
      table[i].created_level = committed_level - 1;
    }
  }
}

#ifdef __cplusplus
}
#endif

#endif  // PG_STAT_CH_INCLUDE_HOOKS_RELATIONS_NESTING_H_
