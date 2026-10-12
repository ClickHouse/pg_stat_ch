#include "hooks/relations_nesting.h"

#include <gtest/gtest.h>

namespace {
constexpr int kMaxDepth = 16;
const int kKeys[20] = {};  // distinct addresses to use as fake QueryDesc* keys
}  // namespace

TEST(RelationsNesting, PushReservesDistinctSlotsForDistinctKeys) {
  PschRelationsNestingEntry table[kMaxDepth] = {};
  int seen[kMaxDepth] = {};
  for (int i = 0; i < kMaxDepth; i++) {
    int slot = PschRelationsNestingPush(table, kMaxDepth, &kKeys[i], 0);
    ASSERT_GE(slot, 0);
    seen[slot]++;
  }
  for (int i = 0; i < kMaxDepth; i++) {
    EXPECT_EQ(seen[i], 1);
  }
}

TEST(RelationsNesting, PushPastCapacityReturnsNegativeOne) {
  PschRelationsNestingEntry table[kMaxDepth] = {};
  for (int i = 0; i < kMaxDepth; i++) {
    PschRelationsNestingPush(table, kMaxDepth, &kKeys[i], 0);
  }
  EXPECT_EQ(PschRelationsNestingPush(table, kMaxDepth, &kKeys[kMaxDepth], 0), -1);
}

TEST(RelationsNesting, PopReturnsPushedSlotThenFindsNothingTwice) {
  PschRelationsNestingEntry table[kMaxDepth] = {};
  int slot = PschRelationsNestingPush(table, kMaxDepth, &kKeys[0], 0);
  EXPECT_EQ(PschRelationsNestingPop(table, kMaxDepth, &kKeys[0]), slot);
  EXPECT_EQ(PschRelationsNestingPop(table, kMaxDepth, &kKeys[0]), -1);
}

TEST(RelationsNesting, PopByWrongKeyFindsNothingAndLeavesRealEntryIntact) {
  PschRelationsNestingEntry table[kMaxDepth] = {};
  int slot = PschRelationsNestingPush(table, kMaxDepth, &kKeys[0], 0);
  EXPECT_EQ(PschRelationsNestingPop(table, kMaxDepth, &kKeys[1]), -1);
  EXPECT_EQ(PschRelationsNestingPop(table, kMaxDepth, &kKeys[0]), slot);
}

TEST(RelationsNesting, OutOfOrderPopDoesNotCrossContaminate) {
  // Two concurrently open cursors: opened A then B, but closed A first --
  // A's pop must not return B's slot.
  PschRelationsNestingEntry table[kMaxDepth] = {};
  int slot_a = PschRelationsNestingPush(table, kMaxDepth, &kKeys[0], 0);
  int slot_b = PschRelationsNestingPush(table, kMaxDepth, &kKeys[1], 0);
  ASSERT_NE(slot_a, slot_b);

  EXPECT_EQ(PschRelationsNestingPop(table, kMaxDepth, &kKeys[0]), slot_a);
  EXPECT_EQ(PschRelationsNestingPop(table, kMaxDepth, &kKeys[1]), slot_b);
}

TEST(RelationsNesting, AbortReleasesOnlyEntriesAtOrAboveLevel) {
  PschRelationsNestingEntry table[kMaxDepth] = {};
  int slot_a = PschRelationsNestingPush(table, kMaxDepth, &kKeys[0], 1);
  PschRelationsNestingPush(table, kMaxDepth, &kKeys[1], 2);

  PschRelationsNestingAbortTo(table, kMaxDepth, 2);

  EXPECT_EQ(PschRelationsNestingPop(table, kMaxDepth, &kKeys[1]), -1);     // rolled back
  EXPECT_EQ(PschRelationsNestingPop(table, kMaxDepth, &kKeys[0]), slot_a); // survives
}

TEST(RelationsNesting, AbortAtOuterLevelReleasesEverythingNestedInside) {
  PschRelationsNestingEntry table[kMaxDepth] = {};
  PschRelationsNestingPush(table, kMaxDepth, &kKeys[0], 1);
  PschRelationsNestingPush(table, kMaxDepth, &kKeys[1], 2);

  PschRelationsNestingAbortTo(table, kMaxDepth, 1);

  EXPECT_EQ(PschRelationsNestingPop(table, kMaxDepth, &kKeys[0]), -1);
  EXPECT_EQ(PschRelationsNestingPop(table, kMaxDepth, &kKeys[1]), -1);
}

TEST(RelationsNesting, CommitReparentsEntryOutOfReachOfASiblingsLaterAbort) {
  // A cursor opened at level 2, released (committed): without reparenting, a
  // later sibling savepoint reusing level 2 would sweep it away on its abort.
  PschRelationsNestingEntry table[kMaxDepth] = {};
  int cursor_slot = PschRelationsNestingPush(table, kMaxDepth, &kKeys[0], 2);

  PschRelationsNestingReparentCommitted(table, kMaxDepth, 2);
  PschRelationsNestingPush(table, kMaxDepth, &kKeys[1], 2);
  PschRelationsNestingAbortTo(table, kMaxDepth, 2);

  EXPECT_EQ(PschRelationsNestingPop(table, kMaxDepth, &kKeys[0]), cursor_slot);
  EXPECT_EQ(PschRelationsNestingPop(table, kMaxDepth, &kKeys[1]), -1);
}

TEST(RelationsNesting, CommitReparentsToLevelZeroSurvivesANextTransactionsAbort) {
  // A WITH HOLD cursor surviving a top-level commit: nestingLevel resets to
  // 1 for the next transaction, so without reparenting to 0 that
  // transaction's own abort would sweep it away too.
  PschRelationsNestingEntry table[kMaxDepth] = {};
  int cursor_slot = PschRelationsNestingPush(table, kMaxDepth, &kKeys[0], 1);

  PschRelationsNestingReparentCommitted(table, kMaxDepth, 1);
  PschRelationsNestingPush(table, kMaxDepth, &kKeys[1], 1);
  PschRelationsNestingAbortTo(table, kMaxDepth, 1);

  EXPECT_EQ(PschRelationsNestingPop(table, kMaxDepth, &kKeys[0]), cursor_slot);
  EXPECT_EQ(PschRelationsNestingPop(table, kMaxDepth, &kKeys[1]), -1);
}

TEST(RelationsNesting, TouchIsNoOpForAnUnknownKey) {
  PschRelationsNestingEntry table[kMaxDepth] = {};
  int slot = PschRelationsNestingPush(table, kMaxDepth, &kKeys[0], 1);

  PschRelationsNestingTouch(table, kMaxDepth, &kKeys[1], 5);

  EXPECT_EQ(PschRelationsNestingPop(table, kMaxDepth, &kKeys[0]), slot);
}

TEST(RelationsNesting, TouchNeverLowersAnAlreadyDeeperActiveLevel) {
  PschRelationsNestingEntry table[kMaxDepth] = {};
  PschRelationsNestingPush(table, kMaxDepth, &kKeys[0], 1);
  PschRelationsNestingTouch(table, kMaxDepth, &kKeys[0], 3);
  PschRelationsNestingTouch(table, kMaxDepth, &kKeys[0], 1);

  PschRelationsNestingAbortTo(table, kMaxDepth, 3);

  EXPECT_EQ(PschRelationsNestingPop(table, kMaxDepth, &kKeys[0]), -1);
}

// The leak this guards against: a cursor declared at level 1, FETCHed from
// at level 2 (inside a savepoint) that fails -- created_level alone misses it.
TEST(RelationsNesting, AbortSweepsAnOlderCursorThatWasFetchedFromInsideTheAbortingSavepoint) {
  PschRelationsNestingEntry table[kMaxDepth] = {};
  PschRelationsNestingPush(table, kMaxDepth, &kKeys[0], 1);
  PschRelationsNestingTouch(table, kMaxDepth, &kKeys[0], 2);

  PschRelationsNestingAbortTo(table, kMaxDepth, 2);

  EXPECT_EQ(PschRelationsNestingPop(table, kMaxDepth, &kKeys[0]), -1);
}

TEST(RelationsNesting, AbortAtADeeperLevelThanAnyUseLeavesTheCursorAlone) {
  PschRelationsNestingEntry table[kMaxDepth] = {};
  int cursor_slot = PschRelationsNestingPush(table, kMaxDepth, &kKeys[0], 1);
  PschRelationsNestingTouch(table, kMaxDepth, &kKeys[0], 1);

  PschRelationsNestingAbortTo(table, kMaxDepth, 2);

  EXPECT_EQ(PschRelationsNestingPop(table, kMaxDepth, &kKeys[0]), cursor_slot);
}

// active_level must reparent down on commit just like created_level does,
// or a later sibling savepoint reusing the same level would sweep it away.
TEST(RelationsNesting, CommitReparentsActiveLevelSoALaterSiblingAbortDoesNotOverReach) {
  PschRelationsNestingEntry table[kMaxDepth] = {};
  int cursor_slot = PschRelationsNestingPush(table, kMaxDepth, &kKeys[0], 1);
  PschRelationsNestingTouch(table, kMaxDepth, &kKeys[0], 2);

  PschRelationsNestingReparentCommitted(table, kMaxDepth, 2);
  PschRelationsNestingPush(table, kMaxDepth, &kKeys[1], 2);
  PschRelationsNestingAbortTo(table, kMaxDepth, 2);

  EXPECT_EQ(PschRelationsNestingPop(table, kMaxDepth, &kKeys[0]), cursor_slot);
  EXPECT_EQ(PschRelationsNestingPop(table, kMaxDepth, &kKeys[1]), -1);
}

TEST(RelationsNesting, TouchReturnsThePreviousActiveLevel) {
  PschRelationsNestingEntry table[kMaxDepth] = {};
  PschRelationsNestingPush(table, kMaxDepth, &kKeys[0], 1);

  EXPECT_EQ(PschRelationsNestingTouch(table, kMaxDepth, &kKeys[0], 2), 1);
  EXPECT_EQ(PschRelationsNestingTouch(table, kMaxDepth, &kKeys[0], 3), 2);
}

// A cursor survives an unrelated savepoint rollback after a successful FETCH
// -- Postgres only fails a portal that was still mid-execution at abort time.
TEST(RelationsNesting, SuccessfulFetchSurvivesAnUnrelatedSavepointRollback) {
  PschRelationsNestingEntry table[kMaxDepth] = {};
  int cursor_slot = PschRelationsNestingPush(table, kMaxDepth, &kKeys[0], 1);

  int prev = PschRelationsNestingTouch(table, kMaxDepth, &kKeys[0], 2);
  PschRelationsNestingSetActiveLevel(table, kMaxDepth, &kKeys[0], prev);  // FETCH succeeded

  PschRelationsNestingAbortTo(table, kMaxDepth, 2);

  EXPECT_EQ(PschRelationsNestingPop(table, kMaxDepth, &kKeys[0]), cursor_slot);
}

TEST(RelationsNesting, SetActiveLevelIsNoOpForAnUnknownKey) {
  PschRelationsNestingEntry table[kMaxDepth] = {};
  int slot = PschRelationsNestingPush(table, kMaxDepth, &kKeys[0], 1);

  PschRelationsNestingSetActiveLevel(table, kMaxDepth, &kKeys[1], 1);

  EXPECT_EQ(PschRelationsNestingPop(table, kMaxDepth, &kKeys[0]), slot);
}

// The leak this guards against: a portal Postgres fails via a path that
// never calls Touch (e.g. FETCH BACKWARD on a NO SCROLL cursor errors before
// ExecutorRun), so the abort sweep never frees it -- the slot must not leak.
TEST(RelationsNesting, PushReclaimsAnAlreadyInUseSlotForTheSameKey) {
  PschRelationsNestingEntry table[kMaxDepth] = {};
  int slot = PschRelationsNestingPush(table, kMaxDepth, &kKeys[0], 1);

  EXPECT_EQ(PschRelationsNestingPush(table, kMaxDepth, &kKeys[0], 2), slot);

  // The reclaim didn't consume a second slot -- every other key still fits.
  for (int i = 1; i < kMaxDepth; i++) {
    EXPECT_GE(PschRelationsNestingPush(table, kMaxDepth, &kKeys[i], 0), 0);
  }
}

TEST(RelationsNesting, PushOnTheSameLeakedKeyNeverExhaustsCapacity) {
  PschRelationsNestingEntry table[kMaxDepth] = {};
  for (int i = 0; i < kMaxDepth * 2; i++) {
    EXPECT_GE(PschRelationsNestingPush(table, kMaxDepth, &kKeys[0], i), 0);
  }
}

TEST(RelationsNesting, PushReclaimDiscardsStaleLevelDataFromTheOrphanedEntry) {
  PschRelationsNestingEntry table[kMaxDepth] = {};
  int slot = PschRelationsNestingPush(table, kMaxDepth, &kKeys[0], 5);
  PschRelationsNestingTouch(table, kMaxDepth, &kKeys[0], 9);

  EXPECT_EQ(PschRelationsNestingPush(table, kMaxDepth, &kKeys[0], 1), slot);

  // A later abort at the orphan's old active_level (9) must not sweep the
  // reclaimed entry -- its level data should reflect only the new reservation.
  PschRelationsNestingAbortTo(table, kMaxDepth, 9);
  EXPECT_EQ(PschRelationsNestingPop(table, kMaxDepth, &kKeys[0]), slot);
}
