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
