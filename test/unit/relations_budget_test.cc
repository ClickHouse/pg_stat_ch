#include "hooks/relations_budget.h"

#include <gtest/gtest.h>

TEST(RelationsWouldExceedBudget, ExactlyAtBudgetFits) {
  EXPECT_FALSE(PschRelationsWouldExceedBudget(0, 10, 10));
  EXPECT_TRUE(PschRelationsWouldExceedBudget(0, 11, 10));
}

TEST(RelationsWouldExceedBudget, NoSeparatorByteCounted) {
  // current_len=5, budget=15: entry of 10 needs exactly 5+10=15, fits -- no
  // extra byte reserved for a separator, unlike the old comma-joined format.
  EXPECT_FALSE(PschRelationsWouldExceedBudget(5, 10, 15));
  EXPECT_TRUE(PschRelationsWouldExceedBudget(5, 11, 15));
}

TEST(RelationsWouldExceedBudget, ZeroBudgetAlwaysExceeds) {
  EXPECT_TRUE(PschRelationsWouldExceedBudget(0, 1, 0));
}
