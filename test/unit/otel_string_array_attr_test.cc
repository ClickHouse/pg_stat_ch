#include "export/otel_string_array_attr.h"

#include <gtest/gtest.h>

using opentelemetry::proto::common::v1::KeyValue;

TEST(SetStringArrayAttr, KeyIsSet) {
  KeyValue kv;
  psch::SetStringArrayAttr(&kv, "relation_names", {});
  EXPECT_EQ(kv.key(), "relation_names");
}

TEST(SetStringArrayAttr, EmptyInputYieldsEmptyArrayNotAbsentAttr) {
  KeyValue kv;
  psch::SetStringArrayAttr(&kv, "relation_names", {});
  ASSERT_TRUE(kv.value().has_array_value());
  EXPECT_EQ(kv.value().array_value().values_size(), 0);
}

TEST(SetStringArrayAttr, SingleEntry) {
  KeyValue kv;
  psch::SetStringArrayAttr(&kv, "relation_names", {"public.foo1"});
  ASSERT_EQ(kv.value().array_value().values_size(), 1);
  EXPECT_EQ(kv.value().array_value().values(0).string_value(), "public.foo1");
}

TEST(SetStringArrayAttr, MultipleEntriesPreserveOrder) {
  KeyValue kv;
  psch::SetStringArrayAttr(&kv, "relation_names", {"public.foo1", "public.foo2", "sch.foo3"});
  const auto& arr = kv.value().array_value();
  ASSERT_EQ(arr.values_size(), 3);
  EXPECT_EQ(arr.values(0).string_value(), "public.foo1");
  EXPECT_EQ(arr.values(1).string_value(), "public.foo2");
  EXPECT_EQ(arr.values(2).string_value(), "sch.foo3");
}

TEST(SetStringArrayAttr, SingleCharElementSurvives) {
  KeyValue kv;
  psch::SetStringArrayAttr(&kv, "relation_kinds", {"r"});
  EXPECT_EQ(kv.value().array_value().values(0).string_value(), "r");
}

TEST(EstimateStringArrayAttrBytes, GrowsWithElementCountAndLength) {
  size_t empty = psch::EstimateStringArrayAttrBytes("relation_names", {});
  size_t one_short = psch::EstimateStringArrayAttrBytes("relation_names", {"a.b"});
  size_t one_long =
      psch::EstimateStringArrayAttrBytes("relation_names", {"public.a_much_longer_name"});
  size_t two = psch::EstimateStringArrayAttrBytes("relation_names", {"a.b", "c.d"});

  EXPECT_GT(one_short, empty);
  EXPECT_GT(one_long, one_short);
  EXPECT_GT(two, one_short);
}

TEST(EstimateStringArrayAttrBytes, CountsPerElementFramingOverhead) {
  size_t zero = psch::EstimateStringArrayAttrBytes("k", {});
  size_t one = psch::EstimateStringArrayAttrBytes("k", {"abc"});
  // Each array element is its own nested AnyValue -- adding one should cost
  // more than just its own string bytes.
  EXPECT_GT(one - zero, 3u);
}
