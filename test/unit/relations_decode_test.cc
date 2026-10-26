#include "export/relations_decode.h"

#include <gtest/gtest.h>

namespace {

// Helper: build a blob byte-string from raw char values so tests read close
// to the wire format rather than fighting C string escaping.
std::string Blob(std::initializer_list<unsigned char> bytes) {
  return std::string(bytes.begin(), bytes.end());
}

}  // namespace

TEST(DecodeRelations, EmptyInputYieldsEmptyResult) {
  auto out = DecodeRelations("", 0);
  EXPECT_FALSE(out.truncated);
  EXPECT_TRUE(out.namespaces.empty());
  EXPECT_TRUE(out.names.empty());
  EXPECT_TRUE(out.kinds.empty());
}

TEST(DecodeRelations, HeaderOnlyNoEntries) {
  auto blob = Blob({0, 0});
  auto out = DecodeRelations(blob.data(), static_cast<uint16_t>(blob.size()));
  EXPECT_FALSE(out.truncated);
  EXPECT_TRUE(out.namespaces.empty());
}

TEST(DecodeRelations, SingleEntry) {
  // truncated=0, count=1, ns="public"(6), name="foo1"(4), kind='r'
  auto blob = Blob({0, 1, 6, 'p', 'u', 'b', 'l', 'i', 'c', 4, 'f', 'o', 'o', '1', 'r'});
  auto out = DecodeRelations(blob.data(), static_cast<uint16_t>(blob.size()));
  EXPECT_FALSE(out.truncated);
  ASSERT_EQ(out.namespaces.size(), 1u);
  EXPECT_EQ(out.namespaces[0], "public");
  EXPECT_EQ(out.names[0], "foo1");
  EXPECT_EQ(out.kinds[0], "r");
}

TEST(DecodeRelations, DotInsideNameSurvivesIntact) {
  // A name containing a literal '.' must not be split -- this is the whole
  // point of moving off the comma/dot-joined text format.
  auto blob = Blob({0, 1, 6, 'p', 'u', 'b', 'l', 'i', 'c', 10, 'w', 'e', 'i', 'r',
                     'd', '.', 'n', 'a', 'm', 'e', 'r'});
  auto out = DecodeRelations(blob.data(), static_cast<uint16_t>(blob.size()));
  ASSERT_EQ(out.names.size(), 1u);
  EXPECT_EQ(out.names[0], "weird.name");
}

TEST(DecodeRelations, CommaInsideNameSurvivesIntact) {
  auto blob = Blob({0, 1, 6, 'p', 'u', 'b', 'l', 'i', 'c', 9, 'h', 'a', 's', ',',
                     'c', 'o', 'm', 'm', 'a', 'r'});
  auto out = DecodeRelations(blob.data(), static_cast<uint16_t>(blob.size()));
  ASSERT_EQ(out.names.size(), 1u);
  EXPECT_EQ(out.names[0], "has,comma");
}

TEST(DecodeRelations, MultipleEntriesAndAllRelkinds) {
  auto blob = Blob({
      0, 3,
      6, 'p', 'u', 'b', 'l', 'i', 'c', 2, 'r', '1', 'r',
      6, 'p', 'u', 'b', 'l', 'i', 'c', 2, 'v', '1', 'v',
      6, 'p', 'u', 'b', 'l', 'i', 'c', 2, 'm', '1', 'm',
  });
  auto out = DecodeRelations(blob.data(), static_cast<uint16_t>(blob.size()));
  ASSERT_EQ(out.names.size(), 3u);
  EXPECT_EQ(out.names[0], "r1");
  EXPECT_EQ(out.kinds[0], "r");
  EXPECT_EQ(out.names[1], "v1");
  EXPECT_EQ(out.kinds[1], "v");
  EXPECT_EQ(out.names[2], "m1");
  EXPECT_EQ(out.kinds[2], "m");
}

TEST(DecodeRelations, TruncatedFlagIsReported) {
  auto blob = Blob({1, 1, 6, 'p', 'u', 'b', 'l', 'i', 'c', 4, 'f', 'o', 'o', '1', 'r'});
  auto out = DecodeRelations(blob.data(), static_cast<uint16_t>(blob.size()));
  EXPECT_TRUE(out.truncated);
}

TEST(DecodeRelations, TruncatedWithZeroCountYieldsEmptyButTruncated) {
  // The blob hooks.c emits when a query never got a nesting slot at all
  // (table full) -- must read as truncated, not as "zero relations".
  auto blob = Blob({1, 0});
  auto out = DecodeRelations(blob.data(), static_cast<uint16_t>(blob.size()));
  EXPECT_TRUE(out.truncated);
  EXPECT_TRUE(out.namespaces.empty());
}

TEST(DecodeRelations, LenShorterThanBufferOnlyReadsLenBytes) {
  auto blob = Blob({0, 1, 6, 'p', 'u', 'b', 'l', 'i', 'c', 4, 'f', 'o', 'o', '1', 'r',
                     'I', 'G', 'N', 'O', 'R', 'E', 'D'});
  auto out = DecodeRelations(blob.data(), 15);  // 15 == real entry length
  ASSERT_EQ(out.names.size(), 1u);
  EXPECT_EQ(out.names[0], "foo1");
}

TEST(DecodeRelations, MalformedLengthDoesNotReadOutOfBounds) {
  // Declares count=1, but ns_len claims 200 bytes when only 3 remain.
  // Must stop cleanly, not read/crash past the buffer.
  auto blob = Blob({0, 1, 200, 'a', 'b', 'c'});
  auto out = DecodeRelations(blob.data(), static_cast<uint16_t>(blob.size()));
  EXPECT_TRUE(out.names.empty());
}

TEST(DecodeRelations, TooShortForHeaderYieldsEmptyResult) {
  auto blob = Blob({0});
  auto out = DecodeRelations(blob.data(), static_cast<uint16_t>(blob.size()));
  EXPECT_TRUE(out.namespaces.empty());
}
