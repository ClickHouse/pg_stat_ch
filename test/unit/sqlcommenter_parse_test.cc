#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "export/sqlcommenter_parse.h"

// ============================================================================
// ExtractLastComment tests
// ============================================================================

TEST(ExtractLastComment, BasicComment) {
  auto result = ExtractLastComment("SELECT 1 /* hello */");
  EXPECT_EQ(result, " hello ");
}

TEST(ExtractLastComment, NoComment) {
  auto result = ExtractLastComment("SELECT 1");
  EXPECT_TRUE(result.empty());
}

TEST(ExtractLastComment, EmptyComment) {
  auto result = ExtractLastComment("SELECT 1 /**/");
  EXPECT_TRUE(result.empty());
}

TEST(ExtractLastComment, MultipleComments) {
  auto result = ExtractLastComment("SELECT /* first */ 1 /* second */");
  EXPECT_EQ(result, " second ");
}

TEST(ExtractLastComment, CommentWithSqlcommenter) {
  auto result = ExtractLastComment(
      "SELECT * FROM users WHERE id = $1 /* controller='users',action='show' */");
  EXPECT_EQ(result, " controller='users',action='show' ");
}

TEST(ExtractLastComment, NoClosingDelimiter) {
  auto result = ExtractLastComment("SELECT 1 /* unclosed");
  EXPECT_TRUE(result.empty());
}

TEST(ExtractLastComment, NoOpeningDelimiter) {
  auto result = ExtractLastComment("SELECT 1 */");
  EXPECT_TRUE(result.empty());
}

TEST(ExtractLastComment, CommentAtStart) {
  auto result = ExtractLastComment("/* comment */ SELECT 1");
  EXPECT_EQ(result, " comment ");
}

TEST(ExtractLastComment, WhitespaceOnlyComment) {
  auto result = ExtractLastComment("SELECT 1 /*   */");
  EXPECT_EQ(result, "   ");
}

TEST(ExtractLastComment, NormalizedQueryWithPlaceholders) {
  auto result = ExtractLastComment(
      "SELECT * FROM users WHERE id = $1 AND name = $2 /* controller='users' */");
  EXPECT_EQ(result, " controller='users' ");
}

TEST(ExtractLastComment, EmptyQuery) {
  auto result = ExtractLastComment("");
  EXPECT_TRUE(result.empty());
}

TEST(ExtractLastComment, AdjacentComments) {
  auto result = ExtractLastComment("SELECT 1 /* a *//* b */");
  EXPECT_EQ(result, " b ");
}

// ============================================================================
// ParseSqlcommenter tests
// ============================================================================

TEST(ParseSqlcommenter, BasicPairs) {
  auto result = ParseSqlcommenter("controller='users',action='show'");
  EXPECT_EQ(result.count, 2);
  EXPECT_EQ(result.labels[0].key, "controller");
  EXPECT_EQ(result.labels[0].value, "users");
  EXPECT_EQ(result.labels[1].key, "action");
  EXPECT_EQ(result.labels[1].value, "show");
  EXPECT_FALSE(result.truncated);
}

TEST(ParseSqlcommenter, SinglePair) {
  auto result = ParseSqlcommenter("key='value'");
  EXPECT_EQ(result.count, 1);
  EXPECT_EQ(result.labels[0].key, "key");
  EXPECT_EQ(result.labels[0].value, "value");
}

TEST(ParseSqlcommenter, WithWhitespace) {
  auto result = ParseSqlcommenter(" controller = 'users' , action = 'show' ");
  EXPECT_EQ(result.count, 2);
  EXPECT_EQ(result.labels[0].key, "controller");
  EXPECT_EQ(result.labels[0].value, "users");
  EXPECT_EQ(result.labels[1].key, "action");
  EXPECT_EQ(result.labels[1].value, "show");
}

TEST(ParseSqlcommenter, EmptyComment) {
  auto result = ParseSqlcommenter("");
  EXPECT_EQ(result.count, 0);
}

TEST(ParseSqlcommenter, WhitespaceOnly) {
  auto result = ParseSqlcommenter("   \t  ");
  EXPECT_EQ(result.count, 0);
}

TEST(ParseSqlcommenter, EmptyValue) {
  auto result = ParseSqlcommenter("key=''");
  EXPECT_EQ(result.count, 1);
  EXPECT_EQ(result.labels[0].key, "key");
  EXPECT_EQ(result.labels[0].value, "");
}

TEST(ParseSqlcommenter, UrlEncodedValue) {
  auto result = ParseSqlcommenter("key='hello%20world'");
  EXPECT_EQ(result.count, 1);
  EXPECT_EQ(result.labels[0].value, "hello world");
}

TEST(ParseSqlcommenter, UrlEncodedComma) {
  auto result = ParseSqlcommenter("key='a%2Cb'");
  EXPECT_EQ(result.count, 1);
  EXPECT_EQ(result.labels[0].value, "a,b");
}

TEST(ParseSqlcommenter, UrlEncodedSingleQuote) {
  auto result = ParseSqlcommenter("key='it%27s'");
  EXPECT_EQ(result.count, 1);
  EXPECT_EQ(result.labels[0].value, "it's");
}

TEST(ParseSqlcommenter, UrlEncodedKey) {
  auto result = ParseSqlcommenter("my%20key='value'");
  EXPECT_EQ(result.count, 1);
  EXPECT_EQ(result.labels[0].key, "my key");
}

TEST(ParseSqlcommenter, MalformedPercentEncoding) {
  // %ZZ is not valid hex; should be passed through literally
  auto result = ParseSqlcommenter("key='%ZZfoo'");
  EXPECT_EQ(result.count, 1);
  EXPECT_EQ(result.labels[0].value, "%ZZfoo");
}

TEST(ParseSqlcommenter, PercentAtEnd) {
  // Trailing % without two hex digits
  auto result = ParseSqlcommenter("key='foo%'");
  EXPECT_EQ(result.count, 1);
  EXPECT_EQ(result.labels[0].value, "foo%");
}

TEST(ParseSqlcommenter, PercentWithOneChar) {
  auto result = ParseSqlcommenter("key='foo%2'");
  EXPECT_EQ(result.count, 1);
  EXPECT_EQ(result.labels[0].value, "foo%2");
}

TEST(ParseSqlcommenter, KeyTruncation) {
  // Key longer than 32 chars should be truncated
  std::string long_key(50, 'a');
  std::string comment = long_key + "='value'";
  auto result = ParseSqlcommenter(comment);
  EXPECT_EQ(result.count, 1);
  EXPECT_EQ(result.labels[0].key.size(), static_cast<size_t>(kMaxKeyLen));
}

TEST(ParseSqlcommenter, ValueTruncation) {
  // Value longer than 128 chars should be truncated
  std::string long_value(200, 'b');
  std::string comment = "key='" + long_value + "'";
  auto result = ParseSqlcommenter(comment);
  EXPECT_EQ(result.count, 1);
  EXPECT_EQ(result.labels[0].value.size(), static_cast<size_t>(kMaxValueLen));
}

TEST(ParseSqlcommenter, MaxLabels) {
  std::string comment;
  for (int i = 0; i < kMaxLabels; ++i) {
    if (i > 0) comment += ',';
    comment += "k" + std::to_string(i) + "='v" + std::to_string(i) + "'";
  }
  auto result = ParseSqlcommenter(comment);
  EXPECT_EQ(result.count, kMaxLabels);
  EXPECT_FALSE(result.truncated);
}

TEST(ParseSqlcommenter, MoreThanMaxLabels) {
  std::string comment;
  for (int i = 0; i < kMaxLabels + 3; ++i) {
    if (i > 0) comment += ',';
    comment += "k" + std::to_string(i) + "='v" + std::to_string(i) + "'";
  }
  auto result = ParseSqlcommenter(comment);
  EXPECT_EQ(result.count, kMaxLabels);
  EXPECT_TRUE(result.truncated);
}

TEST(ParseSqlcommenter, MissingEquals) {
  auto result = ParseSqlcommenter("key'value'");
  EXPECT_EQ(result.count, 0);
}

TEST(ParseSqlcommenter, MissingOpenQuote) {
  auto result = ParseSqlcommenter("key=value'");
  EXPECT_EQ(result.count, 0);
}

TEST(ParseSqlcommenter, MissingCloseQuote) {
  auto result = ParseSqlcommenter("key='value");
  EXPECT_EQ(result.count, 0);
}

TEST(ParseSqlcommenter, MixedValidAndInvalid) {
  auto result = ParseSqlcommenter("good='yes',bad,also_good='yep'");
  EXPECT_EQ(result.count, 2);
  EXPECT_EQ(result.labels[0].key, "good");
  EXPECT_EQ(result.labels[0].value, "yes");
  EXPECT_EQ(result.labels[1].key, "also_good");
  EXPECT_EQ(result.labels[1].value, "yep");
}

TEST(ParseSqlcommenter, TrailingComma) {
  auto result = ParseSqlcommenter("key='value',");
  EXPECT_EQ(result.count, 1);
  EXPECT_EQ(result.labels[0].key, "key");
}

TEST(ParseSqlcommenter, LeadingWhitespace) {
  auto result = ParseSqlcommenter("  key='value'");
  EXPECT_EQ(result.count, 1);
  EXPECT_EQ(result.labels[0].key, "key");
}

TEST(ParseSqlcommenter, RealWorldSqlcommenter) {
  auto result = ParseSqlcommenter(
      "controller='users',action='show',framework='rails',db_driver='pg'");
  EXPECT_EQ(result.count, 4);
  EXPECT_EQ(result.labels[0].key, "controller");
  EXPECT_EQ(result.labels[0].value, "users");
  EXPECT_EQ(result.labels[1].key, "action");
  EXPECT_EQ(result.labels[1].value, "show");
  EXPECT_EQ(result.labels[2].key, "framework");
  EXPECT_EQ(result.labels[2].value, "rails");
  EXPECT_EQ(result.labels[3].key, "db_driver");
  EXPECT_EQ(result.labels[3].value, "pg");
}

// ============================================================================
// Spec compliance: meta character escaping (\' → ')
// Per sqlcommenter spec, single quotes in values are escaped as \'
// ============================================================================

TEST(ParseSqlcommenter, EscapedSingleQuoteInValue) {
  // key='it\'s' → value is "it's"
  auto result = ParseSqlcommenter("key='it\\'s'");
  EXPECT_EQ(result.count, 1);
  EXPECT_EQ(result.labels[0].key, "key");
  EXPECT_EQ(result.labels[0].value, "it's");
}

TEST(ParseSqlcommenter, MultipleEscapedQuotesInValue) {
  // key='can\'t won\'t' → value is "can't won't"
  auto result = ParseSqlcommenter("key='can\\'t won\\'t'");
  EXPECT_EQ(result.count, 1);
  EXPECT_EQ(result.labels[0].value, "can't won't");
}

TEST(ParseSqlcommenter, EscapedQuoteAtValueStart) {
  auto result = ParseSqlcommenter("key='\\'hello'");
  EXPECT_EQ(result.count, 1);
  EXPECT_EQ(result.labels[0].value, "'hello");
}

TEST(ParseSqlcommenter, EscapedQuoteAtValueEnd) {
  auto result = ParseSqlcommenter("key='hello\\''");
  EXPECT_EQ(result.count, 1);
  EXPECT_EQ(result.labels[0].value, "hello'");
}

TEST(ParseSqlcommenter, EscapedQuoteWithUrlEncoding) {
  // Meta unescaping and URL decoding combined
  auto result = ParseSqlcommenter("key='it\\'s%20great'");
  EXPECT_EQ(result.count, 1);
  EXPECT_EQ(result.labels[0].value, "it's great");
}

TEST(ParseSqlcommenter, BackslashNotFollowedByQuote) {
  // Backslash followed by non-quote char is kept literally
  // Input string: key='path\to' (one backslash before 't')
  auto result = ParseSqlcommenter("key='path\\to'");
  EXPECT_EQ(result.count, 1);
  EXPECT_EQ(result.labels[0].value, "path\\to");
}

// ============================================================================
// Spec compliance: spec exhibit round-trip
// ============================================================================

TEST(ParseSqlcommenter, SpecExhibitParsing) {
  // From the sqlcommenter spec exhibit:
  // action='%2Fparam*d',controller='index',framework='spring',
  // traceparent='00-5bd66ef5095369c7b0d1f8f4bd33716a-c532cb4098ac3dd2-01',
  // tracestate='congo%3Dt61rcWkgMzE%2Crojo%3D00f067aa0ba902b7'
  auto result = ParseSqlcommenter(
      "action='%2Fparam*d',"
      "controller='index',"
      "framework='spring',"
      "traceparent='00-5bd66ef5095369c7b0d1f8f4bd33716a-c532cb4098ac3dd2-01',"
      "tracestate='congo%3Dt61rcWkgMzE%2Crojo%3D00f067aa0ba902b7'");
  EXPECT_EQ(result.count, 5);
  EXPECT_EQ(result.labels[0].key, "action");
  EXPECT_EQ(result.labels[0].value, "/param*d");
  EXPECT_EQ(result.labels[1].key, "controller");
  EXPECT_EQ(result.labels[1].value, "index");
  EXPECT_EQ(result.labels[2].key, "framework");
  EXPECT_EQ(result.labels[2].value, "spring");
  EXPECT_EQ(result.labels[3].key, "traceparent");
  EXPECT_EQ(result.labels[3].value,
            "00-5bd66ef5095369c7b0d1f8f4bd33716a-c532cb4098ac3dd2-01");
  EXPECT_EQ(result.labels[4].key, "tracestate");
  EXPECT_EQ(result.labels[4].value, "congo=t61rcWkgMzE,rojo=00f067aa0ba902b7");
}

TEST(ParseSqlcommenter, SpecExhibitFullPipeline) {
  // Full pipeline: extract comment from the spec exhibit SQL, parse, serialize
  std::string_view query =
      "SELECT * FROM FOO "
      "/*action='%2Fparam*d',controller='index',framework='spring',"
      "traceparent='00-5bd66ef5095369c7b0d1f8f4bd33716a-c532cb4098ac3dd2-01',"
      "tracestate='congo%3Dt61rcWkgMzE%2Crojo%3D00f067aa0ba902b7'*/";
  auto comment = ExtractLastComment(query);
  EXPECT_FALSE(comment.empty());
  auto parsed = ParseSqlcommenter(comment);
  EXPECT_EQ(parsed.count, 5);
  EXPECT_EQ(parsed.labels[0].value, "/param*d");
  EXPECT_EQ(parsed.labels[4].value, "congo=t61rcWkgMzE,rojo=00f067aa0ba902b7");
}

TEST(ParseSqlcommenter, UrlEncodedSpecialChars) {
  // Slash, equals, ampersand — common in route and tracestate values
  auto result = ParseSqlcommenter(
      "route='%2Fpolls%201000',state='k1%3Dv1%26k2%3Dv2'");
  EXPECT_EQ(result.count, 2);
  EXPECT_EQ(result.labels[0].key, "route");
  EXPECT_EQ(result.labels[0].value, "/polls 1000");
  EXPECT_EQ(result.labels[1].key, "state");
  EXPECT_EQ(result.labels[1].value, "k1=v1&k2=v2");
}

// ============================================================================
// SerializeLabelsJson tests
// ============================================================================

TEST(SerializeLabelsJson, EmptyResult) {
  ParseResult result;
  EXPECT_EQ(SerializeLabelsJson(result), "{}");
}

TEST(SerializeLabelsJson, SingleLabel) {
  ParseResult result;
  result.count = 1;
  result.labels[0].key = "controller";
  result.labels[0].value = "users";
  EXPECT_EQ(SerializeLabelsJson(result), R"({"controller":"users"})");
}

TEST(SerializeLabelsJson, MultipleLabels) {
  ParseResult result;
  result.count = 2;
  result.labels[0].key = "controller";
  result.labels[0].value = "users";
  result.labels[1].key = "action";
  result.labels[1].value = "show";
  EXPECT_EQ(SerializeLabelsJson(result),
            R"({"controller":"users","action":"show"})");
}

TEST(SerializeLabelsJson, EscapesDoubleQuotes) {
  ParseResult result;
  result.count = 1;
  result.labels[0].key = "key";
  result.labels[0].value = "val\"ue";
  EXPECT_EQ(SerializeLabelsJson(result), R"({"key":"val\"ue"})");
}

TEST(SerializeLabelsJson, EscapesBackslash) {
  ParseResult result;
  result.count = 1;
  result.labels[0].key = "key";
  result.labels[0].value = "path\\to";
  EXPECT_EQ(SerializeLabelsJson(result), R"({"key":"path\\to"})");
}

TEST(SerializeLabelsJson, EscapesControlChars) {
  ParseResult result;
  result.count = 1;
  result.labels[0].key = "key";
  result.labels[0].value = "line1\nline2";
  EXPECT_EQ(SerializeLabelsJson(result), R"({"key":"line1\nline2"})");
}

// ============================================================================
// End-to-end: ExtractLastComment -> ParseSqlcommenter -> SerializeLabelsJson
// ============================================================================

TEST(EndToEnd, FullPipeline) {
  std::string_view query =
      "SELECT * FROM users WHERE id = $1 "
      "/* controller='users',action='show',job='UserSync' */";
  auto comment = ExtractLastComment(query);
  EXPECT_FALSE(comment.empty());

  auto parsed = ParseSqlcommenter(comment);
  EXPECT_EQ(parsed.count, 3);

  std::string json = SerializeLabelsJson(parsed);
  EXPECT_EQ(json,
            R"({"controller":"users","action":"show","job":"UserSync"})");
}

TEST(EndToEnd, QueryWithNoComment) {
  std::string_view query = "SELECT * FROM users WHERE id = $1";
  auto comment = ExtractLastComment(query);
  EXPECT_TRUE(comment.empty());
  // No comment → empty ParseResult → "{}"
  ParseResult result;
  EXPECT_EQ(SerializeLabelsJson(result), "{}");
}

TEST(EndToEnd, UrlEncodedLabels) {
  std::string_view query =
      "SELECT 1 /* app='my%20app',route='/api/users%3Fid%3D1' */";
  auto comment = ExtractLastComment(query);
  auto parsed = ParseSqlcommenter(comment);
  EXPECT_EQ(parsed.count, 2);
  EXPECT_EQ(parsed.labels[0].key, "app");
  EXPECT_EQ(parsed.labels[0].value, "my app");
  EXPECT_EQ(parsed.labels[1].key, "route");
  EXPECT_EQ(parsed.labels[1].value, "/api/users?id=1");
}
