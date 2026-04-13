#ifndef PG_STAT_CH_SRC_EXPORT_SQLCOMMENTER_PARSE_H_
#define PG_STAT_CH_SRC_EXPORT_SQLCOMMENTER_PARSE_H_

#include <array>
#include <string>
#include <string_view>

constexpr int kMaxLabels = 16;
constexpr int kMaxKeyLen = 32;
constexpr int kMaxValueLen = 128;

struct Label {
  std::string_view key;    // Points into query buffer or decoded_key
  std::string_view value;  // Points into query buffer or decoded_value
  char decoded_key[kMaxKeyLen];
  char decoded_value[kMaxValueLen];
};

struct ParseResult {
  std::array<Label, kMaxLabels> labels;
  int count = 0;
  bool truncated = false;
};

// Find the last /* ... */ comment in query text.
// Returns the content between delimiters (exclusive), or empty if not found.
std::string_view ExtractLastComment(std::string_view query);

// Parse sqlcommenter format: key1='value1',key2='value2'
// URL-decodes percent-encoded values per sqlcommenter spec.
// Keys truncated to 32 chars, values to 128 chars.
ParseResult ParseSqlcommenter(std::string_view comment);

// Serialize parsed labels to JSON: {"key1":"val1","key2":"val2"}
// Returns "{}" if count == 0.
std::string SerializeLabelsJson(const ParseResult& result);

#endif  // PG_STAT_CH_SRC_EXPORT_SQLCOMMENTER_PARSE_H_
