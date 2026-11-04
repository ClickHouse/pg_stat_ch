#include "export/sqlcommenter_parse.h"

#include <cctype>
#include <charconv>
#include <unordered_map>
#include <utility>
#include <vector>

namespace {

// Tag length limits, in decoded bytes.
constexpr size_t kMaxKeyLen = 65;
constexpr size_t kMaxValueLen = 1024;

struct Label {
  std::string key;    // Decoded
  std::string value;  // Decoded
};

int DecodeHexByte(char hi, char lo) {
  char buf[2] = {hi, lo};
  unsigned value = 0;
  auto [ptr, ec] = std::from_chars(buf, buf + 2, value, 16);
  if (ec != std::errc{} || ptr != buf + 2)
    return -1;
  return static_cast<int>(value);
}

bool IsSpace(char c) {
  return std::isspace(static_cast<unsigned char>(c)) != 0;
}

bool IsKeyTerminator(char c) {
  return c == '=' || c == ',' || IsSpace(c);
}

// [\w\-./:,] in ASCII, the allowed tag characters. None of them needs
// escaping in JSON.
bool IsTagChar(char c) {
  return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' ||
         c == '-' || c == '.' || c == '/' || c == ':' || c == ',';
}

bool IsValidTagField(std::string_view s, size_t max_len) {
  if (s.empty() || s.size() > max_len)
    return false;
  for (char c : s) {
    if (!IsTagChar(c))
      return false;
  }
  return true;
}

// URL-decode %XX escapes; a '%' without two hex digits is kept as is. The
// sqlcommenter meta-unescape (\' -> ') is skipped: neither ' nor \ is a tag
// character, so a field that needs it is dropped either way.
std::string UrlDecode(std::string_view src) {
  std::string decoded;
  decoded.reserve(src.size());
  for (size_t i = 0; i < src.size(); ++i) {
    if (src[i] == '%' && i + 2 < src.size()) {
      int byte = DecodeHexByte(src[i + 1], src[i + 2]);
      if (byte >= 0) {
        decoded.push_back(static_cast<char>(byte));
        i += 2;
        continue;
      }
    }
    decoded.push_back(src[i]);
  }
  return decoded;
}

// Lightweight scanner for walking through a sqlcommenter comment.
struct Scanner {
  std::string_view text;
  size_t pos = 0;

  bool AtEnd() const { return pos >= text.size(); }

  void SkipWhitespace() {
    while (!AtEnd() && IsSpace(text[pos]))
      ++pos;
  }

  // Consume a specific character; returns false without advancing if not matched.
  bool Consume(char expected) {
    if (AtEnd() || text[pos] != expected)
      return false;
    ++pos;
    return true;
  }

  // Scan a key: sequence of non-terminator characters.
  std::string_view ScanKey() {
    size_t start = pos;
    while (!AtEnd() && !IsKeyTerminator(text[pos]))
      ++pos;
    return text.substr(start, pos - start);
  }

  // Scan a single-quoted value, handling \' escapes per sqlcommenter spec.
  // Assumes the opening quote was already consumed.
  // Returns the raw content (before decoding), or a view with data() == nullptr
  // on an unterminated quote.
  std::string_view ScanQuotedValue() {
    size_t start = pos;
    while (!AtEnd()) {
      if (text[pos] == '\\' && pos + 1 < text.size() && text[pos + 1] == '\'') {
        pos += 2;
      } else if (text[pos] == '\'') {
        auto val = text.substr(start, pos - start);
        ++pos;
        return val;
      } else {
        ++pos;
      }
    }
    return {};
  }
};

// Find the /* ... */ comment that ends the query text and return its content
// between the delimiters, or empty if the query does not end with one.
std::string_view ExtractLastComment(std::string_view query) {
  size_t end = query.size();
  while (end > 0 && IsSpace(query[end - 1]))
    --end;
  if (end < 2 || query.substr(end - 2, 2) != "*/")
    return {};
  size_t end_pos = end - 2;

  auto start_pos = query.rfind("/*", end_pos);
  if (start_pos == std::string_view::npos)
    return {};

  // A "--" on the comment's line, before it or inside it, means the final
  // "*/" may sit in a line comment: the block comment is commented out, or its
  // "/*" was inside a quoted identifier or string. A harmless "--" there is
  // rejected too, which only drops labels and never invents them. Like the
  // PostgreSQL scanner, treat both CR and LF as line ends.
  auto line_start = query.find_last_of("\r\n", start_pos);
  line_start = line_start == std::string_view::npos ? 0 : line_start + 1;
  if (query.substr(line_start, end_pos - line_start).find("--") != std::string_view::npos)
    return {};

  size_t content_start = start_pos + 2;
  if (content_start >= end_pos)
    return {};
  std::string_view content = query.substr(content_start, end_pos - content_start);
  // PostgreSQL ends a comment at its first "*/", so one inside means the query
  // really ends in something else, such as a line comment or the outer part
  // of a nested comment.
  if (content.find("*/") != std::string_view::npos)
    return {};
  return content;
}

// Parse sqlcommenter format: key1='value1',key2='value2'. Malformed syntax
// yields no labels; a well-formed pair that breaks the tag rules is dropped.
// A repeated key keeps its first position and takes the last value.
std::vector<Label> ParseSqlcommenter(std::string_view comment) {
  std::vector<Label> labels;
  std::unordered_map<std::string, size_t> index_by_key;
  Scanner scan{comment};

  scan.SkipWhitespace();
  while (!scan.AtEnd()) {
    auto raw_key = scan.ScanKey();
    scan.SkipWhitespace();
    if (raw_key.empty() || !scan.Consume('='))
      return {};
    scan.SkipWhitespace();
    if (!scan.Consume('\''))
      return {};
    auto raw_value = scan.ScanQuotedValue();
    if (raw_value.data() == nullptr)
      return {};

    std::string key = UrlDecode(raw_key);
    std::string value = UrlDecode(raw_value);
    if (IsValidTagField(key, kMaxKeyLen) && IsValidTagField(value, kMaxValueLen)) {
      auto [it, inserted] = index_by_key.try_emplace(key, labels.size());
      if (inserted)
        labels.push_back({std::move(key), std::move(value)});
      else
        labels[it->second].value = std::move(value);
    }

    scan.SkipWhitespace();
    if (scan.AtEnd())
      break;
    if (!scan.Consume(','))
      return {};
    scan.SkipWhitespace();
    if (scan.AtEnd())
      return {};  // trailing comma
  }

  return labels;
}

// Serialize labels to JSON: {"key1":"val1","key2":"val2"}, or "{}" when empty.
std::string SerializeLabelsJson(const std::vector<Label>& labels) {
  std::string out = "{";
  for (const Label& label : labels) {
    if (out.size() > 1)
      out += ',';
    out += '"';
    out += label.key;
    out += "\":\"";
    out += label.value;
    out += '"';
  }
  out += '}';
  return out;
}

}  // namespace

std::string QueryLabelsJson(std::string_view query_text, size_t max_stored_len) {
  // The cut can land right after a comment from the middle of the query,
  // which would then look like it ends the query.
  if (query_text.size() >= max_stored_len)
    return "{}";
  std::string_view comment = ExtractLastComment(query_text);
  if (comment.empty())
    return "{}";
  return SerializeLabelsJson(ParseSqlcommenter(comment));
}
