#include "export/sqlcommenter_parse.h"

#include <charconv>
#include <cstdio>

namespace {

int DecodeHexByte(char hi, char lo) {
  char buf[2] = {hi, lo};
  unsigned value = 0;
  auto [ptr, ec] = std::from_chars(buf, buf + 2, value, 16);
  if (ec != std::errc{} || ptr != buf + 2)
    return -1;
  return static_cast<int>(value);
}

bool IsWhitespace(char c) {
  return c == ' ' || c == '\t' || c == '\n' || c == '\r';
}

bool IsKeyTerminator(char c) {
  return c == '=' || c == ',' || IsWhitespace(c);
}

bool NeedsDecode(std::string_view sv) {
  return sv.find('%') != std::string_view::npos || sv.find('\\') != std::string_view::npos;
}

// Meta-unescape (\' -> ') and URL-decode (%XX) in a single pass.
// Per sqlcommenter spec, meta unescaping happens before URL decoding,
// but the two transforms don't overlap so a combined pass is equivalent.
size_t MetaUnescapeAndUrlDecode(std::string_view src, char* dst, size_t max_len) {
  size_t written = 0;
  size_t i = 0;
  while (i < src.size() && written < max_len) {
    if (src[i] == '\\' && i + 1 < src.size() && src[i + 1] == '\'') {
      dst[written++] = '\'';
      i += 2;
    } else if (src[i] == '%' && i + 2 < src.size()) {
      int byte = DecodeHexByte(src[i + 1], src[i + 2]);
      if (byte >= 0) {
        dst[written++] = static_cast<char>(byte);
        i += 3;
        continue;
      }
      dst[written++] = src[i++];
    } else {
      dst[written++] = src[i++];
    }
  }
  return written;
}

// Decode a raw field (meta-unescape + URL-decode) into buf, or truncate if no decoding needed.
std::string_view DecodeField(std::string_view raw, char* buf, size_t max_len) {
  if (NeedsDecode(raw)) {
    size_t len = MetaUnescapeAndUrlDecode(raw, buf, max_len);
    return {buf, len};
  }
  return raw.substr(0, max_len);
}

void AppendJsonEscaped(std::string& out, std::string_view sv) {
  for (char c : sv) {
    switch (c) {
      case '"':
        out += "\\\"";
        break;
      case '\\':
        out += "\\\\";
        break;
      case '\b':
        out += "\\b";
        break;
      case '\f':
        out += "\\f";
        break;
      case '\n':
        out += "\\n";
        break;
      case '\r':
        out += "\\r";
        break;
      case '\t':
        out += "\\t";
        break;
      default:
        if (static_cast<unsigned char>(c) < 0x20) {
          char buf[8];
          std::snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned char>(c));
          out += buf;
        } else {
          out += c;
        }
    }
  }
}

// Lightweight scanner for walking through a sqlcommenter comment.
struct Scanner {
  std::string_view text;
  size_t pos = 0;

  bool AtEnd() const { return pos >= text.size(); }

  void SkipWhitespace() {
    while (!AtEnd() && IsWhitespace(text[pos]))
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
  // Returns the raw content (before decoding). Sets *ok = false on unterminated quote.
  std::string_view ScanQuotedValue(bool* ok) {
    size_t start = pos;
    while (!AtEnd()) {
      if (text[pos] == '\\' && pos + 1 < text.size() && text[pos + 1] == '\'') {
        pos += 2;
      } else if (text[pos] == '\'') {
        auto val = text.substr(start, pos - start);
        ++pos;
        *ok = true;
        return val;
      } else {
        ++pos;
      }
    }
    *ok = false;
    return {};
  }
};

}  // namespace

std::string_view ExtractLastComment(std::string_view query) {
  auto end_pos = query.rfind("*/");
  if (end_pos == std::string_view::npos)
    return {};

  auto start_pos = query.rfind("/*", end_pos);
  if (start_pos == std::string_view::npos)
    return {};

  size_t content_start = start_pos + 2;
  if (content_start >= end_pos)
    return {};
  return query.substr(content_start, end_pos - content_start);
}

ParseResult ParseSqlcommenter(std::string_view comment) {
  ParseResult result;
  Scanner scan{comment};

  while (!scan.AtEnd() && result.count < kMaxLabels) {
    scan.SkipWhitespace();

    auto raw_key = scan.ScanKey();
    if (raw_key.empty()) {
      if (!scan.AtEnd())
        ++scan.pos;
      continue;
    }

    scan.SkipWhitespace();
    if (!scan.Consume('='))
      continue;
    scan.SkipWhitespace();
    if (!scan.Consume('\''))
      continue;

    bool ok = false;
    auto raw_value = scan.ScanQuotedValue(&ok);
    if (!ok)
      break;

    Label& label = result.labels[result.count++];
    label.key = DecodeField(raw_key, label.decoded_key, kMaxKeyLen);
    label.value = DecodeField(raw_value, label.decoded_value, kMaxValueLen);

    scan.SkipWhitespace();
    scan.Consume(',');
  }

  if (result.count == kMaxLabels) {
    scan.SkipWhitespace();
    if (!scan.AtEnd())
      result.truncated = true;
  }

  return result;
}

std::string SerializeLabelsJson(const ParseResult& result) {
  if (result.count == 0)
    return "{}";

  std::string json = "{";
  for (int i = 0; i < result.count; ++i) {
    if (i > 0)
      json += ',';
    json += '"';
    AppendJsonEscaped(json, result.labels[i].key);
    json += "\":\"";
    AppendJsonEscaped(json, result.labels[i].value);
    json += '"';
  }
  json += '}';
  return json;
}
