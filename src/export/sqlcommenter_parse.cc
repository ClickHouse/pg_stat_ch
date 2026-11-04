#include "export/sqlcommenter_parse.h"

#include <charconv>

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

// Length of the valid UTF-8 sequence starting at s[i], or 0 if it is invalid
// (bad lead byte, missing continuation, overlong form, surrogate, > U+10FFFF).
size_t Utf8SequenceLen(std::string_view s, size_t i) {
  auto b = [&](size_t k) { return static_cast<unsigned char>(s[k]); };
  unsigned char c = b(i);
  size_t len;
  unsigned char lo = 0x80, hi = 0xBF;  // allowed range of the second byte
  if (c < 0x80) {
    return 1;
  } else if (c >= 0xC2 && c <= 0xDF) {
    len = 2;
  } else if (c >= 0xE0 && c <= 0xEF) {
    len = 3;
    if (c == 0xE0)
      lo = 0xA0;
    if (c == 0xED)
      hi = 0x9F;
  } else if (c >= 0xF0 && c <= 0xF4) {
    len = 4;
    if (c == 0xF0)
      lo = 0x90;
    if (c == 0xF4)
      hi = 0x8F;
  } else {
    return 0;
  }
  if (i + len > s.size() || b(i + 1) < lo || b(i + 1) > hi)
    return 0;
  for (size_t k = 2; k < len; ++k) {
    if (b(i + k) < 0x80 || b(i + k) > 0xBF)
      return 0;
  }
  return len;
}

// Append s as a quoted JSON string. URL decoding can produce arbitrary bytes,
// so invalid UTF-8 bytes become U+FFFD to keep the output valid JSON.
void AppendJsonString(std::string_view s, std::string* out) {
  static constexpr char kHex[] = "0123456789abcdef";
  out->push_back('"');
  for (size_t i = 0; i < s.size();) {
    unsigned char c = static_cast<unsigned char>(s[i]);
    size_t len = Utf8SequenceLen(s, i);
    if (len == 0) {
      out->append("\xEF\xBF\xBD");
      ++i;
      continue;
    }
    if (len > 1) {
      out->append(s.substr(i, len));
    } else if (c == '"' || c == '\\') {
      out->push_back('\\');
      out->push_back(static_cast<char>(c));
    } else if (c == '\b') {
      out->append("\\b");
    } else if (c == '\f') {
      out->append("\\f");
    } else if (c == '\n') {
      out->append("\\n");
    } else if (c == '\r') {
      out->append("\\r");
    } else if (c == '\t') {
      out->append("\\t");
    } else if (c < 0x20) {
      out->append("\\u00");
      out->push_back(kHex[c >> 4]);
      out->push_back(kHex[c & 0xF]);
    } else {
      out->push_back(static_cast<char>(c));
    }
    i += len;
  }
  out->push_back('"');
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
  std::string out = "{";
  for (int i = 0; i < result.count; ++i) {
    std::string_view key = result.labels[i].key;
    // Duplicate keys keep the first position and the last value.
    bool seen = false;
    for (int j = 0; j < i && !seen; ++j)
      seen = result.labels[j].key == key;
    if (seen)
      continue;
    std::string_view value = result.labels[i].value;
    for (int j = i + 1; j < result.count; ++j) {
      if (result.labels[j].key == key)
        value = result.labels[j].value;
    }
    if (out.size() > 1)
      out += ',';
    AppendJsonString(key, &out);
    out += ':';
    AppendJsonString(value, &out);
  }
  out += '}';
  return out;
}
