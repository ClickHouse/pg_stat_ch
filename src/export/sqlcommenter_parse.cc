#include "export/sqlcommenter_parse.h"

#include <cstdio>

namespace {

int DecodeHexByte(char hi, char lo) {
  auto hex = [](char c) -> int {
    if (c >= '0' && c <= '9')
      return c - '0';
    if (c >= 'a' && c <= 'f')
      return c - 'a' + 10;
    if (c >= 'A' && c <= 'F')
      return c - 'A' + 10;
    return -1;
  };
  int h = hex(hi);
  int l = hex(lo);
  if (h < 0 || l < 0)
    return -1;
  return (h << 4) | l;
}

bool NeedsDecode(std::string_view sv) {
  return sv.find('%') != std::string_view::npos || sv.find('\\') != std::string_view::npos;
}

// Meta-unescape (\' → ') and URL-decode in a single pass.
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
  size_t pos = 0;

  auto skip_ws = [&]() {
    while (pos < comment.size() && (comment[pos] == ' ' || comment[pos] == '\t' ||
                                    comment[pos] == '\n' || comment[pos] == '\r')) {
      ++pos;
    }
  };

  while (pos < comment.size() && result.count < kMaxLabels) {
    skip_ws();
    if (pos >= comment.size())
      break;

    // Parse key: read until '=', whitespace, or ','
    size_t key_start = pos;
    while (pos < comment.size() && comment[pos] != '=' && comment[pos] != ' ' &&
           comment[pos] != '\t' && comment[pos] != '\n' && comment[pos] != '\r' &&
           comment[pos] != ',') {
      ++pos;
    }
    if (pos == key_start) {
      ++pos;
      continue;
    }
    std::string_view raw_key = comment.substr(key_start, pos - key_start);

    skip_ws();
    if (pos >= comment.size() || comment[pos] != '=')
      continue;
    ++pos;

    skip_ws();
    if (pos >= comment.size() || comment[pos] != '\'')
      continue;
    ++pos;

    // Parse value: read until unescaped closing single quote.
    // Per sqlcommenter spec, \' is an escaped quote inside the value.
    size_t val_start = pos;
    while (pos < comment.size()) {
      if (comment[pos] == '\\' && pos + 1 < comment.size() && comment[pos + 1] == '\'') {
        pos += 2;  // skip escaped quote
      } else if (comment[pos] == '\'') {
        break;
      } else {
        ++pos;
      }
    }
    if (pos >= comment.size())
      break;
    std::string_view raw_value = comment.substr(val_start, pos - val_start);
    ++pos;

    Label& label = result.labels[result.count];
    if (NeedsDecode(raw_key)) {
      size_t len = MetaUnescapeAndUrlDecode(raw_key, label.decoded_key, kMaxKeyLen);
      label.key = std::string_view(label.decoded_key, len);
    } else {
      label.key = raw_key.substr(0, kMaxKeyLen);
    }

    if (NeedsDecode(raw_value)) {
      size_t len = MetaUnescapeAndUrlDecode(raw_value, label.decoded_value, kMaxValueLen);
      label.value = std::string_view(label.decoded_value, len);
    } else {
      label.value = raw_value.substr(0, kMaxValueLen);
    }

    ++result.count;

    skip_ws();
    if (pos < comment.size() && comment[pos] == ',')
      ++pos;
  }

  if (result.count == kMaxLabels) {
    skip_ws();
    if (pos < comment.size())
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
