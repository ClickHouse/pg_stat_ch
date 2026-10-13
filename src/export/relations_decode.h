// Pure C++, no PostgreSQL headers -- kept separate from stats_exporter.cc so
// it's unit-testable in isolation (see test/unit/relations_decode_test.cc).
#ifndef PG_STAT_CH_SRC_EXPORT_RELATIONS_DECODE_H_
#define PG_STAT_CH_SRC_EXPORT_RELATIONS_DECODE_H_

#include <cstdint>
#include <string_view>
#include <vector>

// Decoded form of the relations blob built by CollectRelations in hooks.c:
// [truncated:u8][count:u8]{[ns_len:u8][ns][name_len:u8][name][relkind:u8]}*count
struct DecodedRelations {
  bool truncated = false;
  std::vector<std::string_view> namespaces;
  std::vector<std::string_view> names;
  std::vector<std::string_view> kinds;  // one-byte spans, e.g. "r", "v", "m"
};

// Decodes buf[0..len). Malformed input (len < 2, or an entry's declared
// length running past the end of buf) stops decoding at that point rather
// than reading out of bounds; entries decoded so far are still returned.
inline DecodedRelations DecodeRelations(const char* buf, uint16_t len) {
  DecodedRelations out;
  if (len < 2) {
    return out;
  }

  out.truncated = buf[0] != 0;
  auto count = static_cast<uint8_t>(buf[1]);
  size_t pos = 2;
  const size_t end = len;

  // Reads a [len:u8][bytes] field at pos, advancing it past the field.
  // Returns false (leaving *field untouched) if len or bytes run past end.
  auto read_field = [&](std::string_view* field) {
    if (pos + 1 > end) {
      return false;
    }
    auto field_len = static_cast<uint8_t>(buf[pos]);
    if (pos + 1 + field_len > end) {
      return false;
    }
    *field = std::string_view(buf + pos + 1, field_len);
    pos += 1 + field_len;
    return true;
  };

  for (uint8_t i = 0; i < count; i++) {
    std::string_view ns;
    std::string_view name;
    if (!read_field(&ns) || !read_field(&name) || pos + 1 > end) {
      break;
    }
    std::string_view kind(buf + pos, 1);
    pos += 1;

    out.namespaces.push_back(ns);
    out.names.push_back(name);
    out.kinds.push_back(kind);
  }

  return out;
}

#endif  // PG_STAT_CH_SRC_EXPORT_RELATIONS_DECODE_H_
