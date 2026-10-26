// Builds an array-valued OTLP attribute for any HC string-array stat.
// Separate from otel_exporter.cc so it's unit-testable without a live
// gRPC channel (test/unit/otel_string_array_attr_test.cc).
#ifndef PG_STAT_CH_SRC_EXPORT_OTEL_STRING_ARRAY_ATTR_H_
#define PG_STAT_CH_SRC_EXPORT_OTEL_STRING_ARRAY_ATTR_H_

#include <opentelemetry/proto/common/v1/common.pb.h>

#include <cstddef>
#include <string_view>
#include <vector>

namespace psch {

// AnyValue.array_value is a plain repeated AnyValue -- every element gets
// its own string_value, same as a top-level string attribute would.
inline void SetStringArrayAttr(opentelemetry::proto::common::v1::KeyValue* kv,
                                std::string_view key,
                                const std::vector<std::string_view>& values) {
  kv->set_key(key.data(), key.size());
  auto* arr = kv->mutable_value()->mutable_array_value();
  for (const auto& value : values) {
    arr->add_values()->set_string_value(value.data(), value.size());
  }
}

// Used to decide when a chunk is full. Unlike a scalar attribute, each
// array element is its own nested AnyValue, with its own protobuf framing
// on top of the string bytes -- kPerElementOverheadBytes covers that.
inline size_t EstimateStringArrayAttrBytes(std::string_view key,
                                            const std::vector<std::string_view>& values) {
  constexpr size_t kAttrOverheadBytes = 24;
  constexpr size_t kPerElementOverheadBytes = 4;
  size_t total = kAttrOverheadBytes + key.size();
  for (const auto& value : values) {
    total += value.size() + kPerElementOverheadBytes;
  }
  return total;
}

}  // namespace psch

#endif  // PG_STAT_CH_SRC_EXPORT_OTEL_STRING_ARRAY_ATTR_H_
