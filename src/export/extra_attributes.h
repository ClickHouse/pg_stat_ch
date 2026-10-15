#ifndef PG_STAT_CH_SRC_EXPORT_EXTRA_ATTRIBUTES_H_
#define PG_STAT_CH_SRC_EXPORT_EXTRA_ATTRIBUTES_H_

#include <string>
#include <string_view>
#include <utility>
#include <vector>

// Parse "key1:val1;key2:val2" into a flat list. First match wins on
// duplicate keys (Get linear-scans from the front). Empty input -> empty list.
class ExtraAttrs {
 public:
  explicit ExtraAttrs(const char* raw) {
    if (raw == nullptr) {
      return;
    }
    std::string_view input(raw);
    while (!input.empty()) {
      const size_t delim = input.find(';');
      const std::string_view token =
          (delim == std::string_view::npos) ? input : input.substr(0, delim);
      const size_t sep = token.find(':');
      if (sep != std::string_view::npos) {
        attrs_.emplace_back(std::string(token.substr(0, sep)), std::string(token.substr(sep + 1)));
      }
      if (delim == std::string_view::npos) {
        break;
      }
      input.remove_prefix(delim + 1);
    }
  }

  std::string Get(std::string_view key) const {
    for (const auto& [k, v] : attrs_) {
      if (k == key) {
        return v;
      }
    }
    return {};
  }

 private:
  std::vector<std::pair<std::string, std::string>> attrs_;
};

#endif  // PG_STAT_CH_SRC_EXPORT_EXTRA_ATTRIBUTES_H_
