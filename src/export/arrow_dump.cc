#include "export/arrow_dump.h"

#include <chrono>
#include <cstdio>
#include <string>

#include "export/diagnostics.h"

void MaybeDumpArrowBatch(std::string_view dir, const uint8_t* data, size_t len, Diagnostics& diag) {
  if (dir.empty() || data == nullptr || len == 0)
    return;

  const auto unix_now_ns =
      static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                std::chrono::system_clock::now().time_since_epoch())
                                .count());
  const std::string path = std::string(dir) + "/arrow_" + std::to_string(unix_now_ns) + ".ipc";
  const std::string tmp_path = path + ".tmp";

  FILE* file = fopen(tmp_path.c_str(), "wb");
  if (file == nullptr) {
    diag.Warn("failed to open Arrow dump file", tmp_path);
    return;
  }
  const size_t written = fwrite(data, 1, len, file);
  fclose(file);
  if (written != len) {
    remove(tmp_path.c_str());
    diag.Warn("short Arrow dump write", tmp_path);
    return;
  }
  if (rename(tmp_path.c_str(), path.c_str()) != 0) {
    remove(tmp_path.c_str());
    diag.Warn("failed to finalize Arrow dump file", path);
  }
}
