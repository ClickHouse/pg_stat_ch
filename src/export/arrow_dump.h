// Debug capture of Arrow IPC payloads
#ifndef PG_STAT_CH_SRC_EXPORT_ARROW_DUMP_H_
#define PG_STAT_CH_SRC_EXPORT_ARROW_DUMP_H_

#include <cstddef>
#include <cstdint>
#include <string_view>

class Diagnostics;

// Write payload to dir as arrow_<unix_ns>.ipc through a renamed temp file.
// Empty dir disables capture. Failures are warnings, export continues
void MaybeDumpArrowBatch(std::string_view dir, const uint8_t* data, size_t len, Diagnostics& diag);

#endif  // PG_STAT_CH_SRC_EXPORT_ARROW_DUMP_H_
