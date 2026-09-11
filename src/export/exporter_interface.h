#ifndef PG_STAT_CH_SRC_EXPORT_EXPORTER_INTERFACE_H_
#define PG_STAT_CH_SRC_EXPORT_EXPORTER_INTERFACE_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

class Diagnostics;
struct ExporterConfig;

// Backend for one destination. Reads settings through config_, reports
// failures and accepted rows through diag_, both owned by the enclosing
// PschExporter handle
class StatsExporter {
 protected:
  StatsExporter(const ExporterConfig* config, Diagnostics* diag) : config_(config), diag_(diag) {}

  const ExporterConfig* const config_;
  Diagnostics* const diag_;

  using string = std::string;
  using string_view = std::string_view;
  template <typename T>
  using shared_ptr = std::shared_ptr<T>;

  template <typename T>
  class Column {
   public:
    virtual void Append(const T& t) = 0;
    virtual ~Column() = default;
  };

 public:
  // Arrow encodes LC strings as dictionaries, ClickHouse follows destination schema
  virtual shared_ptr<Column<string>> StatLCString(string_view name) = 0;
  virtual shared_ptr<Column<uint8_t>> StatLCUInt8(string_view name) = 0;
  virtual shared_ptr<Column<int16_t>> StatLCInt16(string_view name) = 0;
  virtual shared_ptr<Column<int32_t>> StatLCInt32(string_view name) = 0;

  virtual shared_ptr<Column<string_view>> StatHCString(string_view name) = 0;
  virtual shared_ptr<Column<int64_t>> StatHCInt64(string_view name) = 0;
  virtual shared_ptr<Column<uint64_t>> StatHCUInt64(string_view name) = 0;

  // Accept Unix microseconds
  virtual shared_ptr<Column<int64_t>> StatTimestamp(string_view name) = 0;

  // Map database fields to destination names and units
  virtual shared_ptr<Column<string>> DbNameColumn() = 0;
  virtual shared_ptr<Column<string>> DbUserColumn() = 0;
  // Accept microseconds
  virtual shared_ptr<Column<uint64_t>> DbDurationColumn() = 0;
  virtual shared_ptr<Column<string>> DbOperationColumn() = 0;
  virtual shared_ptr<Column<string_view>> DbQueryTextColumn() = 0;

  virtual void BeginBatch() = 0;
  virtual void BeginRow() = 0;
  virtual bool CommitBatch() = 0;

  // Failures are reported through diag_ and return false
  virtual bool EstablishNewConnection() = 0;
  virtual bool IsConnected() const noexcept = 0;
  // Set pg_stat_ch.block_format for collector routing
  virtual bool SendArrowBatch(const uint8_t* ipc_data, size_t ipc_len, int num_rows,
                              string_view block_format) {
    (void)ipc_data;
    (void)ipc_len;
    (void)num_rows;
    (void)block_format;
    return false;
  }

  virtual ~StatsExporter() = default;
};

#endif  // PG_STAT_CH_SRC_EXPORT_EXPORTER_INTERFACE_H_
