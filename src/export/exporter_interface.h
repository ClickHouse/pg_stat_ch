#ifndef PG_STAT_CH_SRC_EXPORT_EXPORTER_INTERFACE_H_
#define PG_STAT_CH_SRC_EXPORT_EXPORTER_INTERFACE_H_

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

class StatsExporter {
 protected:
  using string = std::string;
  using string_view = std::string_view;
  template<typename T> using shared_ptr = std::shared_ptr<T>;

  class BasicColumn {
   public:
    virtual void Crunch() = 0;  // Implementation-defined processing helper.
    virtual ~BasicColumn() = 0;
  };
  template<typename T> class Column : public BasicColumn {
   public:
    virtual void Append(const T &t) = 0;
    virtual ~Column() = 0;
  };

 public:
  // Tags: Columns that serve as narrowing criteria for metrics.
  virtual shared_ptr<Column<string>> TagString(string_view name) = 0;

  // Metrics: Columns that are generally bucketed into histograms.
  virtual shared_ptr<Column<int16_t>> MetricInt16(string_view name) = 0;
  virtual shared_ptr<Column<int32_t>> MetricInt32(string_view name) = 0;
  virtual shared_ptr<Column<int64_t>> MetricInt64(string_view name) = 0;
  virtual shared_ptr<Column<uint8_t>> MetricUInt8(string_view name) = 0;
  virtual shared_ptr<Column<uint64_t>> MetricUInt64(string_view name) = 0;
  virtual shared_ptr<Column<string_view>> MetricFixedString(int len, string_view name) = 0;

  // Records: Data columns you wouldn't want to filter by.
  virtual shared_ptr<Column<int32_t>> RecordInt32(string_view name) = 0;
  virtual shared_ptr<Column<int64_t>> RecordInt64(string_view name) = 0;
  virtual shared_ptr<Column<int64_t>> RecordDateTime(string_view name) = 0;
  virtual shared_ptr<Column<string_view>> RecordString(string_view name) = 0;

  virtual void BeginBatch() = 0;
  virtual void BeginRow() = 0;
  virtual bool CommitBatch() = 0;

  virtual bool EstablishNewConnection() = 0;
  virtual bool IsConnected() const = 0;
  virtual int NumConsecutiveFailures() const = 0;
  virtual void ResetFailures() = 0;
  virtual int NumExported() const = 0;

  virtual ~StatsExporter() = 0;
};

inline StatsExporter::BasicColumn::~BasicColumn() = default;
template<typename T> StatsExporter::Column<T>::~Column() = default;

// Allows PG logging of exceptional cases without postgres.h
void LogNegativeValue(const std::string &column_name, int64_t value);

// Expected usage:
// void ProcessBatch(StatsExporter *exporter) {
//   exporter->BeginBatch(); // no op or ClickHouse column reset
//   auto *col_user = TagString("username");
//   auto *col_rows = AddInt64Column("rows");
// 
//   for (const auto &ev : events) {
//     exporter->BeginRow(); // no-op or initialize tag map
//     col_user->Append(static_cast<int64_t>(ev.queryid));
//     col_rows->Append(ev.rows);
//   }
// 
//   exporter->CommitBatch();  // Inserts or collects stats
// } 

#endif  // PG_STAT_CH_SRC_EXPORT_EXPORTER_INTERFACE_H_
