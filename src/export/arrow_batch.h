#ifndef PG_STAT_CH_SRC_EXPORT_ARROW_BATCH_H_
#define PG_STAT_CH_SRC_EXPORT_ARROW_BATCH_H_

#include <cstddef>
#include <memory>

#include <arrow/buffer.h>
#include <arrow/result.h>

namespace arrow {
class RecordBatch;
}

class Diagnostics;
struct PschExportEvent;

arrow::Result<std::shared_ptr<arrow::Buffer>> SerializeArrowBatch(const arrow::RecordBatch& batch);

// query_logs_arrow Arrow IPC builder. Failures are reported through diag
// and return false, negative values clamped to zero raise warnings
class ArrowBatchBuilder {
 public:
  explicit ArrowBatchBuilder(Diagnostics* diag);
  ~ArrowBatchBuilder();

  ArrowBatchBuilder(ArrowBatchBuilder&&) noexcept;
  ArrowBatchBuilder& operator=(ArrowBatchBuilder&&) noexcept;

  ArrowBatchBuilder(const ArrowBatchBuilder&) = delete;
  ArrowBatchBuilder& operator=(const ArrowBatchBuilder&) = delete;

  void Init(const char* extra_attrs, const char* service_version);
  bool Append(const PschExportEvent& event);

  struct FinishResult {
    std::shared_ptr<arrow::Buffer> ipc_buffer;
    int num_rows = 0;
  };

  FinishResult Finish();
  void Reset();

  int NumRows() const;
  size_t EstimatedBytes() const;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

#endif  // PG_STAT_CH_SRC_EXPORT_ARROW_BATCH_H_
