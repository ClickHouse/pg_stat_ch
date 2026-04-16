#ifndef PG_STAT_CH_SRC_EXPORT_ARROW_BATCH_H_
#define PG_STAT_CH_SRC_EXPORT_ARROW_BATCH_H_

#include <cstddef>
#include <memory>

#include <arrow/buffer.h>

struct PschEvent;

class ArrowBatchBuilder {
 public:
  ArrowBatchBuilder();
  ~ArrowBatchBuilder();

  ArrowBatchBuilder(ArrowBatchBuilder&&) noexcept;
  ArrowBatchBuilder& operator=(ArrowBatchBuilder&&) noexcept;

  ArrowBatchBuilder(const ArrowBatchBuilder&) = delete;
  ArrowBatchBuilder& operator=(const ArrowBatchBuilder&) = delete;

  bool Init(const char* extra_attrs, const char* service_version);
  bool Append(const PschEvent& event);

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
