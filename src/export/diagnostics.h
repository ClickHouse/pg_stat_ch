// Bounded diagnostics for one library call, written directly into caller storage
#ifndef PG_STAT_CH_SRC_EXPORT_DIAGNOSTICS_H_
#define PG_STAT_CH_SRC_EXPORT_DIAGNOSTICS_H_

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <string_view>

#include "export/psch_exporter.h"

class Diagnostics {
 public:
  // Reset result to failure defaults and bind it until End
  void Begin(PschExporterResult* result) noexcept {
    result_ = result;
    *result = PschExporterResult{};
    result->status = PSCH_EXPORTER_FAILED;
  }

  // Derive final status and unbind. Calls made after this are dropped
  PschExporterStatus End() noexcept {
    PschExporterResult* r = result_;
    result_ = nullptr;
    r->status = r->error[0] != '\0' ? r->status : PSCH_EXPORTER_OK;
    return r->status;
  }

  // First failure becomes error text, later ones are retained as warnings
  void Fail(std::string_view context, std::string_view message) noexcept {
    Record(PSCH_EXPORTER_FAILED, context, message);
  }
  void Reject(std::string_view context, std::string_view message) noexcept {
    Record(PSCH_EXPORTER_INVALID_ARGUMENT, context, message);
  }
  void Warn(std::string_view context, std::string_view message) noexcept {
    if (result_ == nullptr)
      return;
    const uint32_t n = result_->warning_count++;
    if (n < PSCH_EXPORTER_WARNINGS_MAX)
      Format(result_->warnings[n], context, message);
  }
  void AddExported(uint32_t n) noexcept {
    if (result_ != nullptr)
      result_->exported += n;
  }
  bool Failed() const noexcept { return result_ != nullptr && result_->error[0] != '\0'; }

 private:
  void Record(PschExporterStatus status, std::string_view context,
              std::string_view message) noexcept {
    if (result_ == nullptr)
      return;
    if (result_->error[0] != '\0') {
      Warn(context, message);
      return;
    }
    result_->status = status;
    Format(result_->error, context, message);
  }

  static void Format(char (&buf)[PSCH_EXPORTER_TEXT_MAX], std::string_view context,
                     std::string_view message) noexcept {
    size_t used = 0;
    auto append = [&](std::string_view text) {
      const size_t n = std::min(text.size(), sizeof buf - 1 - used);
      if (n > 0) {
        memcpy(buf + used, text.data(), n);
        used += n;
      }
    };
    if (!context.empty()) {
      append(context);
      append(": ");
    }
    append(message);
    buf[used] = '\0';
  }

  PschExporterResult* result_ = nullptr;
};

#endif  // PG_STAT_CH_SRC_EXPORT_DIAGNOSTICS_H_
