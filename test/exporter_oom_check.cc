#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <new>
#include <string_view>

#include "export/diagnostics.h"

namespace {

thread_local int allocations_before_failure = -1;
thread_local int rejected_allocations = 0;

}  // namespace

void* operator new(size_t size) {
  if (allocations_before_failure == 0) {
    ++rejected_allocations;
    throw std::bad_alloc();
  }
  if (allocations_before_failure > 0)
    --allocations_before_failure;
  if (void* ptr = std::malloc(size == 0 ? 1 : size))
    return ptr;
  throw std::bad_alloc();
}

void* operator new[](size_t size) {
  return ::operator new(size);
}

void operator delete(void* ptr) noexcept {
  std::free(ptr);
}

void operator delete[](void* ptr) noexcept {
  std::free(ptr);
}

void operator delete(void* ptr, size_t) noexcept {
  std::free(ptr);
}

void operator delete[](void* ptr, size_t) noexcept {
  std::free(ptr);
}

extern "C" int CheckExporterOutOfMemory(void) {
  int failures = 0;
  auto check = [&](const char* what, bool ok) {
    if (!ok) {
      ++failures;
      std::fprintf(stderr, "FAIL: %s\n", what);
    }
  };

  char message[PSCH_EXPORTER_TEXT_MAX * 2];
  std::memset(message, 'x', sizeof message);
  PschExporterResult result;
  Diagnostics diag;
  allocations_before_failure = 0;
  diag.Begin(&result);
  diag.Reject("input", std::string_view(message, sizeof message));
  diag.Fail("later failure", "out of memory");
  for (int i = 0; i < PSCH_EXPORTER_WARNINGS_MAX; ++i)
    diag.Warn({}, "warning");
  const auto status = diag.End();
  diag.Fail("unbound", "ignored");
  allocations_before_failure = -1;

  check("diagnostics avoid allocation", rejected_allocations == 0);
  check("first error survives OOM", status == PSCH_EXPORTER_INVALID_ARGUMENT);
  check("diagnostics truncate and terminate",
        std::strlen(result.error) == PSCH_EXPORTER_TEXT_MAX - 1 &&
            std::strncmp(result.error, "input: xxx", 10) == 0);
  check("later failures become warnings",
        std::strcmp(result.warnings[0], "later failure: out of memory") == 0);
  check("warnings stay bounded",
        result.warning_count == PSCH_EXPORTER_WARNINGS_MAX + 1 &&
            std::strcmp(result.warnings[PSCH_EXPORTER_WARNINGS_MAX - 1], "warning") == 0);

  allocations_before_failure = 0;
  diag.Begin(&result);
  diag.Fail("empty message", {});
  diag.End();
  allocations_before_failure = -1;
  check("empty views need no backing storage", std::strcmp(result.error, "empty message: ") == 0);

  PschExporterConfig config{};
  config.mode = PSCH_EXPORTER_CLICKHOUSE;
  config.service_version = "version long enough to require a string allocation";
  for (int i = 0; i < 2; ++i) {
    PschExporter* exporter = nullptr;
    rejected_allocations = 0;
    allocations_before_failure = i;
    const auto created = PschExporterCreate(&config, &exporter, &result);
    allocations_before_failure = -1;
    check("create reports OOM without another allocation",
          created == PSCH_EXPORTER_FAILED && result.status == created && exporter == nullptr &&
              rejected_allocations == 1 &&
              std::strcmp(result.error, "create exporter: out of memory") == 0);
    PschExporterDestroy(exporter);
  }
  return failures;
}
