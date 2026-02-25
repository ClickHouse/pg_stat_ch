# Findabsl.cmake - Bridge find_package(absl) to vendored abseil targets.
#
# clickhouse-cpp calls find_package(absl REQUIRED) when WITH_SYSTEM_ABSEIL=ON.
# The vendored gRPC (fetched by opentelemetry-cpp) builds abseil as a
# subdirectory, so there's no abslConfig.cmake on disk. This Module-mode
# find script checks that the required target exists and sets absl_FOUND.

if(TARGET absl::int128)
  set(absl_FOUND TRUE)
else()
  set(absl_FOUND FALSE)
  if(absl_FIND_REQUIRED)
    message(FATAL_ERROR "absl::int128 target not found. "
      "Ensure opentelemetry-cpp (which fetches gRPC + abseil) is added before clickhouse-cpp.")
  endif()
endif()
