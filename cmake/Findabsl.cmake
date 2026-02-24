# Findabsl.cmake - Bridge find_package(absl) to targets already built
# by gRPC's bundled abseil (via opentelemetry-cpp FetchContent).
#
# clickhouse-cpp calls find_package(absl REQUIRED) when WITH_SYSTEM_ABSEIL=ON.
# Since abseil was built as a subdirectory (not installed), there's no
# abslConfig.cmake on disk. This Module-mode find script checks that the
# required targets exist and sets absl_FOUND.

if(TARGET absl::int128)
  set(absl_FOUND TRUE)
else()
  set(absl_FOUND FALSE)
  if(absl_FIND_REQUIRED)
    message(FATAL_ERROR "absl::int128 target not found. "
      "Ensure opentelemetry-cpp (which fetches gRPC + abseil) is added before clickhouse-cpp.")
  endif()
endif()
