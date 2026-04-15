# cmake/ThirdPartyOTel.cmake — OpenTelemetry C++ with vendored gRPC + abseil
macro(pg_stat_ch_setup_otel)
  include(FetchContent)
  set(WITH_OTLP_GRPC ON CACHE BOOL "Enable OTel OTLP gRPC exporter" FORCE)
  set(WITH_OTLP_HTTP OFF CACHE BOOL "" FORCE)
  set(BUILD_TESTING OFF CACHE BOOL "Disable tests" FORCE)
  set(WITH_BENCHMARK OFF CACHE BOOL "Disable benchmarks" FORCE)
  set(WITH_EXAMPLES OFF CACHE BOOL "Disable OTel examples" FORCE)
  set(WITH_FUNC_TESTS OFF CACHE BOOL "Disable OTel functional tests" FORCE)
  set(OPENTELEMETRY_INSTALL OFF CACHE BOOL "" FORCE)
  set(WITH_ABSEIL ON CACHE BOOL "Use Abseil for OTel" FORCE)
  # Always use vendored gRPC + abseil (never pick up system packages).
  # This prevents find_package(gRPC) from short-circuiting FetchContent
  # and leaving absl:: targets undefined.
  set(CMAKE_DISABLE_FIND_PACKAGE_gRPC TRUE)
  # Use system OpenSSL for gRPC instead of BoringSSL to avoid symbol
  # conflicts with clickhouse-cpp which also links OpenSSL.
  set(gRPC_SSL_PROVIDER "package" CACHE STRING "Use system OpenSSL for gRPC" FORCE)
  add_subdirectory(${CMAKE_SOURCE_DIR}/third_party/opentelemetry-cpp EXCLUDE_FROM_ALL)
endmacro()
