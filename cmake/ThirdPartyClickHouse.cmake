# cmake/ThirdPartyClickHouse.cmake — ClickHouse C++ client
macro(pg_stat_ch_setup_clickhouse)
  set(WITH_SYSTEM_ABSEIL ON CACHE BOOL "Use abseil from gRPC build" FORCE)
  set(WITH_OPENSSL ${WITH_OPENSSL} CACHE BOOL "Pass OpenSSL setting to clickhouse-cpp" FORCE)
  set(BUILD_TESTS OFF CACHE BOOL "Disable clickhouse-cpp tests" FORCE)
  set(BUILD_BENCHMARK OFF CACHE BOOL "Disable clickhouse-cpp benchmarks" FORCE)
  add_subdirectory(${CMAKE_SOURCE_DIR}/third_party/clickhouse-cpp EXCLUDE_FROM_ALL)
endmacro()
