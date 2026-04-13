cmake_minimum_required(VERSION 3.16)
project(pg_stat_ch VERSION 0.1.0 LANGUAGES CXX C)

if(WIN32)
  message(FATAL_ERROR "Windows is not supported")
endif()

# Add cmake modules
list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")

# Get version from git
include(GitVersion)
get_git_version(GIT_VERSION)
message(STATUS "Version: ${GIT_VERSION}")

# Find PostgreSQL
find_package(PostgreSQLServer REQUIRED)
message(STATUS "PostgreSQL ${PostgreSQLServer_VERSION} found")

# Compiler warnings
include(CompilerWarnings)

# OpenSSL support (enables TLS connections to ClickHouse)
option(WITH_OPENSSL "Enable OpenSSL for TLS connections" ON)
include(PgStatChOpenSSL)
pg_stat_ch_setup_openssl()

# Build all third-party libraries as static with PIC
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(BUILD_SHARED_LIBS OFF CACHE BOOL "Build third-party libs as static" FORCE)

# ---------------------------------------------------------------------------
# Add opentelemetry-cpp FIRST (fetches gRPC + abseil via FetchContent).
# This must come before clickhouse-cpp to avoid duplicate abseil targets.
# ---------------------------------------------------------------------------
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
add_subdirectory(third_party/opentelemetry-cpp EXCLUDE_FROM_ALL)

# ---------------------------------------------------------------------------
# Add clickhouse-cpp AFTER opentelemetry-cpp.
# Use WITH_SYSTEM_ABSEIL so it finds the abseil already built by gRPC above
# (via our cmake/Findabsl.cmake shim).
# ---------------------------------------------------------------------------
set(WITH_SYSTEM_ABSEIL ON CACHE BOOL "Use abseil from gRPC build" FORCE)
set(WITH_OPENSSL ${WITH_OPENSSL} CACHE BOOL "Pass OpenSSL setting to clickhouse-cpp" FORCE)
set(BUILD_TESTS OFF CACHE BOOL "Disable clickhouse-cpp tests" FORCE)
set(BUILD_BENCHMARK OFF CACHE BOOL "Disable clickhouse-cpp benchmarks" FORCE)
add_subdirectory(third_party/clickhouse-cpp EXCLUDE_FROM_ALL)

# Collect source files
file(GLOB_RECURSE SOURCES src/*.cc)

# Build shared library
add_library(pg_stat_ch SHARED ${SOURCES})

target_compile_features(pg_stat_ch PRIVATE cxx_std_17)
target_compile_definitions(pg_stat_ch PRIVATE PG_STAT_CH_VERSION="${GIT_VERSION}")
target_include_directories(pg_stat_ch PRIVATE
  include
  src
)
# Mark third-party includes as SYSTEM to suppress warnings from those headers
target_include_directories(pg_stat_ch SYSTEM PRIVATE
  ${CMAKE_SOURCE_DIR}/third_party/clickhouse-cpp
  ${CMAKE_SOURCE_DIR}/third_party/clickhouse-cpp/contrib/absl
  ${CMAKE_SOURCE_DIR}/third_party/clickhouse-cpp/contrib/zstd
  ${CMAKE_SOURCE_DIR}/third_party/opentelemetry-cpp/api/include
  ${CMAKE_SOURCE_DIR}/third_party/opentelemetry-cpp/sdk/include
  ${CMAKE_SOURCE_DIR}/third_party/opentelemetry-cpp/exporters/otlp/include
  ${CMAKE_SOURCE_DIR}/third_party/opentelemetry-cpp/third_party/nlohmann-json/single_include
)
target_link_libraries(pg_stat_ch PRIVATE
  PostgreSQLServer::PostgreSQLServer
  clickhouse-cpp-lib
  opentelemetry_api
  opentelemetry_sdk
  opentelemetry_exporter_otlp_grpc_metrics
  opentelemetry_exporter_otlp_grpc_log
  opentelemetry_metrics
  opentelemetry_logs
)
if(WITH_OPENSSL)
  target_link_libraries(pg_stat_ch PRIVATE OpenSSL::SSL OpenSSL::Crypto)
endif()
pg_stat_ch_set_warnings(pg_stat_ch)

set_target_properties(pg_stat_ch PROPERTIES
  PREFIX ""
  SUFFIX "${CMAKE_SHARED_LIBRARY_SUFFIX}"
)
if(APPLE)
  target_link_options(pg_stat_ch PRIVATE -undefined dynamic_lookup)
endif()

# Install targets
install(TARGETS pg_stat_ch LIBRARY DESTINATION ${PostgreSQLServer_PKGLIB_DIR})
install(FILES pg_stat_ch.control DESTINATION ${PostgreSQLServer_SHARE_DIR}/extension)
file(GLOB SQL_FILES sql/pg_stat_ch--*.sql)
install(FILES ${SQL_FILES} DESTINATION ${PostgreSQLServer_SHARE_DIR}/extension)

# ---------------------------------------------------------------------------
# C++ unit tests (opt-in; no running PostgreSQL or ClickHouse required)
# Enable with: cmake -DPSCH_BUILD_UNIT_TESTS=ON
# ---------------------------------------------------------------------------
option(PSCH_BUILD_UNIT_TESTS "Build pg_stat_ch C++ unit tests" OFF)
if(PSCH_BUILD_UNIT_TESTS)
  FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.15.2
  )
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)

  enable_testing()

  add_executable(hostname_test
    test/unit/hostname_test.cc
    src/export/otel_exporter.cc
  )
  target_include_directories(hostname_test PRIVATE src include)
  target_include_directories(hostname_test SYSTEM PRIVATE
    ${CMAKE_SOURCE_DIR}/third_party/opentelemetry-cpp/api/include
    ${CMAKE_SOURCE_DIR}/third_party/opentelemetry-cpp/sdk/include
    ${CMAKE_SOURCE_DIR}/third_party/opentelemetry-cpp/exporters/otlp/include
  )
  target_compile_features(hostname_test PRIVATE cxx_std_17)
  target_compile_definitions(hostname_test PRIVATE PG_STAT_CH_VERSION="${GIT_VERSION}")
  target_link_libraries(hostname_test PRIVATE
    GTest::gtest_main
    PostgreSQLServer::PostgreSQLServer
    opentelemetry_api
    opentelemetry_sdk
    opentelemetry_exporter_otlp_grpc_metrics
    opentelemetry_exporter_otlp_grpc_log
    opentelemetry_metrics
    opentelemetry_logs
  )
  pg_stat_ch_set_warnings(hostname_test)

  add_executable(sqlcommenter_parse_test
    test/unit/sqlcommenter_parse_test.cc
    src/export/sqlcommenter_parse.cc
  )
  target_include_directories(sqlcommenter_parse_test PRIVATE src include)
  target_include_directories(sqlcommenter_parse_test SYSTEM PRIVATE
    ${CMAKE_SOURCE_DIR}/third_party/opentelemetry-cpp/third_party/nlohmann-json/single_include
  )
  target_compile_features(sqlcommenter_parse_test PRIVATE cxx_std_17)
  target_link_libraries(sqlcommenter_parse_test PRIVATE GTest::gtest_main)
  pg_stat_ch_set_warnings(sqlcommenter_parse_test)

  include(GoogleTest)
  gtest_discover_tests(hostname_test)
  gtest_discover_tests(sqlcommenter_parse_test)
endif()
