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
# Third-party dependencies (order matters: otel → arrow → clickhouse-cpp)
# ---------------------------------------------------------------------------
include(ThirdPartyOTel)
pg_stat_ch_setup_otel()

include(ThirdPartyArrow)
pg_stat_ch_setup_arrow()

include(ThirdPartyClickHouse)
pg_stat_ch_setup_clickhouse()

# Collect source files
file(GLOB_RECURSE SOURCES src/*.cc src/*.c)

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
  ${CMAKE_SOURCE_DIR}/third_party/arrow/cpp/src
  ${CMAKE_BINARY_DIR}/third_party/arrow/cpp/src
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
  arrow_static
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

  include(GoogleTest)
  gtest_discover_tests(hostname_test)
endif()
