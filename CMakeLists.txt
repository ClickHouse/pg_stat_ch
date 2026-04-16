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

# OpenSSL (required — vcpkg manifest always builds clickhouse-cpp with openssl)
find_package(OpenSSL REQUIRED)
message(STATUS "OpenSSL ${OPENSSL_VERSION} found")

# ---------------------------------------------------------------------------
# Third-party dependencies (via vcpkg)
# ---------------------------------------------------------------------------
find_package(opentelemetry-cpp CONFIG REQUIRED)
find_package(ClickHouseCpp REQUIRED)

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
target_link_libraries(pg_stat_ch PRIVATE
  PostgreSQLServer::PostgreSQLServer
  ClickHouseCpp::ClickHouseCpp
  opentelemetry-cpp::api
  opentelemetry-cpp::sdk
  opentelemetry-cpp::otlp_grpc_metrics_exporter
  opentelemetry-cpp::otlp_grpc_log_record_exporter
  opentelemetry-cpp::metrics
  opentelemetry-cpp::logs
)
target_link_libraries(pg_stat_ch PRIVATE OpenSSL::SSL OpenSSL::Crypto)
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
  find_package(GTest CONFIG REQUIRED)

  enable_testing()

  add_executable(hostname_test
    test/unit/hostname_test.cc
    src/export/otel_exporter.cc
  )
  target_include_directories(hostname_test PRIVATE src include)
  target_compile_features(hostname_test PRIVATE cxx_std_17)
  target_compile_definitions(hostname_test PRIVATE PG_STAT_CH_VERSION="${GIT_VERSION}")
  target_link_libraries(hostname_test PRIVATE
    GTest::gtest_main
    PostgreSQLServer::PostgreSQLServer
    opentelemetry-cpp::api
    opentelemetry-cpp::sdk
    opentelemetry-cpp::otlp_grpc_metrics_exporter
    opentelemetry-cpp::otlp_grpc_log_record_exporter
    opentelemetry-cpp::metrics
    opentelemetry-cpp::logs
  )
  pg_stat_ch_set_warnings(hostname_test)

  include(GoogleTest)
  gtest_discover_tests(hostname_test)
endif()
