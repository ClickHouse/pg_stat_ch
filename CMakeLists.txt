cmake_minimum_required(VERSION 3.16)
project(pg_stat_ch VERSION 0.1.0 LANGUAGES CXX C)

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
include(OpenSSL)
pg_stat_ch_setup_openssl()

# Add clickhouse-cpp library (must be compiled with PIC for shared library linking)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(WITH_OPENSSL ${WITH_OPENSSL} CACHE BOOL "Pass OpenSSL setting to clickhouse-cpp" FORCE)
set(BUILD_TESTS OFF CACHE BOOL "Disable clickhouse-cpp tests")
set(BUILD_BENCHMARK OFF CACHE BOOL "Disable clickhouse-cpp benchmarks")
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
)
target_link_libraries(pg_stat_ch PRIVATE
  PostgreSQLServer::PostgreSQLServer
  clickhouse-cpp-lib
)
if(WITH_OPENSSL)
  target_link_libraries(pg_stat_ch PRIVATE OpenSSL::SSL OpenSSL::Crypto)
endif()
pg_stat_ch_set_warnings(pg_stat_ch)

set_target_properties(pg_stat_ch PROPERTIES
  PREFIX ""
  SUFFIX ".so"
)

# Install targets
install(TARGETS pg_stat_ch LIBRARY DESTINATION ${PostgreSQLServer_PKGLIB_DIR})
install(FILES pg_stat_ch.control DESTINATION ${PostgreSQLServer_SHARE_DIR}/extension)
file(GLOB SQL_FILES sql/pg_stat_ch--*.sql)
install(FILES ${SQL_FILES} DESTINATION ${PostgreSQLServer_SHARE_DIR}/extension)
