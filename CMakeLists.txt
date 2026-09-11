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

# OpenSSL (required for clickhouse-c TLS transport)
find_package(OpenSSL REQUIRED)
message(STATUS "OpenSSL ${OPENSSL_VERSION} found")

# ---------------------------------------------------------------------------
# Third-party dependencies (via vcpkg)
# ---------------------------------------------------------------------------
find_package(opentelemetry-cpp CONFIG REQUIRED)
find_package(Arrow CONFIG REQUIRED)
# clickhouse-c (vendored under third_party/) is header-only; its compression
# codecs link against lz4 + zstd.
find_package(lz4 CONFIG REQUIRED)
find_package(zstd CONFIG REQUIRED)

# ---------------------------------------------------------------------------
# Source ownership
#
# C PostgreSQL integration and C++ exporter library are separate targets with
# explicit source lists. Exporter target has no PostgreSQL include path, so a
# PostgreSQL header included from src/export fails to compile.
# ---------------------------------------------------------------------------
set(PG_STAT_CH_PLUGIN_SOURCES
  src/pg_stat_ch.c
  src/config/guc.c
  src/hooks/hooks.c
  src/hooks/query_normalize.c
  src/hooks/query_normalize_state.c
  src/queue/local_batch.c
  src/queue/psch_dsa.c
  src/queue/query_intern.c
  src/queue/shmem.c
  src/worker/bgworker.c
  src/worker/exporter_bridge.c
)
set(PG_STAT_CH_EXPORTER_SOURCES
  src/export/arrow_batch.cc
  src/export/arrow_dump.cc
  src/export/clickhouse_c_impl.c
  src/export/clickhouse_exporter.cc
  src/export/otel_arrow_exporter.cc
  src/export/otel_exporter.cc
  src/export/psch_exporter.cc
)

# Every source under src/ must belong to one target, so nothing is silently
# dropped from the build or placed outside its boundary
file(GLOB_RECURSE PG_STAT_CH_UNLISTED_SOURCES CONFIGURE_DEPENDS
  RELATIVE ${CMAKE_SOURCE_DIR}
  src/*.c src/*.cc src/*.cxx src/*.cpp src/*.c++
)
list(REMOVE_ITEM PG_STAT_CH_UNLISTED_SOURCES
  ${PG_STAT_CH_PLUGIN_SOURCES} ${PG_STAT_CH_EXPORTER_SOURCES})
if(PG_STAT_CH_UNLISTED_SOURCES)
  message(FATAL_ERROR
    "pg_stat_ch: sources not assigned to a target in CMakeLists.txt:\n"
    "  ${PG_STAT_CH_UNLISTED_SOURCES}")
endif()

# ---------------------------------------------------------------------------
# Exporter library: C++ exporters plus clickhouse-c implementation unit.
# Standard headers and exporter dependencies only.
# ---------------------------------------------------------------------------
add_library(pg_stat_ch_exporter STATIC ${PG_STAT_CH_EXPORTER_SOURCES})
target_compile_features(pg_stat_ch_exporter PRIVATE cxx_std_17)
target_compile_definitions(pg_stat_ch_exporter PRIVATE CHC_PROVIDE_STDLIB_ALLOC)
target_include_directories(pg_stat_ch_exporter PRIVATE src)
# clickhouse-c is vendored third-party code: include it as SYSTEM so its
# header-only bodies don't trip the strict warning flags / -Werror below.
target_include_directories(pg_stat_ch_exporter SYSTEM PRIVATE
  ${CMAKE_SOURCE_DIR}/third_party/clickhouse-c
)
target_link_libraries(pg_stat_ch_exporter PRIVATE
  opentelemetry-cpp::api
  opentelemetry-cpp::sdk
  opentelemetry-cpp::otlp_grpc_metrics_exporter
  opentelemetry-cpp::otlp_grpc_log_record_exporter
  opentelemetry-cpp::metrics
  opentelemetry-cpp::logs
  Arrow::arrow_static
  OpenSSL::SSL OpenSSL::Crypto lz4::lz4 zstd::libzstd
)
# Forbid namespace-scope C++ objects needing construction or destruction:
# they would run at dlopen and at PostgreSQL process exit. Clang-only flag, CI
# runs a Clang build job for it
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  target_compile_options(pg_stat_ch_exporter PRIVATE
    $<$<COMPILE_LANGUAGE:CXX>:-Wglobal-constructors;-Werror=global-constructors>)
endif()
pg_stat_ch_set_warnings(pg_stat_ch_exporter)

# Links exporter without PostgreSQL: a stray PostgreSQL symbol reference in
# the library fails here at link time
add_executable(pg_stat_ch_exporter_check test/exporter_check.c test/exporter_oom_check.cc)
target_compile_features(pg_stat_ch_exporter_check PRIVATE cxx_std_17)
target_include_directories(pg_stat_ch_exporter_check PRIVATE src)
target_link_libraries(pg_stat_ch_exporter_check PRIVATE pg_stat_ch_exporter)
set_target_properties(pg_stat_ch_exporter_check PROPERTIES LINKER_LANGUAGE CXX)

# ---------------------------------------------------------------------------
# Extension module: C PostgreSQL integration linking exporter library
# ---------------------------------------------------------------------------
add_library(pg_stat_ch MODULE ${PG_STAT_CH_PLUGIN_SOURCES})
target_compile_definitions(pg_stat_ch PRIVATE PG_STAT_CH_VERSION="${GIT_VERSION}")
target_include_directories(pg_stat_ch PRIVATE
  include
  src
)
target_link_libraries(pg_stat_ch PRIVATE
  PostgreSQLServer::PostgreSQLServer
  pg_stat_ch_exporter
)
# Hide static-archive symbols from dynamic export: PG dlopens RTLD_GLOBAL, so
# exported dep symbols (grpc, Arrow, protobuf — ~45k) interpose across backend
# and other extensions.  dlsym entry points live in our own objects and stay
# exported (Pg_magic_func, _PG_init, pg_finfo_*, SQL funcs, PschBgworkerMain)
if(NOT APPLE)
  target_link_options(pg_stat_ch PRIVATE LINKER:--exclude-libs,ALL)
endif()
pg_stat_ch_set_warnings(pg_stat_ch)

set_target_properties(pg_stat_ch PROPERTIES
  PREFIX ""
  SUFFIX "${CMAKE_SHARED_LIBRARY_SUFFIX}"
  LINKER_LANGUAGE CXX
)
if(APPLE)
  # PGXS uses -bundle_loader on Darwin (Makefile.port) instead of
  # -undefined dynamic_lookup so undefined symbols resolve against the
  # postgres executable rather than whichever loaded dylib exports the name
  # first.  Without this, common PG symbols (e.g. hash_search) clash with
  # libSystem exports of the same name and the wrong implementation gets
  # bound at runtime — corruption / SIGTRAP.
  execute_process(COMMAND ${PG_CONFIG} --bindir
                  OUTPUT_VARIABLE PG_BINDIR
                  OUTPUT_STRIP_TRAILING_WHITESPACE)
  target_link_options(pg_stat_ch PRIVATE -bundle_loader ${PG_BINDIR}/postgres)
endif()

# Install targets
install(TARGETS pg_stat_ch LIBRARY DESTINATION ${PostgreSQLServer_PKGLIB_DIR})
install(FILES pg_stat_ch.control DESTINATION ${PostgreSQLServer_SHARE_DIR}/extension)
file(GLOB SQL_FILES sql/pg_stat_ch--*.sql)
install(FILES ${SQL_FILES} DESTINATION ${PostgreSQLServer_SHARE_DIR}/extension)
