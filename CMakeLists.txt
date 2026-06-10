cmake_minimum_required(VERSION 3.16)
project(pg_stat_ch VERSION 0.1.0 LANGUAGES C)

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

# OpenSSL (required for clickhouse-c TLS transport and the OTLP https path)
find_package(OpenSSL REQUIRED)
message(STATUS "OpenSSL ${OPENSSL_VERSION} found")

# ---------------------------------------------------------------------------
# Third-party dependencies (via vcpkg)
# ---------------------------------------------------------------------------
# clickhouse-c (vendored under third_party/) is header-only; its compression
# codecs link against lz4 + zstd.  zstd is also used directly for Arrow IPC
# per-buffer body compression in the export path.
find_package(lz4 CONFIG REQUIRED)
find_package(zstd CONFIG REQUIRED)

# The extension is pure C.  PostgreSQL's error handling is longjmp-based, and
# the production SIGABRT this rewrite removed came from C++ dependencies that
# abort() under memory pressure (see OTEL_REWRITE_DESIGN.md).  Reject any C++
# source — with LANGUAGES C it could not be compiled anyway, but the explicit
# check turns "silently dropped from the build" into a configure-time error.
#
# CONFIGURE_DEPENDS: re-run the glob on every build, not just at first
# configure.  Without it, a newly-added source would slip past the build (and
# the stray check) until someone manually reconfigured.
file(GLOB_RECURSE PG_STAT_CH_SOURCES CONFIGURE_DEPENDS src/*.c)
file(GLOB_RECURSE PG_STAT_CH_OFFENDING_SOURCES CONFIGURE_DEPENDS
  src/*.cc src/*.cxx src/*.cpp src/*.c++
)
if(PG_STAT_CH_OFFENDING_SOURCES)
  message(FATAL_ERROR
    "pg_stat_ch is pure C (project LANGUAGES C): C++ sources are not "
    "compiled.  Port to C.  Offending:\n"
    "  ${PG_STAT_CH_OFFENDING_SOURCES}")
endif()

# nanoarrow (vendored amalgamation, see third_party/nanoarrow/VERSION): array
# building + IPC flatbuffer machinery for the C exporter.  Symbols are
# namespaced (PgStatCh prefix) by the upstream bundler.  Compiled as ordinary
# TUs; warnings suppressed because it is third-party code (headers are SYSTEM).
# FLATCC_NO_ASSERT outside Debug: every failure path in nanoarrow/flatcc then
# returns an error code (ENOMEM etc.) — no abort()/assert() reachable from the
# export path, which is the property the exporter rewrite exists to guarantee.
set(PG_STAT_CH_NANOARROW_SOURCES
  third_party/nanoarrow/src/nanoarrow.c
  third_party/nanoarrow/src/nanoarrow_ipc.c
  third_party/nanoarrow/src/flatcc.c
)
set_source_files_properties(${PG_STAT_CH_NANOARROW_SOURCES} PROPERTIES
  COMPILE_OPTIONS "-w"
  COMPILE_DEFINITIONS "$<$<NOT:$<CONFIG:Debug>>:FLATCC_NO_ASSERT>"
)
# flatcc's emitter must allocate its pages from our fixed pool, not the system
# heap, so steady-state Arrow export stays allocation-free (see
# src/export/flatcc_emitter_alloc.h).  Force-include the override header before
# flatcc_emitter.h's #ifndef FLATCC_EMITTER_ALLOC guard sees the defaults.
set_source_files_properties(third_party/nanoarrow/src/flatcc.c PROPERTIES
  COMPILE_OPTIONS "-w;-include;${CMAKE_SOURCE_DIR}/src/export/flatcc_emitter_alloc.h"
  COMPILE_DEFINITIONS "$<$<NOT:$<CONFIG:Debug>>:FLATCC_NO_ASSERT>"
)

# Build shared library
add_library(pg_stat_ch MODULE
  ${PG_STAT_CH_SOURCES}
  ${PG_STAT_CH_NANOARROW_SOURCES}
)

target_compile_definitions(pg_stat_ch PRIVATE PG_STAT_CH_VERSION="${GIT_VERSION}")
target_include_directories(pg_stat_ch PRIVATE
  include
  src
)
# clickhouse-c and nanoarrow are vendored third-party code: include them as
# SYSTEM so their header-only bodies don't trip the strict warning flags /
# -Werror below.
target_include_directories(pg_stat_ch SYSTEM PRIVATE
  ${CMAKE_SOURCE_DIR}/third_party/clickhouse-c
  ${CMAKE_SOURCE_DIR}/third_party/nanoarrow/include
)
target_link_libraries(pg_stat_ch PRIVATE
  PostgreSQLServer::PostgreSQLServer
)
target_link_libraries(pg_stat_ch PRIVATE OpenSSL::SSL OpenSSL::Crypto lz4::lz4 zstd::libzstd)
pg_stat_ch_set_warnings(pg_stat_ch)

# gnu17, not c17: PostgreSQL headers rely on GNU extensions, and C23 (some
# compilers' default) repurposes `bool` in a way that conflicts with older PG
# headers.  Pinned here so builds outside mise (CI, Docker) don't depend on
# env CFLAGS.
set_target_properties(pg_stat_ch PROPERTIES
  PREFIX ""
  SUFFIX "${CMAKE_SHARED_LIBRARY_SUFFIX}"
  C_STANDARD 17
  C_EXTENSIONS ON
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
