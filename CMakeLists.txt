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

# Source layout: src/*.c is the C plugin layer; src/export/*.cc is the C++
# exporter.  Keeping the PG ↔ extension boundary in pure C is what prevents
# an uncaught C++ throw from unwinding across PG's longjmp frames.  Reject
# anything that fell outside the two legitimate globs — C++ in the wrong
# location, or any C++ extension we don't compile (.cxx/.cpp/.c++) that
# would otherwise be silently dropped from the build.
file(GLOB_RECURSE PG_STAT_CH_PLUGIN_SOURCES CONFIGURE_DEPENDS src/*.c)
file(GLOB_RECURSE PG_STAT_CH_EXPORTER_SOURCES CONFIGURE_DEPENDS src/export/*.cc)

# CONFIGURE_DEPENDS: re-run the glob on every build, not just at first
# configure.  Without it, a newly-added src/foo.cc would silently slip past
# the stray check until someone manually reconfigured.
file(GLOB_RECURSE PG_STAT_CH_OFFENDING_SOURCES CONFIGURE_DEPENDS
  src/*.cc src/*.cxx src/*.cpp src/*.c++
)
list(FILTER PG_STAT_CH_OFFENDING_SOURCES
  EXCLUDE REGEX "^${CMAKE_SOURCE_DIR}/src/export/.*\\.cc$")
if(PG_STAT_CH_OFFENDING_SOURCES)
  message(FATAL_ERROR
    "pg_stat_ch: C++ sources are allowed only as *.cc under src/export/.  "
    "Port to C, move under src/export/, or rename to .cc.  Offending:\n"
    "  ${PG_STAT_CH_OFFENDING_SOURCES}")
endif()

# Defense in depth on top of the C-only plugin lock above: when building the
# exporter with Clang, forbid namespace-scope C++ variables that need a
# non-trivial constructor or destructor.  -Wglobal-constructors catches both
# halves at namespace scope: the load-time "std::thread above main" / "static
# std::map<...> g_cache" hazard, and the exit-time "destructor runs after PG
# has already torn down" hazard.
#
# Clang-only: GCC has no equivalent flag (the warning has been requested
# upstream for years but never landed).  GCC builds rely on code review +
# clang-tidy.  Our CI runs an additional Clang build job so regressions are
# caught before they merge.
#
# -Wexit-time-destructors (the sibling warning) is intentionally NOT enabled
# here.  At namespace scope, -Wglobal-constructors already covers what we
# care about.  -Wexit-time-destructors additionally fires on function-local
# statics — the two cached Arrow DataType shared_ptrs in arrow_batch.cc
# would trip it today.  Not enabling it avoids those annotations for now;
# easy to layer on later if a regression motivates it.
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  set_source_files_properties(${PG_STAT_CH_EXPORTER_SOURCES}
    PROPERTIES COMPILE_OPTIONS
      "-Wglobal-constructors;-Werror=global-constructors")
endif()

# Build shared library
add_library(pg_stat_ch MODULE
  ${PG_STAT_CH_PLUGIN_SOURCES}
  ${PG_STAT_CH_EXPORTER_SOURCES}
)

target_compile_features(pg_stat_ch PRIVATE cxx_std_17)
target_compile_definitions(pg_stat_ch PRIVATE PG_STAT_CH_VERSION="${GIT_VERSION}")
# Force-include libintl.h before any source file so its declarations are parsed
# before PostgreSQL's postgres.h defines gettext/ngettext as macros.  The include
# guard then prevents re-inclusion when C++ <locale> pulls it in later (via Arrow).
target_compile_options(pg_stat_ch PRIVATE -include libintl.h)
target_include_directories(pg_stat_ch PRIVATE
  include
  src
)
# clickhouse-c is vendored third-party code: include it as SYSTEM so its
# header-only bodies don't trip the strict warning flags / -Werror below.
target_include_directories(pg_stat_ch SYSTEM PRIVATE
  ${CMAKE_SOURCE_DIR}/third_party/clickhouse-c
)
target_link_libraries(pg_stat_ch PRIVATE
  PostgreSQLServer::PostgreSQLServer
  opentelemetry-cpp::api
  opentelemetry-cpp::sdk
  opentelemetry-cpp::otlp_grpc_metrics_exporter
  opentelemetry-cpp::otlp_grpc_log_record_exporter
  opentelemetry-cpp::metrics
  opentelemetry-cpp::logs
  Arrow::arrow_static
)
target_link_libraries(pg_stat_ch PRIVATE OpenSSL::SSL OpenSSL::Crypto lz4::lz4 zstd::libzstd)
pg_stat_ch_set_warnings(pg_stat_ch)

set_target_properties(pg_stat_ch PROPERTIES
  PREFIX ""
  SUFFIX "${CMAKE_SHARED_LIBRARY_SUFFIX}"
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

