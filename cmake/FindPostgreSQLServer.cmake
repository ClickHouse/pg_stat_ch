# FindPostgreSQLServer.cmake
# Finds PostgreSQL server development files using pg_config
#
# This module defines:
#   PostgreSQLServer_FOUND        - True if PostgreSQL server headers found
#   PostgreSQLServer_INCLUDE_DIR  - Server include directory
#   PostgreSQLServer_PKGLIB_DIR   - Extension library directory
#   PostgreSQLServer_SHARE_DIR    - Share directory (for .control and .sql files)
#   PostgreSQLServer_VERSION      - PostgreSQL version string
#   PostgreSQLServer::PostgreSQLServer - Imported interface target
#
# Hints:
#   PG_CONFIG - Path to pg_config executable

include(FindPackageHandleStandardArgs)

# Find pg_config
find_program(PG_CONFIG pg_config
  HINTS ${PG_CONFIG}
  DOC "Path to pg_config executable"
)

if(PG_CONFIG)
  # Query pg_config for paths
  execute_process(
    COMMAND ${PG_CONFIG} --includedir-server
    OUTPUT_VARIABLE PostgreSQLServer_INCLUDE_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
  )

  execute_process(
    COMMAND ${PG_CONFIG} --pkglibdir
    OUTPUT_VARIABLE PostgreSQLServer_PKGLIB_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
  )

  execute_process(
    COMMAND ${PG_CONFIG} --sharedir
    OUTPUT_VARIABLE PostgreSQLServer_SHARE_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
  )

  execute_process(
    COMMAND ${PG_CONFIG} --version
    OUTPUT_VARIABLE _pg_version_output
    OUTPUT_STRIP_TRAILING_WHITESPACE
  )

  # Extract version number (e.g., "PostgreSQL 16.0" -> "16.0")
  string(REGEX REPLACE "^[^ ]+ ([0-9.]+).*$" "\\1" PostgreSQLServer_VERSION "${_pg_version_output}")
endif()

find_package_handle_standard_args(PostgreSQLServer
  REQUIRED_VARS
    PG_CONFIG
    PostgreSQLServer_INCLUDE_DIR
    PostgreSQLServer_PKGLIB_DIR
    PostgreSQLServer_SHARE_DIR
  VERSION_VAR PostgreSQLServer_VERSION
)

# Create imported target
if(PostgreSQLServer_FOUND AND NOT TARGET PostgreSQLServer::PostgreSQLServer)
  add_library(PostgreSQLServer::PostgreSQLServer INTERFACE IMPORTED)
  set_target_properties(PostgreSQLServer::PostgreSQLServer PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${PostgreSQLServer_INCLUDE_DIR}"
    INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "${PostgreSQLServer_INCLUDE_DIR}"
  )
endif()

mark_as_advanced(PG_CONFIG)
