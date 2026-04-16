# FindClickHouseCpp.cmake - Find the clickhouse-cpp library.
#
# The vcpkg port for clickhouse-cpp does not ship a CMake config file,
# so this module locates the library and headers manually.
#
# Defines:
#   ClickHouseCpp::ClickHouseCpp  - Imported STATIC library target
#   ClickHouseCpp_FOUND

include(FindPackageHandleStandardArgs)

find_path(ClickHouseCpp_INCLUDE_DIR
  NAMES clickhouse/client.h
)

find_library(ClickHouseCpp_LIBRARY
  NAMES clickhouse-cpp-lib
)

find_package_handle_standard_args(ClickHouseCpp
  REQUIRED_VARS ClickHouseCpp_LIBRARY ClickHouseCpp_INCLUDE_DIR
)

if(ClickHouseCpp_FOUND AND NOT TARGET ClickHouseCpp::ClickHouseCpp)
  add_library(ClickHouseCpp::ClickHouseCpp STATIC IMPORTED)
  set_target_properties(ClickHouseCpp::ClickHouseCpp PROPERTIES
    IMPORTED_LOCATION "${ClickHouseCpp_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${ClickHouseCpp_INCLUDE_DIR}"
    INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "${ClickHouseCpp_INCLUDE_DIR}"
  )

  # clickhouse-cpp depends on abseil, lz4, cityhash, and zstd
  find_package(absl CONFIG REQUIRED)
  find_library(_LZ4_LIB NAMES lz4 REQUIRED)
  find_library(_CITYHASH_LIB NAMES cityhash REQUIRED)
  find_library(_ZSTD_LIB NAMES zstd REQUIRED)

  target_link_libraries(ClickHouseCpp::ClickHouseCpp INTERFACE
    absl::int128
    ${_LZ4_LIB}
    ${_CITYHASH_LIB}
    ${_ZSTD_LIB}
  )
endif()

mark_as_advanced(ClickHouseCpp_INCLUDE_DIR ClickHouseCpp_LIBRARY)
