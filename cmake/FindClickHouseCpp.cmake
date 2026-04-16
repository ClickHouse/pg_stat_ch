# FindClickHouseCpp.cmake — Locate the ClickHouse C++ client library.
#
# The vcpkg port for clickhouse-cpp does not export CMake config files,
# so we use a traditional FindModule approach.
#
# Creates the imported target: ClickHouseCpp::ClickHouseCpp

include(FindPackageHandleStandardArgs)

find_path(CLICKHOUSE_CPP_INCLUDE_DIR clickhouse/client.h)
find_library(CLICKHOUSE_CPP_LIBRARY NAMES clickhouse-cpp-lib)

find_package_handle_standard_args(ClickHouseCpp
  REQUIRED_VARS CLICKHOUSE_CPP_LIBRARY CLICKHOUSE_CPP_INCLUDE_DIR
)

if(ClickHouseCpp_FOUND AND NOT TARGET ClickHouseCpp::ClickHouseCpp)
  add_library(ClickHouseCpp::ClickHouseCpp STATIC IMPORTED)
  set_target_properties(ClickHouseCpp::ClickHouseCpp PROPERTIES
    IMPORTED_LOCATION "${CLICKHOUSE_CPP_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${CLICKHOUSE_CPP_INCLUDE_DIR}"
  )

  # Transitive dependencies installed by vcpkg alongside clickhouse-cpp
  find_package(absl CONFIG REQUIRED)
  find_package(lz4 CONFIG REQUIRED)
  find_package(cityhash CONFIG REQUIRED)
  find_package(ZLIB REQUIRED)
  target_link_libraries(ClickHouseCpp::ClickHouseCpp INTERFACE
    absl::int128
    lz4::lz4
    cityhash
    ZLIB::ZLIB
  )
  if(WITH_OPENSSL)
    target_link_libraries(ClickHouseCpp::ClickHouseCpp INTERFACE
      OpenSSL::SSL OpenSSL::Crypto
    )
  endif()
endif()

mark_as_advanced(CLICKHOUSE_CPP_INCLUDE_DIR CLICKHOUSE_CPP_LIBRARY)
