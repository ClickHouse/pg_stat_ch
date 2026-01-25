# OpenSSL support for pg_stat_ch
# This module finds OpenSSL and configures it for use with clickhouse-cpp

macro(pg_stat_ch_setup_openssl)
    if(WITH_OPENSSL)
        find_package(OpenSSL REQUIRED)
        message(STATUS "OpenSSL ${OPENSSL_VERSION} found")
        add_compile_definitions(WITH_OPENSSL=1)
    else()
        message(STATUS "OpenSSL support disabled")
    endif()
endmacro()
