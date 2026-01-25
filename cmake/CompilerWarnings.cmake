# CompilerWarnings.cmake
# Common compiler warning flags for C++ targets
#
# Provides:
#   pg_stat_ch_set_warnings(target) - Apply warning flags to a target

function(pg_stat_ch_set_warnings target)
  target_compile_options(${target} PRIVATE
    # Core warnings
    -Wall
    -Wextra

    # Additional useful warnings
    -Wshadow
    -Wswitch
    -Wunused-parameter
    -Wunreachable-code

    # Suppress noisy warnings
    -Wno-sign-compare

    # Code generation
    -fPIC
    -fvisibility=hidden
    # Note: We need exceptions and RTTI for clickhouse-cpp integration
    # -fno-exceptions
    # -fno-rtti
  )

  # Optional: treat warnings as errors in CI
  if(DEFINED ENV{CI} OR WERROR)
    target_compile_options(${target} PRIVATE -Werror)
  endif()
endfunction()
