# CompilerWarnings.cmake
# Common compiler warning flags for the project's C targets
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

    # C interface hygiene (PostgreSQL practice): every external function has
    # a prototype, no K&R declarations/definitions, no stack-sized-by-input
    # variable-length arrays.
    -Wmissing-prototypes
    -Wstrict-prototypes
    -Wold-style-definition
    -Wvla

    # Suppress noisy warnings
    -Wno-sign-compare

    # Code generation
    -fPIC
    -fvisibility=hidden
    -fno-omit-frame-pointer  # Enable frame pointers for perf profiling
  )

  # Optional: treat warnings as errors in CI
  if(DEFINED ENV{CI} OR WERROR)
    target_compile_options(${target} PRIVATE -Werror)
  endif()
endfunction()
