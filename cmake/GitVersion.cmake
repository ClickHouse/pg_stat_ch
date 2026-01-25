# GitVersion.cmake
# Extracts version information from git
#
# Sets:
#   GIT_VERSION - Version string from git describe, or fallback

function(get_git_version output_var)
  set(fallback_version "0.1.0")

  find_package(Git QUIET)

  if(GIT_FOUND)
    execute_process(
      COMMAND ${GIT_EXECUTABLE} describe --always --dirty
      WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
      OUTPUT_VARIABLE _git_version
      OUTPUT_STRIP_TRAILING_WHITESPACE
      ERROR_QUIET
      RESULT_VARIABLE _git_result
    )

    if(_git_result EQUAL 0 AND _git_version)
      set(${output_var} "${_git_version}" PARENT_SCOPE)
      return()
    endif()
  endif()

  set(${output_var} "${fallback_version}" PARENT_SCOPE)
endfunction()
