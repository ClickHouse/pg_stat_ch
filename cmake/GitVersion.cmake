# GitVersion.cmake
# Extracts version information from git
#
# Sets:
#   GIT_VERSION - Version string from git describe, or fallback

function(get_git_version output_var)
  set(fallback_version "0.1")

  if(NOT "${PG_STAT_CH_PACKAGE_VERSION}" STREQUAL "")
    set(${output_var} "${PG_STAT_CH_PACKAGE_VERSION}" PARENT_SCOPE)
    return()
  endif()

  find_package(Git QUIET)

  if(GIT_FOUND AND EXISTS "${CMAKE_SOURCE_DIR}/.git")
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

  if(EXISTS "${CMAKE_SOURCE_DIR}/META.json")
    if(CMAKE_VERSION VERSION_GREATER_EQUAL "3.19")
      file(READ "${CMAKE_SOURCE_DIR}/META.json" metadata)
      string(JSON package_version ERROR_VARIABLE json_error GET "${metadata}" version)
      if(NOT json_error AND NOT "${package_version}" STREQUAL "")
        set(fallback_version "${package_version}")
      endif()
    else()
      # CMake 3.16 through 3.18 has no built-in JSON reader.
      find_package(Python3 QUIET COMPONENTS Interpreter)
      if(Python3_Interpreter_FOUND)
        execute_process(
          COMMAND "${Python3_EXECUTABLE}" -c
                  "import json, sys; print(json.load(open(sys.argv[1]))['version'])"
                  "${CMAKE_SOURCE_DIR}/META.json"
          OUTPUT_VARIABLE package_version OUTPUT_STRIP_TRAILING_WHITESPACE
          RESULT_VARIABLE json_result ERROR_QUIET)
        if(json_result EQUAL 0 AND NOT "${package_version}" STREQUAL "")
          set(fallback_version "${package_version}")
        endif()
      endif()
    endif()
  endif()

  set(${output_var} "${fallback_version}" PARENT_SCOPE)
endfunction()
