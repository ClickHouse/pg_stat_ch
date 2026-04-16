vcpkg_check_linkage(ONLY_STATIC_LIBRARY)

vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO google/cityhash
    REF f5dc54147fcce12cefd16548c8e760d68ac04226
    SHA512 bebdcc3a68b7f7405447b7a87ee9bc1f5b74f0223c93c1d741473775ae0f160f15a60198e0894910e0001e64f91f67f84bff04b3769613bd22dc448fa71ad338
    HEAD_REF master
)

file(COPY "${CMAKE_CURRENT_LIST_DIR}/CMakeLists.txt" DESTINATION "${SOURCE_PATH}")

if(VCPKG_TARGET_IS_WINDOWS)
    file(COPY "${CMAKE_CURRENT_LIST_DIR}/config.h" DESTINATION "${SOURCE_PATH}/src")
else()
    # Upstream cityhash ships a 2009-vintage config.guess that does not
    # recognise aarch64. Pass --build explicitly so configure does not
    # invoke config.guess at all.
    set(_cityhash_configure_args "")
    if(VCPKG_TARGET_ARCHITECTURE STREQUAL "arm64")
        list(APPEND _cityhash_configure_args "--build=aarch64-unknown-linux-gnu")
    elseif(VCPKG_TARGET_ARCHITECTURE STREQUAL "x64")
        list(APPEND _cityhash_configure_args "--build=x86_64-unknown-linux-gnu")
    endif()

    file(MAKE_DIRECTORY "${SOURCE_PATH}/out")
    vcpkg_execute_required_process(
        COMMAND "${SOURCE_PATH}/configure" ${_cityhash_configure_args}
        WORKING_DIRECTORY "${SOURCE_PATH}/out"
        LOGNAME configure-${TARGET_TRIPLET}
    )
    file(COPY "${SOURCE_PATH}/out/config.h" DESTINATION "${SOURCE_PATH}/src")
endif()

vcpkg_check_features(OUT_FEATURE_OPTIONS FEATURE_OPTIONS
    FEATURES
        "sse"   ENABLE_SSE
)

vcpkg_cmake_configure(
    SOURCE_PATH ${SOURCE_PATH}
    OPTIONS
        ${FEATURE_OPTIONS}
)

vcpkg_cmake_install()
vcpkg_copy_pdbs()
vcpkg_cmake_config_fixup(CONFIG_PATH share/cmake/cityhash)

file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include"
                    "${CURRENT_PACKAGES_DIR}/debug/share")

configure_file("${SOURCE_PATH}/COPYING" "${CURRENT_PACKAGES_DIR}/share/cityhash/copyright" COPYONLY)
