set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CMAKE_SYSTEM_NAME Linux)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)
set(VCPKG_CXX_FLAGS "-fPIC")
set(VCPKG_C_FLAGS "-fPIC")

# Link OpenSSL dynamically: PG dlopens extensions RTLD_GLOBAL, so SSL-enabled
# backends' system libssl.so.3 interposes an embedded copy anyway; dynamic
# linkage makes that explicit and leaves CVE patching to distro
if(PORT STREQUAL "openssl")
  set(VCPKG_LIBRARY_LINKAGE dynamic)
endif()
