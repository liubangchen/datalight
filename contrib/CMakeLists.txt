#include(ExternalProject)
include(FetchContent)
set(FETCHCONTENT_QUIET OFF)
set(FETCHCONTENT_UPDATES_DISCONNECTED ON)

#####################################################################start fmt
add_subdirectory(fmtlib-cmake)
#####################################################################end fmt

#####################################################################start boost
add_subdirectory(boost-cmake)
#####################################################################end boost

#####################################################################start openssl
add_subdirectory(openssl-cmake)
#####################################################################end openssl

#####################################################################start gflags
add_subdirectory(gflags-cmake)
#####################################################################end gflags
##libunwind on linux TODO


#####################################################################start glog
add_subdirectory(glog-cmake)
#####################################################################end glog

#####################################################################start libevent
fetchcontent_declare(
  libevent
  GIT_REPOSITORY https://github.com/libevent/libevent.git
  GIT_TAG release-2.1.12-stable
  GIT_PROGRESS TRUE
  SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/libevent
)
fetchcontent_getproperties(libevent)
if(NOT libevent_POPULATED)
  message(STATUS "Fetching libevent")
  fetchcontent_populate(libevent)
  message(STATUS "Fetching libevent - done")
  set(LIBEVENT_SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/libevent)
endif()

set(EVENT__LIBRARY_TYPE STATIC)
set(EVENT__FORCE_KQUEUE_CHECK ON)
add_subdirectory(libevent)
#####################################################################end libevent

#####################################################################start double-conversion
add_subdirectory(double-conversion-cmake)
#####################################################################end

##################################################################### compress libraries
#lz4
add_subdirectory(lz4-cmake)
#lzo2
add_subdirectory(lzo-cmake)
add_library(lzo ALIAS lzo_static_lib)
add_library(lzo2 ALIAS lzo_static_lib)
#zstd
add_subdirectory(zstd-cmake)
##zlib
add_subdirectory(zlib-cmake)
#snappy
fetchcontent_declare(
  snappy
  GIT_REPOSITORY https://github.com/google/snappy.git
  GIT_TAG 1.1.9
  GIT_PROGRESS TRUE
  SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/snappy
)

fetchcontent_getproperties(snappy)

if(NOT snappy_POPULATED)
  message(STATUS "Fetching snappy")
  fetchcontent_populate(snappy)
  message(STATUS "Fetching snappy - done")
  set(SNAPPY_SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/snappy)
endif()

set(BUILD_SHARED_LIBS OFF)
set(SNAPPY_BUILD_TESTS OFF)
set(SNAPPY_BUILD_BENCHMARKS OFF)
set(SNAPPY_INSTALL OFF)
add_subdirectory(snappy)

##bzip2
fetchcontent_declare(
  bzip2
  GIT_REPOSITORY https://github.com/libarchive/bzip2.git
  #GIT_TAG master
  GIT_PROGRESS TRUE
  SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/bzip2
)

fetchcontent_getproperties(bzip2)

if(NOT bzip2_POPULATED)
  message(STATUS "Fetching bzip2")
  fetchcontent_populate(bzip2)
  message(STATUS "Fetching bzip2 - done")
  set(BZIP2_SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/bzip2)
endif()

set(ENABLE_STATIC_LIB ON)
set(ENABLE_SHARED_LIB OFF)
set(ENABLE_DEBUG OFF)
set(ENABLE_LIB_ONLY ON)
add_subdirectory(bzip2)
add_library(bzip2 ALIAS bz2_static)
#####################################################################

#####################################################################start re2
#re2
fetchcontent_declare(
  re2
  GIT_REPOSITORY https://github.com/google/re2.git
  GIT_TAG 2021-11-01
  GIT_PROGRESS TRUE
  SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/re2
)

fetchcontent_getproperties(re2)

if(NOT re2_POPULATED)
  message(STATUS "Fetching re2")
  fetchcontent_populate(re2)
  message(STATUS "Fetching re2 - done")
  set(RE2_SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/re2)
endif()

set(RE2_BUILD_TESTING OFF)
add_subdirectory(re2)
#####################################################################end


###icu4c
##set(ENABLE_ICU ON)
##add_subdirectory(icu-cmake)
##add_library(icu ALIAS _icu)

#####################################################################start protobuf
add_subdirectory(protobuf-cmake)
#add_library(protobuf::libprotobuf ALIAS libprotobuf)
#add_dependencies(libprotobuf protoc)
#include(${CMAKE_INSTALL_CMAKEDIR}/protobuf-config.cmake)
#include(${CMAKE_INSTALL_CMAKEDIR}/protobuf-config-version.cmake)
#include(${CMAKE_INSTALL_CMAKEDIR}/protobuf-module.cmake)
#include(${CMAKE_INSTALL_CMAKEDIR}/protobuf-options.cmake)
#####################################################################end

#####################################################################start folly
add_definitions(-DFOLLY_MOBILE=0)
add_subdirectory(folly-cmake)
#####################################################################end


#velox
add_subdirectory(velox-cmake)
add_subdirectory(velox/third_party)
add_subdirectory(velox/velox)
