#include(ExternalProject)
include(FetchContent)
set(FETCHCONTENT_QUIET OFF)
set(FETCHCONTENT_UPDATES_DISCONNECTED ON)
set(FB_LIB_VERSION "v2022.05.30.00")

#####################################################################start icu4c
if(${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
  add_subdirectory(liburing-cmake)
endif()
#####################################################################end

#####################################################################start hash lib
add_subdirectory(hash-library-cmake)
#####################################################################end

#####################################################################start icu4c
add_subdirectory(icu-cmake)
#####################################################################end

#####################################################################start json
add_subdirectory(json-cmake)
#####################################################################end

#####################################################################start utfcpp
add_subdirectory(utfcpp-cmake)
#####################################################################end

#####################################################################start antlr4
add_subdirectory(antlr4-cmake)
#####################################################################end

#####################################################################start fmt
add_subdirectory(fmtlib-cmake)
#####################################################################end fmt

#####################################################################start libsodium
add_subdirectory(libsodium-cmake)
#####################################################################end fmt


#####################################################################start boost
add_subdirectory(boost-cmake)
#####################################################################end boost

#####################################################################start range-v3
add_subdirectory(rangev3-cmake)
#####################################################################end

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
set(EVENT__LIBRARY_TYPE "STATIC")
add_subdirectory(libevent-cmake)
#####################################################################end libevent

#####################################################################start double-conversion
add_subdirectory(double-conversion-cmake)
set(DOUBLE_CONVERSION "double-conversion")
#####################################################################end

##################################################################### compress libraries
#lz4
add_subdirectory(lz4-cmake)
set(LZ4 "lz4")

#lzo2
add_subdirectory(lzo-cmake)
add_library(lzo ALIAS lzo_static_lib)
add_library(lzo2 ALIAS lzo_static_lib)
set(LZO "lzo")

#zstd
add_subdirectory(zstd-cmake)
set(ZSTD "zstd")
set(ZSTD "zstd")
##zlib
add_subdirectory(zlib-cmake)
set(ZLIB "zlib")

#snappy
add_subdirectory(snappy-cmake)

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
set(BZIP "bz2_static")
#####################################################################

#####################################################################start re2
#re2
add_subdirectory(re2-cmake)
set(RE2 "re2")
#####################################################################end


#####################################################################start protobuf
add_subdirectory(protobuf-cmake)
#####################################################################end

#####################################################################start folly
add_definitions(-DFOLLY_MOBILE=0)
add_subdirectory(folly-cmake)
#####################################################################end


#####################################################################start velox
add_subdirectory(velox-cmake)
#####################################################################end

#####################################################################start wangle
add_subdirectory(wangle-cmake)
#####################################################################end

#####################################################################start fizz
add_subdirectory(fizz-cmake)
#####################################################################end

#####################################################################start proxygen
add_subdirectory(proxygen-cmake)
#####################################################################end
