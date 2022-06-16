fetchcontent_declare(
  lz4
  GIT_REPOSITORY https://github.com/lz4/lz4.git
  GIT_TAG v1.9.3
  SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/lz4
)

fetchcontent_getproperties(lz4)

if(NOT lz4_POPULATED)
  message(STATUS "Fetching lz4")
  fetchcontent_populate(lz4)
  message(STATUS "Fetching lz4 - done")
  set(LZ4_SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/lz4)
endif()

set(LZ4_LIB_SOURCE_DIR "${LZ4_SOURCE_DIR}/lib")

set(
  LZ4_SOURCES
  "${LZ4_LIB_SOURCE_DIR}/lz4.c"
  "${LZ4_LIB_SOURCE_DIR}/lz4hc.c"
  "${LZ4_LIB_SOURCE_DIR}/lz4.h"
  "${LZ4_LIB_SOURCE_DIR}/lz4hc.h"
  "${LZ4_LIB_SOURCE_DIR}/lz4frame.c"
  "${LZ4_LIB_SOURCE_DIR}/lz4frame.h"
  "${LZ4_LIB_SOURCE_DIR}/xxhash.c"
)


add_library(lz4_static STATIC ${LZ4_SOURCES})
target_include_directories(
  lz4_static
  SYSTEM BEFORE INTERFACE ${LZ4_LIB_SOURCE_DIR}
)
add_library(lz4 ALIAS lz4_static)
