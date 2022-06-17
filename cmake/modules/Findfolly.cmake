option(USE_INTERNAL_FOLLY_LIBRARY "Set to FALSE to use system fmt library instead of bundled)" ON)

if(NOT USE_INTERNAL_FOLLY_LIBRARY)
  find_library(folly)
endif()

i

message(STATUS "Using fmt ${FOLLY_INCLUDE_DIR} : ${FOLLY_LIBRARY}")
