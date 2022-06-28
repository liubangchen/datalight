fetchcontent_declare(
  range-v3
  GIT_REPOSITORY https://github.com/ericniebler/range-v3.git
  GIT_TAG 0.12.0
  GIT_PROGRESS TRUE
  SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/range-v3
)
fetchcontent_getproperties(range-v3)
if(NOT range-v3_POPULATED)
  message(STATUS "Fetching range-v3")
  fetchcontent_populate(range-v3)
  message(STATUS "Fetching range-v3 - done")
  set(RANGEV3_SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/range-v3)
endif()

set(RANGES_BUILD_CALENDAR_EXAMPLE OFF)
set(RANGES_DEBUG_INFO OFF)
set(RANGES_ENABLE_WERROR OFF)
set(CMAKE_BUILD_TYPE "Release")
add_subdirectory(${RANGEV3_SOURCE_DIR} ${CMAKE_BINARY_DIR}/range-v3)
