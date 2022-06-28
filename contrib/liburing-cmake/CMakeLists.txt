fetchcontent_declare(
  liburing
  GIT_REPOSITORY https://github.com/axboe/liburing.git
  GIT_TAG liburing-2.2
  GIT_PROGRESS TRUE
  SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/liburing
)

fetchcontent_getproperties(liburing)

if(NOT liburing_POPULATED)
  message(STATUS "Fetching liburing")
  fetchcontent_populate(liburing)
  message(STATUS "Fetching liburing - done")
  set(LIBURING_SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/liburing)
endif()

#gen compat.h file
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/compat.h.in ${LIBURING_SOURCE_DIR}/src/include/liburing/compat.h @ONLY)

file(GLOB LIBURING_SRCS ${LIBURING_SOURCE_DIR}/src/*.c)
filter_regex(EXCLUDE "nolibc\\.c" LIBURING_SRCS ${LIBURING_SRCS})

add_library(uring STATIC ${LIBURING_SRCS})
target_compile_definitions(
  uring
  PRIVATE
  -DLIBURING_INTERNAL
)
target_compile_options(
  uring
  PRIVATE
  -Wno-unused-parameter
  -Wno-sign-compare
)
target_include_directories(
  uring
  PRIVATE
  ${LIBURING_SOURCE_DIR}/src/include
)

target_include_directories(uring SYSTEM PUBLIC ${LIBURING_SOURCE_DIR}/src/include)
