fetchcontent_declare(
  fmt
  GIT_REPOSITORY https://github.com/fmtlib/fmt.git
  GIT_TAG 8.1.1
  SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/fmt
)

fetchcontent_getproperties(fmt)

if(NOT fmt_POPULATED)
  message(STATUS "Fetching fmt")
  fetchcontent_populate(fmt)
  message(STATUS "Fetching fmt - done")
  set(FMT_SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/fmt)

endif()

set(
  SRCS
  ${FMT_SOURCE_DIR}/src/format.cc
  ${FMT_SOURCE_DIR}/src/os.cc
  
  ${FMT_SOURCE_DIR}/include/fmt/args.h
  ${FMT_SOURCE_DIR}/include/fmt/chrono.h
  ${FMT_SOURCE_DIR}/include/fmt/color.h
  ${FMT_SOURCE_DIR}/include/fmt/compile.h
  ${FMT_SOURCE_DIR}/include/fmt/core.h
  ${FMT_SOURCE_DIR}/include/fmt/format.h
  ${FMT_SOURCE_DIR}/include/fmt/format-inl.h
  ${FMT_SOURCE_DIR}/include/fmt/locale.h
  ${FMT_SOURCE_DIR}/include/fmt/os.h
  ${FMT_SOURCE_DIR}/include/fmt/ostream.h
  ${FMT_SOURCE_DIR}/include/fmt/printf.h
  ${FMT_SOURCE_DIR}/include/fmt/ranges.h
  ${FMT_SOURCE_DIR}/include/fmt/xchar.h
)

add_library(fmt STATIC ${SRCS})
target_include_directories(
  fmt
  PRIVATE
  ${FMT_SOURCE_DIR}/include
)

add_library(fmt::fmt ALIAS fmt)
file(COPY ${FMT_SOURCE_DIR}/include/ DESTINATION ${CMAKE_THIRDPARTY_HEADER_DIR}/fmt)
target_include_directories(fmt SYSTEM PUBLIC ${CMAKE_THIRDPARTY_HEADER_DIR}/fmt)

set(FMT_INCLUDE_DIR ${CMAKE_THIRDPARTY_HEADER_DIR}/fmt)
set(FMT_LIBRARY ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/libfmt.a)
set(FMT_FOUND TRUE)
