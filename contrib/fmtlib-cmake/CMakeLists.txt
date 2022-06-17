fetchcontent_declare(
  fmt
  GIT_REPOSITORY https://github.com/fmtlib/fmt.git
  GIT_TAG 8.1.1
  GIT_PROGRESS TRUE
  SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/fmt
)

fetchcontent_getproperties(fmt)

if(NOT fmt_POPULATED)
  message(STATUS "Fetching fmt")
  fetchcontent_populate(fmt)
  message(STATUS "Fetching fmt - done")
  set(FMT_SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/fmt)

endif()

option(FMT_PEDANTIC "Enable extra warnings and expensive tests." OFF)
option(
  FMT_WERROR "Halt the compilation with an error on compiler warnings."
  OFF
)

include(${FMT_SOURCE_DIR}/support/cmake/cxx14.cmake)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU")
  set(
    PEDANTIC_COMPILE_FLAGS -pedantic-errors -Wall -Wextra -pedantic
    -Wold-style-cast -Wundef
    -Wredundant-decls -Wwrite-strings -Wpointer-arith
    -Wcast-qual -Wformat=2 -Wmissing-include-dirs
    -Wcast-align
    -Wctor-dtor-privacy -Wdisabled-optimization
    -Winvalid-pch -Woverloaded-virtual
    -Wconversion -Wundef
    -Wno-ctor-dtor-privacy -Wno-format-nonliteral
  )
  if(NOT CMAKE_CXX_COMPILER_VERSION VERSION_LESS 4.6)
    set(
      PEDANTIC_COMPILE_FLAGS ${PEDANTIC_COMPILE_FLAGS}
      -Wno-dangling-else -Wno-unused-local-typedefs
    )
  endif()
  if(NOT CMAKE_CXX_COMPILER_VERSION VERSION_LESS 5.0)
    set(
      PEDANTIC_COMPILE_FLAGS ${PEDANTIC_COMPILE_FLAGS} -Wdouble-promotion
      -Wtrampolines -Wzero-as-null-pointer-constant -Wuseless-cast
      -Wvector-operation-performance -Wsized-deallocation -Wshadow
    )
  endif()
  if(NOT CMAKE_CXX_COMPILER_VERSION VERSION_LESS 6.0)
    set(
      PEDANTIC_COMPILE_FLAGS ${PEDANTIC_COMPILE_FLAGS} -Wshift-overflow=2
      -Wnull-dereference -Wduplicated-cond
    )
  endif()
  set(WERROR_FLAG -Werror)
endif()

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  set(
    PEDANTIC_COMPILE_FLAGS -Wall -Wextra -pedantic -Wconversion -Wundef
    -Wdeprecated -Wweak-vtables -Wshadow
    -Wno-gnu-zero-variadic-macro-arguments
  )
  check_cxx_compiler_flag(-Wzero-as-null-pointer-constant HAS_NULLPTR_WARNING)
  if(HAS_NULLPTR_WARNING)
    set(
      PEDANTIC_COMPILE_FLAGS ${PEDANTIC_COMPILE_FLAGS}
      -Wzero-as-null-pointer-constant
    )
  endif()
  set(WERROR_FLAG -Werror)
endif()

set(
  SRCS
  ${FMT_SOURCE_DIR}/src/format.cc
  ${FMT_SOURCE_DIR}/src/os.cc
)

#add_definitions(-DFMT_USE_CONSTEXPR)

add_library(fmt STATIC ${SRCS})

if(FMT_WERROR)
  target_compile_options(fmt PRIVATE ${WERROR_FLAG})
endif()
if(FMT_PEDANTIC)
  target_compile_options(fmt PRIVATE ${PEDANTIC_COMPILE_FLAGS})
endif()

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
set(FMT ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/libfmt.a)
set(FMT_FOUND TRUE)
