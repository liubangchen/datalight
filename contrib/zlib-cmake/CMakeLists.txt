fetchcontent_declare(
  zlib
  GIT_REPOSITORY https://github.com/madler/zlib.git
  GIT_TAG v1.2.12
  GIT_PROGRESS TRUE
  SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/zlib
)

fetchcontent_getproperties(zlib)

if(NOT zlib_POPULATED)
  message(STATUS "Fetching zlib")
  fetchcontent_populate(zlib)
  message(STATUS "Fetching zlib - done")
  set(ZLIB_SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/zlib)
endif()


option(ASM686 "Enable building i686 assembly implementation")
option(AMD64 "Enable building amd64 assembly implementation")

include(CheckTypeSize)
include(CheckFunctionExists)
include(CheckIncludeFile)
include(CheckCSourceCompiles)

check_include_file(sys/types.h HAVE_SYS_TYPES_H)
check_include_file(stdint.h HAVE_STDINT_H)
check_include_file(stddef.h HAVE_STDDEF_H)
#
# Check to see if we have large file support
#
set(CMAKE_REQUIRED_DEFINITIONS -D_LARGEFILE64_SOURCE=1)
# We add these other definitions here because CheckTypeSize.cmake
# in CMake 2.4.x does not automatically do so and we want
# compatibility with CMake 2.4.x.
if(HAVE_SYS_TYPES_H)
  list(APPEND CMAKE_REQUIRED_DEFINITIONS -DHAVE_SYS_TYPES_H)
endif()
if(HAVE_STDINT_H)
  list(APPEND CMAKE_REQUIRED_DEFINITIONS -DHAVE_STDINT_H)
endif()
if(HAVE_STDDEF_H)
  list(APPEND CMAKE_REQUIRED_DEFINITIONS -DHAVE_STDDEF_H)
endif()
check_type_size(off64_t OFF64_T)
if(HAVE_OFF64_T)
  add_definitions(-D_LARGEFILE64_SOURCE=1)
endif()
set(CMAKE_REQUIRED_DEFINITIONS) # clear variable

#
# Check for fseeko
#
check_function_exists(fseeko HAVE_FSEEKO)
if(NOT HAVE_FSEEKO)
  add_definitions(-DNO_FSEEKO)
endif()

#
# Check for unistd.h
#
check_include_file(unistd.h Z_HAVE_UNISTD_H)

if(MSVC)
  set(CMAKE_DEBUG_POSTFIX "d")
  add_definitions(-D_CRT_SECURE_NO_DEPRECATE)
  add_definitions(-D_CRT_NONSTDC_NO_DEPRECATE)
  include_directories(${CMAKE_CURRENT_SOURCE_DIR})
endif()

if(NOT ZLIB_SOURCE_DIR STREQUAL CMAKE_CURRENT_BINARY_DIR)
  # If we're doing an out of source build and the user has a zconf.h
  # in their source tree...
  if(EXISTS ${ZLIB_SOURCE_DIR}/zconf.h)
    message(STATUS "Renaming")
    message(STATUS "    ${ZLIB_SOURCE_DIR}/zconf.h")
    message(STATUS "to 'zconf.h.included' because this file is included with zlib")
    message(STATUS "but CMake generates it automatically in the build directory.")
    file(RENAME ${ZLIB_SOURCE_DIR}/zconf.h ${ZLIB_SOURCE_DIR}/zconf.h.included)
  endif()
endif()


configure_file(
  ${ZLIB_SOURCE_DIR}/zconf.h.cmakein
  ${CMAKE_THIRDPARTY_HEADER_DIR}/zlib/zconf.h @ONLY
)

file(
  COPY
  ${ZLIB_SOURCE_DIR}/zlib.h
  DESTINATION ${CMAKE_THIRDPARTY_HEADER_DIR}/zlib/
)

set(
  ZLIB_PUBLIC_HDRS
  ${CMAKE_THIRDPARTY_HEADER_DIR}/zlib/zconf.h
  ${ZLIB_SOURCE_DIR}/zlib.h
)

set(
  ZLIB_PRIVATE_HDRS
  ${ZLIB_SOURCE_DIR}/crc32.h
  ${ZLIB_SOURCE_DIR}/deflate.h
  ${ZLIB_SOURCE_DIR}/gzguts.h
  ${ZLIB_SOURCE_DIR}/inffast.h
  ${ZLIB_SOURCE_DIR}/inffixed.h
  ${ZLIB_SOURCE_DIR}/inflate.h
  ${ZLIB_SOURCE_DIR}/inftrees.h
  ${ZLIB_SOURCE_DIR}/trees.h
  ${ZLIB_SOURCE_DIR}/zutil.h
)
set(
  ZLIB_SRCS
  ${ZLIB_SOURCE_DIR}/adler32.c
  ${ZLIB_SOURCE_DIR}/compress.c
  ${ZLIB_SOURCE_DIR}/crc32.c
  ${ZLIB_SOURCE_DIR}/deflate.c
  ${ZLIB_SOURCE_DIR}/gzclose.c
  ${ZLIB_SOURCE_DIR}/gzlib.c
  ${ZLIB_SOURCE_DIR}/gzread.c
  ${ZLIB_SOURCE_DIR}/gzwrite.c
  ${ZLIB_SOURCE_DIR}/inflate.c
  ${ZLIB_SOURCE_DIR}/infback.c
  ${ZLIB_SOURCE_DIR}/inftrees.c
  ${ZLIB_SOURCE_DIR}/inffast.c
  ${ZLIB_SOURCE_DIR}/trees.c
  ${ZLIB_SOURCE_DIR}/uncompr.c
  ${ZLIB_SOURCE_DIR}/zutil.c
)

if(CMAKE_COMPILER_IS_GNUCC)
  if(ASM686)
    set(ZLIB_ASMS ${ZLIB_SOURCE_DIR}/contrib/asm686/match.S)
  elseif(AMD64)
    set(ZLIB_ASMS ${ZLIB_SOURCE_DIR}/contrib/amd64/amd64-match.S)
  endif()

  if(ZLIB_ASMS)
    add_definitions(-DASMV)
    set_source_files_properties(${ZLIB_ASMS} PROPERTIES LANGUAGE C COMPILE_FLAGS -DNO_UNDERLINE)
  endif()
endif()

file(READ ${ZLIB_SOURCE_DIR}/zlib.h _zlib_h_contents)
string(
  REGEX REPLACE ".*#define[ \t]+ZLIB_VERSION[ \t]+\"([-0-9A-Za-z.]+)\".*"
  "\\1" ZLIB_FULL_VERSION ${_zlib_h_contents}
)

add_library(
  zlib
  STATIC
  ${ZLIB_SRCS}
  ${ZLIB_ASMS}
)

target_include_directories(
  zlib
  PRIVATE
  ${CMAKE_THIRDPARTY_HEADER_DIR}/zlib
)


target_include_directories(
  zlib
  SYSTEM
  PUBLIC
  ${CMAKE_THIRDPARTY_HEADER_DIR}/zlib
)
