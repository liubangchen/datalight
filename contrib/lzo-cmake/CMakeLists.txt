fetchcontent_declare(
  lzo
  GIT_REPOSITORY https://github.com/nemequ/lzo.git
  #GIT_TAG master
  GIT_PROGRESS TRUE
  SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/lzo
)

fetchcontent_getproperties(lzo)

if(NOT lzo_POPULATED)
  message(STATUS "Fetching lzo")
  fetchcontent_populate(lzo)
  message(STATUS "Fetching lzo - done")
  set(LZO_SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/lzo)
endif()


set(LZO_SOURCE_DIR "${PROJECT_SOURCE_DIR}/contrib/lzo2")


set(CMAKE_BUILD_TYPE "Release" CACHE STRING "" FORCE)

file(GLOB lzo_SOURCES "${LZO_SOURCE_DIR}/src/*.c")
list(SORT lzo_SOURCES)

set(lzo_USE_OBJECT_LIB 0)
include(CheckFunctionExists)
include(CheckIncludeFile)
include(CheckLibraryExists)
include(CheckSymbolExists)
include(CheckTypeSize)
include(TestBigEndian)

if(MSVC)
  # disable silly warnings about using "deprecated" POSIX functions like fopen()
  add_definitions(-D_CRT_NONSTDC_NO_DEPRECATE)
  add_definitions(-D_CRT_NONSTDC_NO_WARNINGS)
  add_definitions(-D_CRT_SECURE_NO_DEPRECATE)
  add_definitions(-D_CRT_SECURE_NO_WARNINGS)
endif()


# Checks for header files
macro(mfx_check_include_file f var)
  check_include_file("${f}" "mfx_${var}")
  if(NOT ",${mfx_${var}}," STREQUAL ",,")
    add_definitions(-D${var}=1)
    set(mfx_${var} 1)
  else()
    set(mfx_${var} 0)
  endif()
endmacro()
# mfx_ACC_CHECK_HEADERS
set(l assert.h ctype.h dirent.h errno.h fcntl.h float.h limits.h malloc.h memory.h setjmp.h signal.h stdarg.h stddef.h stdint.h stdio.h stdlib.h string.h strings.h time.h unistd.h utime.h sys/mman.h sys/resource.h sys/stat.h sys/time.h sys/types.h sys/wait.h)
foreach(f ${l})
  string(TOUPPER "${f}" var)
  string(REGEX REPLACE "[^0-9A-Z_]" "_" var "${var}")
  mfx_check_include_file("${f}" "HAVE_${var}")
endforeach()

# Checks for typedefs and structures
macro(mfx_check_type_size type var)
  check_type_size("${type}" "mfx_${var}")
  if("${mfx_${var}}" MATCHES "^[1-9][0-9]*$")
    add_definitions(-D${var}=${mfx_${var}})
  else()
    set(mfx_${var} 0)
  endif()
endmacro()
# mfx_ACC_CHECK_SIZEOF + mfx_CHECK_SIZEOF
set(l short int long "long long" __int16 __int32 __int64 "void *" size_t ptrdiff_t intmax_t uintmax_t intptr_t uintptr_t float double "long double" dev_t fpos_t mode_t off_t ssize_t time_t)
foreach(f ${l})
  string(TOUPPER "${f}" var)
  string(REGEX REPLACE " \\*" "_P" var "${var}")
  string(REGEX REPLACE "[^0-9A-Z_]" "_" var "${var}")
  mfx_check_type_size("${f}" "SIZEOF_${var}")
endforeach()

# Checks for library functions
macro(mfx_check_function_exists func var)
  check_function_exists("${func}" "mfx_${var}")
  if(NOT ",${mfx_${var}}," STREQUAL ",,")
    add_definitions(-D${var}=1)
    set(mfx_${var} 1)
  else()
    set(mfx_${var} 0)
  endif()
endmacro()
# mfx_ACC_CHECK_FUNCS
set(l access alloca atexit atoi atol chmod chown clock_getcpuclockid clock_getres clock_gettime ctime difftime fstat getenv getpagesize getrusage gettimeofday gmtime isatty localtime longjmp lstat memcmp memcpy memmove memset mkdir mktime mmap mprotect munmap qsort raise rmdir setjmp signal snprintf strcasecmp strchr strdup strerror strftime stricmp strncasecmp strnicmp strrchr strstr time umask utime vsnprintf)
foreach(f ${l})
  string(TOUPPER "${f}" var)
  string(REGEX REPLACE "[^0-9A-Z_]" "_" var "${var}")
  mfx_check_function_exists("${f}" "HAVE_${var}")
endforeach()

# mfx_LZO_CHECK_ENDIAN
test_big_endian(big_endian)
if("${big_endian}" MATCHES "^1$")
  add_definitions(-DLZO_ABI_BIG_ENDIAN=1)
elseif("${big_endian}" MATCHES "^0$")
  add_definitions(-DLZO_ABI_LITTLE_ENDIAN=1)
else()
  message(FATAL_ERROR "ERROR: TEST_BIG_ENDIAN failed with result '${big_endian}'.")
endif()

# LZO_HAVE_CONFIG_H
add_definitions(-DLZO_CFG_NO_CONFIG_HEADER=1)

# warnings
foreach(t ${lzo_COMPILE_TARGETS})
  if(CMAKE_C_COMPILER_ID MATCHES "^(Clang|GNU)$")
    target_compile_options(${t} PRIVATE -Wall -W -Wcast-qual)
  endif()
endforeach()


add_library(lzo_static_lib STATIC ${lzo_SOURCES})
target_include_directories(
  lzo_static_lib
  PUBLIC ${LZO_SOURCE_DIR} ${LZO_SOURCE_DIR}/include
)
