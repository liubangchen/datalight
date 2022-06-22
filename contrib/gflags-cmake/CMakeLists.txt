fetchcontent_declare(
  gflags
  GIT_REPOSITORY https://github.com/gflags/gflags.git
  GIT_TAG v2.2.2
  GIT_PROGRESS TRUE
  SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/gflags
)

fetchcontent_getproperties(gflags)

if(NOT gflags_POPULATED)
  message(STATUS "Fetching gflags")
  fetchcontent_populate(gflags)
  message(STATUS "Fetching gflags - done")
  set(GFLAGS_SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/gflags)
endif()

if(POLICY CMP0042)
  cmake_policy(SET CMP0042 NEW)
endif()

if(POLICY CMP0048)
  cmake_policy(SET CMP0048 NEW)
endif()

include("${GFLAGS_SOURCE_DIR}/cmake/utils.cmake")

set(PACKAGE_NAME "gflags")
set(PACKAGE_VERSION "2.2.2")
set(PACKAGE_STRING "${PACKAGE_NAME} ${PACKAGE_VERSION}")
set(PACKAGE_TARNAME "${PACKAGE_NAME}-${PACKAGE_VERSION}")
set(PACKAGE_BUGREPORT "https://github.com/gflags/gflags/issues")
set(PACKAGE_DESCRIPTION "A commandline flags library that allows for distributed flags.")
set(PACKAGE_URL "http://gflags.github.io/gflags")

if(CMAKE_VERSION VERSION_LESS 3.4)
  enable_language(C)
endif()

set(GFLAGS_IS_SUBPROJECT TRUE)
set(BUILD_STATIC_LIBS ON)
#set(TYPES uint32_t u_int32_t)
set(GFLAGS_INTTYPES_FORMAT_C99 0)
set(GFLAGS_INTTYPES_FORMAT_BSD 0)
set(GFLAGS_INTTYPES_FORMAT_VC7 0)
version_numbers(
  ${PACKAGE_VERSION}
  PACKAGE_VERSION_MAJOR
  PACKAGE_VERSION_MINOR
  PACKAGE_VERSION_PATCH
)

if(GFLAGS_SOVERSION)
  set(PACKAGE_SOVERSION "${GFLAGS_SOVERSION}")
else()
  set(PACKAGE_SOVERSION "${PACKAGE_VERSION_MAJOR}.${PACKAGE_VERSION_MINOR}")
endif()

if(NOT DEFINED GFLAGS_IS_SUBPROJECT)
  if("^${CMAKE_SOURCE_DIR}$" STREQUAL "^${PROJECT_SOURCE_DIR}$")
    set(GFLAGS_IS_SUBPROJECT FALSE)
  else()
    set(GFLAGS_IS_SUBPROJECT TRUE)
  endif()
endif()


# prefix for package variables in CMake configuration file
string(TOUPPER "${PACKAGE_NAME}" PACKAGE_PREFIX)

# convert file path on Windows with back slashes to path with forward slashes
# otherwise this causes an issue with the cmake_install.cmake script
file(TO_CMAKE_PATH "${CMAKE_INSTALL_PREFIX}" CMAKE_INSTALL_PREFIX)

# ----------------------------------------------------------------------------
# options

# maintain binary backwards compatibility with gflags library version <= 2.0,
# but at the same time enable the use of the preferred new "gflags" namespace
gflags_define(STRING NAMESPACE "Name(s) of library namespace (separate multiple options by semicolon)" "google;${PACKAGE_NAME}" "${PACKAGE_NAME}")
gflags_property(NAMESPACE ADVANCED TRUE)
set(GFLAGS_NAMESPACE_SECONDARY "${NAMESPACE}")
list(REMOVE_DUPLICATES GFLAGS_NAMESPACE_SECONDARY)
if(NOT GFLAGS_NAMESPACE_SECONDARY)
  message(FATAL_ERROR "GFLAGS_NAMESPACE must be set to one (or more) valid C++ namespace identifier(s separated by semicolon \";\").")
endif()
foreach(ns IN LISTS GFLAGS_NAMESPACE_SECONDARY)
  if(NOT ns MATCHES "^[a-zA-Z][a-zA-Z0-9_]*$")
    message(FATAL_ERROR "GFLAGS_NAMESPACE contains invalid namespace identifier: ${ns}")
  endif()
endforeach()
list(GET GFLAGS_NAMESPACE_SECONDARY 0 GFLAGS_NAMESPACE)
list(REMOVE_AT GFLAGS_NAMESPACE_SECONDARY 0)

# cached build options when gflags is not a subproject, otherwise non-cached CMake variables
# usage: gflags_define(BOOL <name> <doc> <default> [<subproject default>])
gflags_define(BOOL BUILD_SHARED_LIBS "Request build of shared libraries." OFF OFF)
gflags_define(BOOL BUILD_STATIC_LIBS "Request build of static libraries (default if BUILD_SHARED_LIBS is OFF)." OFF ON)
gflags_define(BOOL BUILD_gflags_LIB "Request build of the multi-threaded gflags library." ON ON)
gflags_define(BOOL BUILD_gflags_nothreads_LIB "Request build of the single-threaded gflags library." ON OFF)
gflags_define(BOOL BUILD_PACKAGING "Enable build of distribution packages using CPack." OFF OFF)
gflags_define(BOOL BUILD_TESTING "Enable build of the unit tests and their execution using CTest." OFF OFF)
gflags_define(BOOL INSTALL_HEADERS "Request installation of headers and other development files." ON OFF)
gflags_define(BOOL INSTALL_SHARED_LIBS "Request installation of shared libraries." ON OFF)
gflags_define(BOOL INSTALL_STATIC_LIBS "Request installation of static libraries." ON OFF)
gflags_define(BOOL REGISTER_BUILD_DIR "Request entry of build directory in CMake's package registry." OFF OFF)
gflags_define(BOOL REGISTER_INSTALL_PREFIX "Request entry of installed package in CMake's package registry." ON OFF)

gflags_property(BUILD_STATIC_LIBS ADVANCED TRUE)
gflags_property(INSTALL_HEADERS ADVANCED TRUE)
gflags_property(INSTALL_SHARED_LIBS ADVANCED TRUE)
gflags_property(INSTALL_STATIC_LIBS ADVANCED TRUE)

if(NOT GFLAGS_IS_SUBPROJECT)
  foreach(varname IN ITEMS CMAKE_INSTALL_PREFIX)
    gflags_property(${varname} ADVANCED FALSE)
  endforeach()
  foreach(varname IN ITEMS CMAKE_CONFIGURATION_TYPES CMAKE_OSX_ARCHITECTURES CMAKE_OSX_DEPLOYMENT_TARGET CMAKE_OSX_SYSROOT)
    gflags_property(${varname} ADVANCED TRUE)
  endforeach()
  if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CXX_FLAGS)
    gflags_set(CMAKE_BUILD_TYPE Release)
  endif()
  if(CMAKE_CONFIGURATION_TYPES)
    gflags_property(CMAKE_BUILD_TYPE STRINGS "${CMAKE_CONFIGURATION_TYPES}")
  endif()
endif() # NOT GFLAGS_IS_SUBPROJECT

if(NOT BUILD_SHARED_LIBS AND NOT BUILD_STATIC_LIBS)
  set(BUILD_STATIC_LIBS ON)
endif()
if(NOT BUILD_gflags_LIB AND NOT BUILD_gflags_nothreads_LIB)
  message(FATAL_ERROR "At least one of [GFLAGS_]BUILD_gflags_LIB and [GFLAGS_]BUILD_gflags_nothreads_LIB must be ON.")
endif()

gflags_define(STRING INCLUDE_DIR "Name of include directory of installed header files relative to CMAKE_INSTALL_PREFIX/include/" "${PACKAGE_NAME}")
gflags_property(INCLUDE_DIR ADVANCED TRUE)
file(TO_CMAKE_PATH "${INCLUDE_DIR}" INCLUDE_DIR)
if(IS_ABSOLUTE INCLUDE_DIR)
  message(FATAL_ERROR "[GFLAGS_]INCLUDE_DIR must be a path relative to CMAKE_INSTALL_PREFIX/include/")
endif()
if(INCLUDE_DIR MATCHES "^\\.\\.[/\\]")
  message(FATAL_ERROR "[GFLAGS_]INCLUDE_DIR must not start with parent directory reference (../)")
endif()
set(GFLAGS_INCLUDE_DIR "${INCLUDE_DIR}")

# ----------------------------------------------------------------------------
# system checks
include(CheckTypeSize)
include(CheckIncludeFileCXX)
include(CheckCXXSymbolExists)

if(WIN32 AND NOT CYGWIN)
  set(OS_WINDOWS 1)
else()
  set(OS_WINDOWS 0)
endif()

if(MSVC)
  set(HAVE_SYS_TYPES_H 1)
  set(HAVE_STDDEF_H 1) # used by CheckTypeSize module
  set(HAVE_UNISTD_H 0)
  set(HAVE_SYS_STAT_H 1)
  set(HAVE_SHLWAPI_H 1)
  if(MSVC_VERSION VERSION_LESS 1600)
    check_include_file_cxx("stdint.h" HAVE_STDINT_H)
    bool_to_int(HAVE_STDINT_H)  # used in #if directive
  else()
    set(HAVE_STDINT_H 1)
  endif()
  if(MSVC_VERSION VERSION_LESS 1800)
    check_include_file_cxx("inttypes.h" HAVE_INTTYPES_H)
    bool_to_int(HAVE_INTTYPES_H)  # used in #if directive
  else()
    set(HAVE_INTTYPES_H 1)
  endif()
else()
  foreach(fname IN ITEMS unistd stdint inttypes sys/types sys/stat fnmatch)
    string(TOUPPER "${fname}" FNAME)
    string(REPLACE "/" "_" FNAME "${FNAME}")
    if(NOT HAVE_${FNAME}_H)
      check_include_file_cxx("${fname}.h" HAVE_${FNAME}_H)
    endif()
  endforeach()
  if(NOT HAVE_FNMATCH_H AND OS_WINDOWS)
    check_include_file_cxx("shlwapi.h" HAVE_SHLWAPI_H)
  endif()
  # the following are used in #if directives not #ifdef
  bool_to_int(HAVE_STDINT_H)
  bool_to_int(HAVE_SYS_TYPES_H)
  bool_to_int(HAVE_INTTYPES_H)
endif()

gflags_define(STRING INTTYPES_FORMAT "Format of integer types: \"C99\" (uint32_t), \"BSD\" (u_int32_t), \"VC7\" (__int32)" "")
gflags_property(INTTYPES_FORMAT STRINGS "C99;BSD;VC7")
gflags_property(INTTYPES_FORMAT ADVANCED TRUE)
if(NOT INTTYPES_FORMAT)
  set(TYPES uint32_t u_int32_t)
  if(MSVC)
    list(INSERT TYPES 0 __int32)
  endif()
  foreach(type IN LISTS TYPES)
    check_type_size(${type} ${type} LANGUAGE CXX)
    if(HAVE_${type})
      break()
    endif()
  endforeach()
  if(HAVE_uint32_t)
    gflags_set(INTTYPES_FORMAT C99)
  elseif(HAVE_u_int32_t)
    gflags_set(INTTYPES_FORMAT BSD)
  elseif(HAVE___int32)
    gflags_set(INTTYPES_FORMAT VC7)
  else()
    gflags_property(INTTYPES_FORMAT ADVANCED FALSE)
    message(
      FATAL_ERROR "Do not know how to define a 32-bit integer quantity on your system!"
      " Neither uint32_t, u_int32_t, nor __int32 seem to be available."
      " Set [GFLAGS_]INTTYPES_FORMAT to either C99, BSD, or VC7 and try again."
    )
  endif()
endif()
# use of special characters in strings to circumvent bug #0008226
if("^${INTTYPES_FORMAT}$" STREQUAL "^WIN$")
  gflags_set(INTTYPES_FORMAT VC7)
endif()
if(NOT INTTYPES_FORMAT MATCHES "^(C99|BSD|VC7)$")
  message(FATAL_ERROR "Invalid value for [GFLAGS_]INTTYPES_FORMAT! Choose one of \"C99\", \"BSD\", or \"VC7\"")
endif()
set(GFLAGS_INTTYPES_FORMAT "${INTTYPES_FORMAT}")
set(GFLAGS_INTTYPES_FORMAT_C99 0)
set(GFLAGS_INTTYPES_FORMAT_BSD 0)
set(GFLAGS_INTTYPES_FORMAT_VC7 0)
set("GFLAGS_INTTYPES_FORMAT_${INTTYPES_FORMAT}" 1)

if(MSVC)
  set(HAVE_strtoll 0)
  set(HAVE_strtoq 0)
else()
  check_cxx_symbol_exists(strtoll stdlib.h HAVE_STRTOLL)
  if(NOT HAVE_STRTOLL)
    check_cxx_symbol_exists(strtoq stdlib.h HAVE_STRTOQ)
  endif()
endif()

if(BUILD_gflags_LIB)
  set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
  find_package(Threads)
  if(Threads_FOUND AND CMAKE_USE_PTHREADS_INIT)
    set(HAVE_PTHREAD 1)
    check_type_size(pthread_rwlock_t RWLOCK LANGUAGE CXX)
  else()
    set(HAVE_PTHREAD 0)
  endif()
  if(UNIX AND NOT HAVE_PTHREAD)
    if(CMAKE_HAVE_PTHREAD_H)
      set(what "library")
    else()
      set(what ".h file")
    endif()
    message(
      FATAL_ERROR "Could not find pthread${what}. Check the log file"
      "\n\t${CMAKE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/CMakeError.log"
      "\nor disable the build of the multi-threaded gflags library (BUILD_gflags_LIB=OFF)."
    )
  endif()
else()
  set(HAVE_PTHREAD 0)
endif()

if(GFLAGS_NAMESPACE_SECONDARY)
  set(INCLUDE_GFLAGS_NS_H "// Import gflags library symbols into alternative/deprecated namespace(s)")
  foreach(ns IN LISTS GFLAGS_NAMESPACE_SECONDARY)
    string(TOUPPER "${ns}" NS)
    set(gflags_ns_h "${PROJECT_BINARY_DIR}/include/${GFLAGS_INCLUDE_DIR}/gflags_${ns}.h")
    configure_file("${PROJECT_SOURCE_DIR}/src/gflags_ns.h.in" "${gflags_ns_h}" @ONLY)
    list(APPEND PUBLIC_HDRS "${gflags_ns_h}")
    set(INCLUDE_GFLAGS_NS_H "${INCLUDE_GFLAGS_NS_H}\n#include \"gflags_${ns}.h\"")
  endforeach()
else()
  set(INCLUDE_GFLAGS_NS_H)
endif()

if(NOT DEFINED GFLAGS_ATTRIBUTE_UNUSED)
  if(CMAKE_COMPILER_IS_GNUCXX)
    set(GFLAGS_ATTRIBUTE_UNUSED "__attribute((unused))")
  else()
    set(GFLAGS_ATTRIBUTE_UNUSED)
  endif()
endif()

if(NOT DEFINED GFLAGS_IS_A_DLL)
  if(BUILD_SHARED_LIBS)
    set(GFLAGS_IS_A_DLL 1)
  else()
    set(GFLAGS_IS_A_DLL 0)
  endif()
endif()

#######
set(
  GFLAGS_GFLAGS_SRCS
  "${GFLAGS_SOURCE_DIR}/src/gflags.cc"
  "${GFLAGS_SOURCE_DIR}/src/gflags_reporting.cc"
  "${GFLAGS_SOURCE_DIR}/src/gflags_completions.cc"
)

configure_file("${GFLAGS_SOURCE_DIR}/src/defines.h.in" ${CMAKE_THIRDPARTY_HEADER_DIR}/gflags/defines.h @ONLY)
configure_file("${GFLAGS_SOURCE_DIR}/src/gflags.h.in" ${CMAKE_THIRDPARTY_HEADER_DIR}/gflags/gflags.h @ONLY)
configure_file("${GFLAGS_SOURCE_DIR}/src/gflags_declare.h.in" ${CMAKE_THIRDPARTY_HEADER_DIR}/gflags/gflags_declare.h @ONLY)
configure_file("${GFLAGS_SOURCE_DIR}/src/gflags_completions.h.in" ${CMAKE_THIRDPARTY_HEADER_DIR}/gflags/gflags_completions.h @ONLY)
configure_file("${GFLAGS_SOURCE_DIR}/src/gflags_ns.h.in" ${CMAKE_THIRDPARTY_HEADER_DIR}/gflags/gflags_ns.h @ONLY)


add_library(gflags STATIC ${GFLAGS_GFLAGS_SRCS})
target_include_directories(
  gflags
  PRIVATE
  ${CMAKE_THIRDPARTY_HEADER_DIR}/gflags
)

set(GFLAGS_LIBRARIES "gflags")
set(gflags_LIBRARIES ${GFLAGS_LIBRARIES})

target_include_directories(
  gflags
  SYSTEM
  PUBLIC
  ${CMAKE_THIRDPARTY_HEADER_DIR}/
)

set(GFLAGS_INCLUDE_DIR ${CMAKE_THIRDPARTY_HEADER_DIR}/)
set(GFLAGS_LIBRARY ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/libgflags.a)
set(GFLAGS_FOUND TRUE)
