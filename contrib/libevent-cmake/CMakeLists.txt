fetchcontent_declare(
  libevent
  GIT_REPOSITORY https://github.com/libevent/libevent.git
  GIT_TAG release-2.1.12-stable
  GIT_PROGRESS TRUE
  SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/libevent
)
fetchcontent_getproperties(libevent)
if(NOT libevent_POPULATED)
  message(STATUS "Fetching libevent")
  fetchcontent_populate(libevent)
  message(STATUS "Fetching libevent - done")
  set(LIBEVENT_SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/libevent)
endif()
message("libevent lib path: ${LIBEVENT_SOURCE_DIR}")

if(POLICY CMP0054)
  cmake_policy(SET CMP0054 NEW)
endif()
if(POLICY CMP0074)
  cmake_policy(SET CMP0074 NEW)
endif()
if(POLICY CMP0075)
  cmake_policy(SET CMP0075 NEW)
endif()

set(CMAKE_BUILD_TYPE Release)

string(TOLOWER "${CMAKE_BUILD_TYPE}" CMAKE_BUILD_TYPE_LOWER)

# get rid of the extra default configurations
# what? why would you get id of other useful build types? - Ellzey
set(CMAKE_CONFIGURATION_TYPES "Debug;Release" CACHE STRING "Limited configurations" FORCE)

set(
  EVENT__LIBRARY_TYPE DEFAULT CACHE STRING
  "Set library type to SHARED/STATIC/BOTH (default SHARED for MSVC, otherwise BOTH)"
)

list(APPEND CMAKE_MODULE_PATH "${LIBEVENT_SOURCE_DIR}/cmake")
string(REGEX MATCH "SunOS" SOLARIS "${CMAKE_SYSTEM_NAME}")


include(CheckTypeSize)
include(CheckFileOffsetBits)
include(Macros)
include(CheckVariableExists)
include(CheckSymbolExists)
include(CheckStructHasMember)
include(CheckCSourceCompiles)
include(CheckPrototypeDefinition)
include(CheckFunctionKeywords)
include(CheckConstExists)
include(AddCompilerFlags)
include(VersionViaGit)

event_fuzzy_version_from_git()

set(EVENT_VERSION_MAJOR ${EVENT_GIT___VERSION_MAJOR})
set(EVENT_VERSION_MINOR ${EVENT_GIT___VERSION_MINOR})
set(EVENT_VERSION_PATCH ${EVENT_GIT___VERSION_PATCH})
set(EVENT_VERSION_STAGE ${EVENT_GIT___VERSION_STAGE})


set(EVENT_ABI_MAJOR ${EVENT_VERSION_MAJOR})
set(EVENT_ABI_MINOR ${EVENT_VERSION_MINOR})
set(EVENT_ABI_PATCH ${EVENT_VERSION_PATCH})

set(
  EVENT_ABI_LIBVERSION
  "${EVENT_ABI_MAJOR}.${EVENT_ABI_MINOR}.${EVENT_ABI_PATCH}"
)

set(
  EVENT_PACKAGE_VERSION
  "${EVENT_VERSION_MAJOR}.${EVENT_VERSION_MINOR}.${EVENT_VERSION_PATCH}"
)

set(EVENT_NUMERIC_VERSION 0x02010c00)
# equals to VERSION_INFO in Makefile.am
set(EVENT_ABI_LIBVERSION_CURRENT 7)
set(EVENT_ABI_LIBVERSION_REVISION 1)
set(EVENT_ABI_LIBVERSION_AGE 0)

# equals to RELEASE in Makefile.am
set(EVENT_PACKAGE_RELEASE 2.1)

# only a subset of names can be used, defaults to "beta"
set(EVENT_STAGE_NAME ${EVENT_VERSION_STAGE})


# only a subset of names can be used, defaults to "beta"
set(EVENT_STAGE_NAME ${EVENT_VERSION_STAGE})

# a list that defines what can set for EVENT_STAGE_VERSION
set(
  EVENT__ALLOWED_STAGE_NAMES
  rc
  beta
  alpha
  alpha-dev
  release
  stable
)

list(
  FIND EVENT__ALLOWED_STAGE_NAMES
  "${EVENT_STAGE_NAME}"
  EVENT__STAGE_RET
)

if(EVENT__STAGE_RET EQUAL -1)
  message(
    WARNING
    "stage ${EVENT_STAGE_NAME} is not allowed, reset to beta"
  )
  set(EVENT_STAGE_NAME beta)
endif()

set(
  EVENT_VERSION
  "${EVENT_VERSION_MAJOR}.${EVENT_VERSION_MINOR}.${EVENT_VERSION_PATCH}-${EVENT_STAGE_NAME}"
)

option(
  EVENT__DISABLE_DEBUG_MODE
  "Define if libevent should build without support for a debug mode" OFF
)

option(
  EVENT__ENABLE_VERBOSE_DEBUG
  "Enables verbose debugging" OFF
)

option(
  EVENT__DISABLE_MM_REPLACEMENT
  "Define if libevent should not allow replacing the mm functions" OFF
)

option(
  EVENT__DISABLE_THREAD_SUPPORT
  "Define if libevent should not be compiled with thread support" OFF
)

option(
  EVENT__DISABLE_OPENSSL
  "Define if libevent should build without support for OpenSSL encryption" OFF
)

option(
  EVENT__DISABLE_BENCHMARK
  "Defines if libevent should build without the benchmark executables" OFF
)

option(
  EVENT__DISABLE_TESTS
  "If tests should be compiled or not" ON
)

option(
  EVENT__DISABLE_REGRESS
  "Disable the regress tests" OFF
)

option(
  EVENT__DISABLE_SAMPLES
  "Disable sample files" OFF
)

option(
  EVENT__DISABLE_CLOCK_GETTIME
  "Do not use clock_gettime even if it is available" OFF
)

option(
  EVENT__FORCE_KQUEUE_CHECK
  "When crosscompiling forces running a test program that verifies that Kqueue works with pipes. Note that this requires you to manually run the test program on the cross compilation target to verify that it works. See cmake documentation for try_run for more details" OFF
)

# TODO: Add --disable-largefile     omit support for large files
option(
  EVENT__COVERAGE
  "Enable running gcov to get a test coverage report (only works with GCC/CLang). Make sure to enable -DCMAKE_BUILD_TYPE=Debug as well." OFF
)

# Put the libaries and binaries that get built into directories at the
# top of the build tree rather than in hard-to-find leaf directories.
#
# But only if this variables are not defined yet
# (i.e. libevent is used via add_subdirectory())
if(NOT DEFINED CMAKE_RUNTIME_OUTPUT_DIRECTORY)
  set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/bin)
endif()
if(NOT DEFINED CMAKE_LIBRARY_OUTPUT_DIRECTORY)
  set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)
endif()
if(NOT DEFINED CMAKE_ARCHIVE_OUTPUT_DIRECTORY)
  set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)
endif()

if(EVENT__ENABLE_VERBOSE_DEBUG)
  add_definitions(-DUSE_DEBUG=1)
endif()

# make it colorful under ninja-build
if("${CMAKE_GENERATOR}" STREQUAL "Ninja")
  add_compiler_flags(-fdiagnostics-color=always)
endif()

# Setup compiler flags for coverage.
if(EVENT__COVERAGE)
  if(NOT "${CMAKE_BUILD_TYPE_LOWER}" STREQUAL "debug")
    message(FATAL_ERROR "Coverage requires -DCMAKE_BUILD_TYPE=Debug")
  endif()

  message(STATUS "Setting coverage compiler flags")

  set(CMAKE_REQUIRED_LIBRARIES "--coverage")
  add_compiler_flags(-g -O0 --coverage)
  set(CMAKE_REQUIRED_LIBRARIES "")
endif()

set(GNUC 0)
set(CLANG 0)
set(MSVC 0)
if(
  ("${CMAKE_C_COMPILER_ID}" STREQUAL "Clang") OR
  ("${CMAKE_C_COMPILER_ID}" STREQUAL "AppleClang")
)
  set(CLANG 1)
endif()
if(("${CMAKE_C_COMPILER_ID}" STREQUAL "GNU") OR (${CLANG}))
  set(GNUC 1)
endif()
if(("${CMAKE_C_COMPILER_ID}" STREQUAL "MSVC") OR (${CLANG}))
  set(MSVC 1)
endif()

# Detect library type
set(EVENT_LIBRARY_TYPE "STATIC")
if("${EVENT__LIBRARY_TYPE}" STREQUAL "DEFAULT")
  if(${MSVC})
    set(EVENT_LIBRARY_TYPE SHARED)
  else()
    set(EVENT_LIBRARY_TYPE BOTH)
  endif()
else()
  string(TOUPPER "${EVENT__LIBRARY_TYPE}" EVENT_LIBRARY_TYPE)
endif()
if((${MSVC}) AND ("${EVENT_LIBRARY_TYPE}" STREQUAL "BOTH"))
  message(
    WARNING
    "Building SHARED and STATIC is not supported for MSVC "
    "(due to conflicts in library name"
    " between STATIC library and IMPORTED library for SHARED libraries)"
  )
endif()
set(EVENT_LIBRARY_STATIC ON)
set(EVENT_LIBRARY_SHARED OFF)
if("${EVENT_LIBRARY_TYPE}" STREQUAL "BOTH")
  set(EVENT_LIBRARY_STATIC ON)
  set(EVENT_LIBRARY_SHARED ON)
elseif("${EVENT_LIBRARY_TYPE}" STREQUAL "STATIC")
  set(EVENT_LIBRARY_STATIC ON)
elseif("${EVENT_LIBRARY_TYPE}" STREQUAL "SHARED")
  set(EVENT_LIBRARY_SHARED ON)
else()
  message(FATAL_ERROR "${EVENT_LIBRARY_TYPE} is not supported")
endif()

if(${MSVC})
  set(msvc_static_runtime OFF)
  if("${EVENT_LIBRARY_TYPE}" STREQUAL "STATIC")
    set(msvc_static_runtime ON)
  endif()

  # For more info:
  # - https://docs.microsoft.com/en-us/cpp/build/reference/md-mt-ld-use-run-time-library?view=vs-2017
  # - https://gitlab.kitware.com/cmake/community/wikis/FAQ#how-can-i-build-my-msvc-application-with-a-static-runtime
  option(
    EVENT__MSVC_STATIC_RUNTIME
    "Link static runtime libraries"
    ${msvc_static_runtime}
  )

  if(EVENT__MSVC_STATIC_RUNTIME)
    foreach(
      flag_var
      CMAKE_C_FLAGS_DEBUG
      CMAKE_C_FLAGS_RELEASE
      CMAKE_C_FLAGS_MINSIZEREL
      CMAKE_C_FLAGS_RELWITHDEBINFO
    )
      if(${flag_var} MATCHES "/MD")
        string(REGEX REPLACE "/MD" "/MT" ${flag_var} "${${flag_var}}")
      endif()
    endforeach()
  endif()
endif()

# GNUC specific options.
if(${GNUC})
  option(EVENT__DISABLE_GCC_WARNINGS "Disable verbose warnings with GCC" OFF)
  option(EVENT__ENABLE_GCC_HARDENING "Enable compiler security checks" OFF)
  option(EVENT__ENABLE_GCC_FUNCTION_SECTIONS "Enable gcc function sections" OFF)
  option(EVENT__ENABLE_GCC_WARNINGS "Make all GCC warnings into errors" OFF)

  set(GCC_V ${CMAKE_C_COMPILER_VERSION})

  list(
    APPEND __FLAGS
    -Wall -Wextra -Wno-unused-parameter -Wstrict-aliasing -Wstrict-prototypes
    -fno-strict-aliasing # gcc 2.9.5+
    -Wmissing-prototypes
    # gcc 4
    -Winit-self
    -Wmissing-field-initializers
    -Wdeclaration-after-statement
    # gcc 4.2
    -Waddress
    -Wnormalized=id
    -Woverride-init
    # gcc 4.5
    -Wlogical-op
    -Wwrite-strings
  )

  if(${CLANG})
    list(APPEND __FLAGS -Wno-unused-function)
  endif()

  if(EVENT__DISABLE_GCC_WARNINGS)
    list(APPEND __FLAGS -w)
  endif()

  if(EVENT__ENABLE_GCC_HARDENING)
    list(
      APPEND __FLAGS
      -fstack-protector-all
      -fwrapv
      -fPIE
      -Wstack-protector
      "--param ssp-buffer-size=1"
    )
    add_definitions(-D_FORTIFY_SOURCE=2)
  endif()

  if(EVENT__ENABLE_GCC_FUNCTION_SECTIONS)
    list(APPEND __FLAGS -ffunction-sections)
    # TODO: Add --gc-sections support. We need some checks for NetBSD to ensure this works.
  endif()

  if(EVENT__ENABLE_GCC_WARNINGS)
    list(APPEND __FLAGS -Werror)
  endif()

  add_compiler_flags(${__FLAGS})
endif()

if(APPLE)
  # Get rid of deprecated warnings for OpenSSL on OSX 10.7 and greater.
  add_compiler_flags(
    -Wno-error=deprecated-declarations
    -Qunused-arguments
  )
endif()

if(MINGW OR CYGWIN)
  set(WIN32 TRUE)
endif()

# Winsock.
if(WIN32)
  set(CMAKE_REQUIRED_LIBRARIES ws2_32 shell32 advapi32)
  set(CMAKE_REQUIRED_DEFINITIONS -FIwinsock2.h -FIws2tcpip.h -D_WIN32_WINNT=0x0600)
endif()
if(SOLARIS)
  set(CMAKE_REQUIRED_LIBRARIES socket nsl)
endif()

# Check if _GNU_SOURCE is available.
if(NOT DEFINED _GNU_SOURCE)
  check_symbol_exists(__GNU_LIBRARY__ "features.h" _GNU_SOURCE)

  if(NOT _GNU_SOURCE)
    unset(_GNU_SOURCE CACHE)
    check_symbol_exists(_GNU_SOURCE "features.h" _GNU_SOURCE)
  endif()

  if(ANDROID)
    set(_GNU_SOURCE TRUE)
  endif()
endif()

if(_GNU_SOURCE)
  add_definitions(-D_GNU_SOURCE=1)
  set(CMAKE_REQUIRED_DEFINITIONS -D_GNU_SOURCE)
endif()

# Check if header files exist
list(
  APPEND FILES_TO_CHECK
  fcntl.h
  inttypes.h
  memory.h
  signal.h
  stdarg.h
  stddef.h
  stdint.h
  stdlib.h
  string.h
  errno.h
  unistd.h
  time.h
  sys/types.h
  sys/stat.h
  sys/time.h
  sys/param.h
)
if(WIN32)
  list(
    APPEND FILES_TO_CHECK
    io.h
    winsock2.h
    ws2tcpip.h
    afunix.h
  )
else()
  list(
    APPEND FILES_TO_CHECK
    netdb.h
    dlfcn.h
    arpa/inet.h
    poll.h
    port.h
    sys/socket.h
    sys/random.h
    sys/un.h
    sys/devpoll.h
    sys/epoll.h
    sys/eventfd.h
    sys/event.h
    sys/ioctl.h
    sys/mman.h
    sys/queue.h
    sys/select.h
    sys/sendfile.h
    sys/uio.h
    sys/wait.h
    sys/resource.h
    sys/timerfd.h
    netinet/in.h
    netinet/in6.h
    netinet/tcp.h
    ifaddrs.h
  )
endif()

if(NOT "${CMAKE_HOST_SYSTEM_NAME}" STREQUAL "Linux")
  list(APPEND FILES_TO_CHECK sys/sysctl.h)
endif()

if(APPLE)
  list(
    APPEND FILES_TO_CHECK
    mach/mach_time.h
    mach/mach.h
  )
endif()

foreach(FILE ${FILES_TO_CHECK})
  check_include_file_concat(${FILE} "EVENT")
endforeach()
unset(FILES_TO_CHECK)

# Check if functions exist
list(
  APPEND SYMBOLS_TO_CHECK
  getaddrinfo
  getnameinfo
  getprotobynumber
  getservbyname
  gethostbyname
  inet_ntop
  inet_pton
  gettimeofday
  signal
  strtoll
  splice
  strlcpy
  strsep
  strtok_r
  vasprintf
  timerclear
  timercmp
  timerisset
  timeradd
  nanosleep
  putenv
  umask
)
if(NOT EVENT__DISABLE_CLOCK_GETTIME)
  list(APPEND SYMBOLS_TO_CHECK clock_gettime)
endif()

if(WIN32)
  list(
    APPEND SYMBOLS_TO_CHECK
    _gmtime64_s
    _gmtime64
  )
else()
  list(
    APPEND SYMBOLS_TO_CHECK
    getifaddrs
    select
    epoll_create
    epoll_create1
    epoll_ctl
    eventfd
    poll
    port_create
    kqueue
    fcntl
    mmap
    pipe
    pipe2
    sendfile
    sigaction
    strsignal
    sysctl
    accept4
    arc4random
    arc4random_buf
    arc4random_addrandom
    getrandom
    getegid
    geteuid
    issetugid
    usleep
    timerfd_create
    setenv
    unsetenv
    setrlimit
    gethostbyname_r
  )
  if(APPLE)
    list(APPEND SYMBOLS_TO_CHECK mach_absolute_time)
  endif()
endif()

# Add stdio.h for vasprintf
set(EVENT_INCLUDES ${EVENT_INCLUDES} stdio.h)
check_symbols_exist("${SYMBOLS_TO_CHECK}" "${EVENT_INCLUDES}" "EVENT")
unset(SYMBOLS_TO_CHECK)
set(EVENT__HAVE_EPOLL ${EVENT__HAVE_EPOLL_CREATE})

# Get the gethostbyname_r prototype.
if(EVENT__HAVE_GETHOSTBYNAME_R)
  check_prototype_definition(
    gethostbyname_r
    "int gethostbyname_r(const char *name, struct hostent *hp, struct hostent_data *hdata)"
    "0"
    "netdb.h"
    EVENT__HAVE_GETHOSTBYNAME_R_3_ARG
  )

  check_prototype_definition(
    gethostbyname_r
    "struct hostent *gethostbyname_r(const char *name, struct hostent *hp, char *buf, size_t buflen, int *herr)"
    "NULL"
    "netdb.h"
    EVENT__HAVE_GETHOSTBYNAME_R_5_ARG
  )

  check_prototype_definition(
    gethostbyname_r
    "int gethostbyname_r(const char *name, struct hostent *hp, char *buf, size_t buflen, struct hostent **result, int *herr)"
    "0"
    "netdb.h"
    EVENT__HAVE_GETHOSTBYNAME_R_6_ARG
  )
endif()

if(HAVE_PORT_H AND HAVE_PORT_CREATE)
  set(EVENT__HAVE_EVENT_PORTS 1)
endif()

# Only `CHECK_TYPE_SIZE()' will use `CMAKE_EXTRA_INCLUDE_FILES'
set(CMAKE_EXTRA_INCLUDE_FILES ${EVENT_INCLUDES})

check_type_size("struct sockaddr_un" EVENT__HAVE_STRUCT_SOCKADDR_UN)
check_type_size("uint8_t" EVENT__HAVE_UINT8_T)
check_type_size("uint16_t" EVENT__HAVE_UINT16_T)
check_type_size("uint32_t" EVENT__HAVE_UINT32_T)
check_type_size("uint64_t" EVENT__HAVE_UINT64_T)
check_type_size("short" EVENT__SIZEOF_SHORT BUILTIN_TYPES_ONLY)
check_type_size("int" EVENT__SIZEOF_INT BUILTIN_TYPES_ONLY)
check_type_size("unsigned" EVENT__SIZEOF_UNSIGNED BUILTIN_TYPES_ONLY)
check_type_size("unsigned int" EVENT__SIZEOF_UNSIGNED_INT BUILTIN_TYPES_ONLY)
check_type_size("long" EVENT__SIZEOF_LONG BUILTIN_TYPES_ONLY)
check_type_size("long long" EVENT__SIZEOF_LONG_LONG BUILTIN_TYPES_ONLY)

if(WIN32)
  # These aren't available until Windows Vista.
  # But you can still link them. They just won't be found when running the exe.
  set(EVENT__HAVE_INET_NTOP 0)
  set(EVENT__HAVE_INET_PTON 0)
endif()

# Check for different inline keyword versions.
check_function_keywords("inline" "__inline" "__inline__")

if(HAVE_INLINE)
  set(EVENT__inline inline)
elseif(HAVE___INLINE)
  set(EVENT__inline __inline)
elseif(HAVE___INLINE__)
  set(EVENT__inline __inline__)
else()
  set(EVENT__inline)
endif()

# __func__/__FUNCTION__ is not a macros in general
check_symbol_exists("__func__" "" EVENT__HAVE___func__)
check_symbol_exists("__FUNCTION__" "" EVENT__HAVE___FUNCTION__)

check_symbol_exists(TAILQ_FOREACH sys/queue.h EVENT__HAVE_TAILQFOREACH)
check_const_exists(CTL_KERN sys/sysctl.h EVENT__HAVE_DECL_CTL_KERN)
check_const_exists(KERN_ARND sys/sysctl.h EVENT__HAVE_DECL_KERN_ARND)
check_symbol_exists(F_SETFD fcntl.h EVENT__HAVE_SETFD)

check_type_size(fd_mask EVENT__HAVE_FD_MASK)

check_type_size(size_t EVENT__SIZEOF_SIZE_T)
if(NOT EVENT__SIZEOF_SIZE_T)
  set(EVENT__size_t "unsigned")
  set(EVENT__SIZEOF_SIZE_T ${EVENT__SIZEOF_UNSIGNED})
else()
  set(EVENT__size_t size_t)
endif()

check_type_size("off_t" EVENT__SIZEOF_OFF_T LANGUAGE C)


# XXX we should functionalize these size and type sets. --elley

# Winssck.
if(_MSC_VER)
  list(APPEND CMAKE_EXTRA_INCLUDE_FILES BaseTsd.h)
endif()
check_type_size("ssize_t" EVENT__SIZEOF_SSIZE_T_LOWER LANGUAGE C)
check_type_size("SSIZE_T" EVENT__SIZEOF_SSIZE_T_UPPER LANGUAGE C)

if(EVENT__SIZEOF_SSIZE_T_LOWER)
  set(EVENT__ssize_t "ssize_t")
  set(EVENT__SIZEOF_SSIZE_T ${EVENT__SIZEOF_SSIZE_T_LOWER})
elseif(EVENT__SIZEOF_SSIZE_T_UPPER)
  set(EVENT__ssize_t "SSIZE_T")
  set(EVENT__SIZEOF_SSIZE_T ${EVENT__SIZEOF_SSIZE_T_UPPER})
else()
  set(EVENT__ssize_t "int")
  set(EVENT__SIZEOF_SSIZE_T ${EVENT__SIZEOF_INT})
endif()

check_type_size(socklen_t EVENT__SIZEOF_SOCKLEN_T)
if(NOT EVENT__SIZEOF_SOCKLEN_T)
  set(EVENT__socklen_t "unsigned int")
  set(EVENT__SIZEOF_SOCKLEN_T ${EVENT__SIZEOF_UNSIGNED_INT})
else()
  set(EVENT__socklen_t "socklen_t")
endif()

check_type_size(pid_t EVENT__SIZEOF_PID_T)
if(NOT EVENT__SIZEOF_PID_T)
  set(EVENT__SIZEOF_PID_T ${EVENT__SIZEOF_INT})
else()
  set(EVENT__SIZEOF_PID_T EVENT__SIZEOF_PID_T)
endif()

if(NOT EVENT__DISABLE_THREAD_SUPPORT)
  if(NOT WIN32)
    list(APPEND CMAKE_EXTRA_INCLUDE_FILES pthread.h)
  endif()
  check_type_size(pthread_t EVENT__SIZEOF_PTHREAD_T)
endif()

if(EVENT__HAVE_CLOCK_GETTIME)
  set(EVENT__DNS_USE_CPU_CLOCK_FOR_ID 1)
endif()

# we're just getting lazy now.
check_type_size("uintptr_t" EVENT__HAVE_UINTPTR_T)
check_type_size("void *" EVENT__SIZEOF_VOID_P)
check_type_size("time_t" EVENT__SIZEOF_TIME_T)

# Tests file offset bits.
# TODO: Add AIX test for if -D_LARGE_FILES is needed.

# XXX: Why is this here? we don't even use it. Well, we don't even use it
#      on top of that, why is it set in the config.h?! IT_MAKES_NO_SENSE
#      I'm commenting it out for now.
#      - ellzey

#CHECK_FILE_OFFSET_BITS()
if(APPLE)
  set(EVENT__FORCE_KQUEUE_CHECK ON)
endif()
# Verify kqueue works with pipes.
if(EVENT__HAVE_KQUEUE)
  if((CMAKE_CROSSCOMPILING OR APPLE) AND NOT EVENT__FORCE_KQUEUE_CHECK)
    message(WARNING "Cannot check if kqueue works with pipes when crosscompiling, use EVENT__FORCE_KQUEUE_CHECK to be sure (this requires manually running a test program on the cross compilation target)")
    set(EVENT__HAVE_WORKING_KQUEUE 1)
  else()
    message(STATUS "Checking if kqueue works with pipes...")
    include(CheckWorkingKqueue)
  endif()
endif()

if(EVENT__HAVE_NETDB_H)
  list(APPEND CMAKE_EXTRA_INCLUDE_FILES netdb.h)
  check_type_size("struct addrinfo" EVENT__HAVE_STRUCT_ADDRINFO)
elseif(WIN32)
  list(APPEND CMAKE_EXTRA_INCLUDE_FILES ws2tcpip.h)
  check_type_size("struct addrinfo" EVENT__HAVE_STRUCT_ADDRINFO)
endif()

# Check for sockaddr structure sizes.
set(SOCKADDR_HEADERS)
if(WIN32)
  set(CMAKE_REQUIRED_DEFINITIONS "-DWIN32_LEAN_AND_MEAN")
  if(_MSC_VER LESS 1300)
    set(SOCKADDR_HEADERS winsock.h)
  else()
    set(SOCKADDR_HEADERS winsock2.h ws2tcpip.h)
  endif()
else()
  if(EVENT__HAVE_NETINET_IN_H)
    set(SOCKADDR_HEADERS ${SOCKADDR_HEADERS} netinet/in.h)
  endif()

  if(EVENT__HAVE_NETINET_IN6_H)
    set(SOCKADDR_HEADERS ${SOCKADDR_HEADERS} netinet/in6.h)
  endif()

  if(EVENT__HAVE_SYS_SOCKET_H)
    set(SOCKADDR_HEADERS ${SOCKADDR_HEADERS} sys/socket.h)
  endif()

  if(EVENT__HAVE_NETDB_H)
    set(SOCKADDR_HEADERS ${SOCKADDR_HEADERS} netdb.h)
  endif()
endif()

check_type_size("struct in6_addr" EVENT__HAVE_STRUCT_IN6_ADDR)
if(EVENT__HAVE_STRUCT_IN6_ADDR)
  check_struct_has_member(
    "struct in6_addr"
    s6_addr16 "${SOCKADDR_HEADERS}"
    EVENT__HAVE_STRUCT_IN6_ADDR_S6_ADDR16
  )

  check_struct_has_member(
    "struct in6_addr"
    s6_addr32 "${SOCKADDR_HEADERS}"
    EVENT__HAVE_STRUCT_IN6_ADDR_S6_ADDR32
  )
endif()

check_type_size("sa_family_t" EVENT__HAVE_SA_FAMILY_T)
check_type_size("struct sockaddr_in6" EVENT__HAVE_STRUCT_SOCKADDR_IN6)

if(EVENT__HAVE_STRUCT_SOCKADDR_IN6)
  check_struct_has_member(
    "struct sockaddr_in6"
    sin6_len "${SOCKADDR_HEADERS}"
    EVENT__HAVE_STRUCT_SOCKADDR_IN6_SIN6_LEN
  )

  check_struct_has_member(
    "struct sockaddr_in6"
    sin_len "${SOCKADDR_HEADERS}"
    EVENT__HAVE_STRUCT_SOCKADDR_IN_SIN_LEN
  )
endif()

check_type_size("struct sockaddr_storage" EVENT__HAVE_STRUCT_SOCKADDR_STORAGE)
if(EVENT__HAVE_STRUCT_SOCKADDR_STORAGE)
  check_struct_has_member(
    "struct sockaddr_storage"
    ss_family "${SOCKADDR_HEADERS}"
    EVENT__HAVE_STRUCT_SOCKADDR_STORAGE_SS_FAMILY
  )

  check_struct_has_member(
    "struct sockaddr_storage"
    __ss_family "${SOCKADDR_HEADERS}" EVENT__HAVE_STRUCT_SOCKADDR_STORAGE___SS_FAMILY
  )
endif()

check_type_size("struct linger" EVENT__HAVE_STRUCT_LINGER)
set(EVENT__HAVE_STRUCT_IN6_ADDR 1)
set(EVENT__HAVE_SA_FAMILY_T 1)
set(EVENT__HAVE_STRUCT_SOCKADDR_IN6 1)
set(EVENT__HAVE_STRUCT_SOCKADDR_STORAGE 1)
set(EVENT__HAVE_GETTIMEOFDAY 1)
set(EVENT__HAVE_STRSEP 1)
set(EVENT__HAVE_STRTOK_R 1)
if(CMAKE_SYSTEM_NAME MATCHES "Linux")
  set(EVENT__HAVE_POLL 1)
  set(EVENT__HAVE_EPOLL 1)
endif()
if(CMAKE_SYSTEM_NAME MATCHES "Darwin")
  set(EVENT__HAVE_KQUEUE 1)
endif()
# Group the source files.
set(
  HDR_PRIVATE
  ${LIBEVENT_SOURCE_DIR}/bufferevent-internal.h
  ${LIBEVENT_SOURCE_DIR}/changelist-internal.h
  ${LIBEVENT_SOURCE_DIR}/defer-internal.h
  ${LIBEVENT_SOURCE_DIR}/epolltable-internal.h
  ${LIBEVENT_SOURCE_DIR}/evbuffer-internal.h
  ${LIBEVENT_SOURCE_DIR}/event-internal.h
  ${LIBEVENT_SOURCE_DIR}/evmap-internal.h
  ${LIBEVENT_SOURCE_DIR}/evrpc-internal.h
  ${LIBEVENT_SOURCE_DIR}/evsignal-internal.h
  ${LIBEVENT_SOURCE_DIR}/evthread-internal.h
  ${LIBEVENT_SOURCE_DIR}/ht-internal.h
  ${LIBEVENT_SOURCE_DIR}/http-internal.h
  ${LIBEVENT_SOURCE_DIR}/iocp-internal.h
  ${LIBEVENT_SOURCE_DIR}/ipv6-internal.h
  ${LIBEVENT_SOURCE_DIR}/log-internal.h
  ${LIBEVENT_SOURCE_DIR}/minheap-internal.h
  ${LIBEVENT_SOURCE_DIR}/mm-internal.h
  ${LIBEVENT_SOURCE_DIR}/ratelim-internal.h
  ${LIBEVENT_SOURCE_DIR}/strlcpy-internal.h
  ${LIBEVENT_SOURCE_DIR}/util-internal.h
  ${LIBEVENT_SOURCE_DIR}/evconfig-private.h
  ${LIBEVENT_SOURCE_DIR}/compat/sys/queue.h
)

set(
  HDR_COMPAT
  ${LIBEVENT_SOURCE_DIR}/include/evdns.h
  ${LIBEVENT_SOURCE_DIR}/include/evrpc.h
  ${LIBEVENT_SOURCE_DIR}/include/event.h
  ${LIBEVENT_SOURCE_DIR}/include/evhttp.h
  ${LIBEVENT_SOURCE_DIR}/include/evutil.h
)

set(
  HDR_PUBLIC
  ${LIBEVENT_SOURCE_DIR}/include/event2/buffer.h
  ${LIBEVENT_SOURCE_DIR}/include/event2/bufferevent.h
  ${LIBEVENT_SOURCE_DIR}/include/event2/bufferevent_compat.h
  ${LIBEVENT_SOURCE_DIR}/include/event2/bufferevent_struct.h
  ${LIBEVENT_SOURCE_DIR}/include/event2/buffer_compat.h
  ${LIBEVENT_SOURCE_DIR}/include/event2/dns.h
  ${LIBEVENT_SOURCE_DIR}/include/event2/dns_compat.h
  ${LIBEVENT_SOURCE_DIR}/include/event2/dns_struct.h
  ${LIBEVENT_SOURCE_DIR}/include/event2/event.h
  ${LIBEVENT_SOURCE_DIR}/include/event2/event_compat.h
  ${LIBEVENT_SOURCE_DIR}/include/event2/event_struct.h
  ${LIBEVENT_SOURCE_DIR}/include/event2/http.h
  ${LIBEVENT_SOURCE_DIR}/include/event2/http_compat.h
  ${LIBEVENT_SOURCE_DIR}/include/event2/http_struct.h
  ${LIBEVENT_SOURCE_DIR}/include/event2/keyvalq_struct.h
  ${LIBEVENT_SOURCE_DIR}/include/event2/listener.h
  ${LIBEVENT_SOURCE_DIR}/include/event2/rpc.h
  ${LIBEVENT_SOURCE_DIR}/include/event2/rpc_compat.h
  ${LIBEVENT_SOURCE_DIR}/include/event2/rpc_struct.h
  ${LIBEVENT_SOURCE_DIR}/include/event2/tag.h
  ${LIBEVENT_SOURCE_DIR}/include/event2/tag_compat.h
  ${LIBEVENT_SOURCE_DIR}/include/event2/thread.h
  ${LIBEVENT_SOURCE_DIR}/include/event2/util.h
  ${LIBEVENT_SOURCE_DIR}/include/event2/visibility.h
  ${CMAKE_CURRENT_BINARY_DIR}/include/event2/event-config.h
)

set(
  SRC_CORE
  ${LIBEVENT_SOURCE_DIR}/buffer.c
  ${LIBEVENT_SOURCE_DIR}/bufferevent.c
  ${LIBEVENT_SOURCE_DIR}/bufferevent_filter.c
  ${LIBEVENT_SOURCE_DIR}/bufferevent_pair.c
  ${LIBEVENT_SOURCE_DIR}/bufferevent_ratelim.c
  ${LIBEVENT_SOURCE_DIR}/bufferevent_sock.c
  ${LIBEVENT_SOURCE_DIR}/event.c
  ${LIBEVENT_SOURCE_DIR}/evmap.c
  ${LIBEVENT_SOURCE_DIR}/evthread.c
  ${LIBEVENT_SOURCE_DIR}/evutil.c
  ${LIBEVENT_SOURCE_DIR}/evutil_rand.c
  ${LIBEVENT_SOURCE_DIR}/evutil_time.c
  ${LIBEVENT_SOURCE_DIR}/listener.c
  ${LIBEVENT_SOURCE_DIR}/log.c
  ${LIBEVENT_SOURCE_DIR}/signal.c
  ${LIBEVENT_SOURCE_DIR}/strlcpy.c
)

if(EVENT__HAVE_SELECT)
  list(APPEND SRC_CORE ${LIBEVENT_SOURCE_DIR}/select.c)
endif()

if(EVENT__HAVE_POLL)
  list(APPEND SRC_CORE ${LIBEVENT_SOURCE_DIR}/poll.c)
endif()

if(EVENT__HAVE_KQUEUE)
  list(APPEND SRC_CORE ${LIBEVENT_SOURCE_DIR}/kqueue.c)
endif()

if(EVENT__HAVE_DEVPOLL)
  list(APPEND SRC_CORE ${LIBEVENT_SOURCE_DIR}/devpoll.c)
endif()

if(EVENT__HAVE_EPOLL)
  list(APPEND SRC_CORE ${LIBEVENT_SOURCE_DIR}/epoll.c)
endif()

if(EVENT__HAVE_EVENT_PORTS)
  list(APPEND SRC_CORE ${LIBEVENT_SOURCE_DIR}/evport.c)
endif()

if(NOT EVENT__DISABLE_OPENSSL)
  #find_package(OpenSSL REQUIRED)
  set(EVENT__HAVE_OPENSSL 1)

  #message(STATUS "OpenSSL include: ${OPENSSL_INCLUDE_DIR}")
  #message(STATUS "OpenSSL lib: ssl")

  #include_directories(${OPENSSL_INCLUDE_DIR})

  list(APPEND SRC_OPENSSL ${LIBEVENT_SOURCE_DIR}/bufferevent_openssl.c)
  list(APPEND HDR_PUBLIC ${LIBEVENT_SOURCE_DIR}/include/event2/bufferevent_ssl.h)
  #list(APPEND LIB_APPS ${OPENSSL_LIBRARIES})
endif()

if(NOT EVENT__DISABLE_THREAD_SUPPORT)
  if(WIN32)
    list(APPEND SRC_CORE ${LIBEVENT_SOURCE_DIR}/evthread_win32.c)
  else()
    find_package(Threads REQUIRED)
    if(NOT CMAKE_USE_PTHREADS_INIT)
      message(
        FATAL_ERROR
        "Failed to find Pthreads, set EVENT__DISABLE_THREAD_SUPPORT to disable"
      )
    endif()

    set(EVENT__HAVE_PTHREADS 1)
    list(APPEND LIB_APPS ${CMAKE_THREAD_LIBS_INIT})
  endif()
endif()

set(
  SRC_EXTRA
  ${LIBEVENT_SOURCE_DIR}/event_tagging.c
  ${LIBEVENT_SOURCE_DIR}/http.c
  ${LIBEVENT_SOURCE_DIR}/evdns.c
  ${LIBEVENT_SOURCE_DIR}/evrpc.c
)

add_definitions(-DHAVE_CONFIG_H)

# We use BEFORE here so we don't accidentally look in system directories
# first for some previous versions of the headers that are installed.
#include_directories(
#  BEFORE ${PROJECT_SOURCE_DIR}
#  ${LIBEVENT_LIBRARY}/compat
#  ${LIBEVENT_LIBRARY}/include
#)

if(WIN32)
  list(
    APPEND SRC_CORE
    ${LIBEVENT_SOURCE_DIR}/buffer_iocp.c
    ${LIBEVENT_SOURCE_DIR}/bufferevent_async.c
    ${LIBEVENT_SOURCE_DIR}/event_iocp.c
    ${LIBEVENT_SOURCE_DIR}/win32select.c
  )

  list(APPEND HDR_PRIVATE ${LIBEVENT_SOURCE_DIR}/WIN32-Code/getopt.h)

  set(EVENT__DNS_USE_FTIME_FOR_ID 1)
  set(LIB_PLATFORM ws2_32 shell32 advapi32)
  add_definitions(
    -D_CRT_SECURE_NO_WARNINGS
    -D_CRT_NONSTDC_NO_DEPRECATE
  )

  include_directories(${LIBEVENT_SOURCE_DIR}/WIN32-Code)
endif()

if(SOLARIS)
  list(APPEND LIB_PLATFORM socket nsl)
endif()

source_group("Headers Private" FILES ${HDR_PRIVATE})
source_group("Header Compat" FILES ${HDR_COMPAT})
source_group("Headers Public" FILES ${HDR_PUBLIC})
source_group("Source Core" FILES ${SRC_CORE})
source_group("Source Extra" FILES ${SRC_EXTRA})

# Generate the configure headers.
# (Place them in the build dir so we don't polute the source tree with generated files).
#include_directories(BEFORE ${CMAKE_CURRENT_BINARY_DIR}/include)

if(${GNUC})
  set(EVENT_SHARED_FLAGS -fvisibility=hidden)
elseif("${CMAKE_C_COMPILER_ID}" STREQUAL "SunPro")
  set(EVENT_SHARED_FLAGS -xldscope=hidden)
endif()

configure_file(
  ${LIBEVENT_SOURCE_DIR}/event-config.h.cmake
  ${CMAKE_CURRENT_BINARY_DIR}/include/event2/event-config.h
  NEWLINE_STYLE UNIX
)

configure_file(
  ${LIBEVENT_SOURCE_DIR}/evconfig-private.h.cmake
  ${CMAKE_CURRENT_BINARY_DIR}/include/evconfig-private.h
)

macro(add_library_include LIB_NAME)
  target_include_directories(
    ${LIB_NAME}
    PRIVATE
    "${CMAKE_CURRENT_BINARY_DIR}/include/"
  )
  target_include_directories(
    ${LIB_NAME}
    SYSTEM
    PUBLIC
    "${LIBEVENT_SOURCE_DIR}/include"
    "${CMAKE_CURRENT_BINARY_DIR}/include/"
  )
endmacro()

#
# Create the libraries.
#
add_library(
  event_core
  STATIC
  ${SRC_CORE}
)
add_library_include(event_core)

add_library(
  event_extra
  STATIC
  ${SRC_EXTRA}
)
target_link_libraries(
  event_extra
  PUBLIC
  event_core
)
add_library_include(event_extra)

if(NOT EVENT__DISABLE_OPENSSL)
  add_library(
    event_openssl
    STATIC
    ${SRC_OPENSSL}
  )
  target_link_libraries(
    event_openssl
    PUBLIC
    event_core
    ssl
  )
  add_library_include(event_openssl)
endif()

if(EVENT__HAVE_PTHREADS)
  set(SRC_PTHREADS ${LIBEVENT_SOURCE_DIR}/evthread_pthread.c)
  add_library(
    event_pthreads
    STATIC
    ${SRC_PTHREADS}
  )
  target_link_libraries(
    event_pthreads
    PUBLIC
    event_core
  )
  add_library_include(event_pthreads)
endif()

add_library(
  event
  ${SRC_CORE} ${SRC_EXTRA}
)
add_library_include(event)

set(
  EVENT__INCLUDE_DIRS
  "${LIBEVENT_SOURCE_DIR}/include"
  "${CMAKE_THIRDPARTY_HEADER_DIR}/event2"
)

set(
  LIBEVENT_INCLUDE_DIRS
  ${EVENT__INCLUDE_DIRS}
  CACHE PATH "Libevent include directories"
)


##copy head file
#foreach(headfile ${HDR_PUBLIC})
#  file(
#    COPY
#    ${headfile}
#    DESTINATION ${CMAKE_THIRDPARTY_HEADER_DIR}/event2/
#  )
#endforeach()


#set(WIN32_GETOPT)
#
# We run all tests with the different backends turned on one at a time.
#

# Add event backends based on system introspection result.
set(BACKENDS "")

if(EVENT__HAVE_EPOLL)
  list(APPEND BACKENDS EPOLL)
endif()

if(EVENT__HAVE_SELECT)
  list(APPEND BACKENDS SELECT)
endif()

if(EVENT__HAVE_POLL)
  list(APPEND BACKENDS POLL)
endif()

if(EVENT__HAVE_KQUEUE)
  list(APPEND BACKENDS KQUEUE)
endif()

if(EVENT__HAVE_EVENT_PORTS)
  list(APPEND BACKENDS EVPORT)
endif()

if(EVENT__HAVE_DEVPOLL)
  list(APPEND BACKENDS DEVPOLL)
endif()

if(WIN32)
  list(APPEND BACKENDS WIN32)
endif()


message(STATUS "")
message(STATUS "        ---( Libevent " ${EVENT_VERSION} " )---")
message(STATUS "")
message(STATUS "Available event backends: ${BACKENDS}")
message(STATUS "CMAKE_BINARY_DIR:         ${CMAKE_BINARY_DIR}")
message(STATUS "CMAKE_CURRENT_BINARY_DIR: ${CMAKE_CURRENT_BINARY_DIR}")
message(STATUS "CMAKE_SOURCE_DIR:         ${LIBEVENT_SOURCE_DIR}")
message(STATUS "LIBEVENT_SOURCE_DIR:      ${LIBEVENT_SOURCE_DIR}")
message(STATUS "PROJECT_BINARY_DIR:       ${PROJECT_BINARY_DIR}")
message(STATUS "PROJECT_SOURCE_DIR:       ${}")
message(STATUS "CMAKE_MODULE_PATH:        ${CMAKE_MODULE_PATH}")
message(STATUS "CMAKE_COMMAND:            ${CMAKE_COMMAND}")
message(STATUS "CMAKE_ROOT:               ${CMAKE_ROOT}")
message(STATUS "CMAKE_SYSTEM:             ${CMAKE_SYSTEM}")
message(STATUS "CMAKE_SYSTEM_NAME:        ${CMAKE_SYSTEM_NAME}")
message(STATUS "CMAKE_SYSTEM_VERSION:     ${CMAKE_SYSTEM_VERSION}")
message(STATUS "CMAKE_SYSTEM_PROCESSOR:   ${CMAKE_SYSTEM_PROCESSOR}")
message(STATUS "CMAKE_SKIP_RPATH:         ${CMAKE_SKIP_RPATH}")
message(STATUS "CMAKE_VERBOSE_MAKEFILE:   ${CMAKE_VERBOSE_MAKEFILE}")
message(STATUS "CMAKE_C_FLAGS:            ${CMAKE_C_FLAGS}")
message(STATUS "CMAKE_BUILD_TYPE:         ${CMAKE_BUILD_TYPE}")
message(STATUS "CMAKE_C_COMPILER:         ${CMAKE_C_COMPILER} (id ${CMAKE_C_COMPILER_ID}, clang ${CLANG}, GNUC ${GNUC})")
message(STATUS "CMAKE_AR:                 ${CMAKE_AR}")
message(STATUS "CMAKE_RANLIB:             ${CMAKE_RANLIB}")
message(STATUS "")
