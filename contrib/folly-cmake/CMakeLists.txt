fetchcontent_declare(
  folly
  GIT_REPOSITORY https://github.com/facebook/folly.git
  GIT_TAG ${FB_LIB_VERSION}
  GIT_PROGRESS TRUE
  SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/folly
)

fetchcontent_getproperties(folly)

if(NOT folly_POPULATED)
  message(STATUS "Fetching folly")
  fetchcontent_populate(folly)
  message(STATUS "Fetching folly - done")
  set(FOLLY_SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/folly)
endif()

include(${FOLLY_SOURCE_DIR}/CMake/FollyConfigChecks.cmake)
if(POLICY CMP0054)
  cmake_policy(SET CMP0054 NEW)
endif()
if(POLICY CMP0057)
  cmake_policy(SET CMP0057 NEW)
endif()

# CMP0075 Include file check macros honor CMAKE_REQUIRED_LIBRARIES
if(POLICY CMP0075)
  cmake_policy(SET CMP0075 NEW)
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(TOP_DIR "${CMAKE_CURRENT_SOURCE_DIR}")
set(FOLLY_DIR "${FOLLY_SOURCE_DIR}/folly")

include(GNUInstallDirs)

set(CMAKE_THREAD_PREFER_PTHREAD ON)
set(THREADS_PREFER_PTHREAD_FLAG ON)

find_package(Threads REQUIRED)
set(FOLLY_HAVE_PTHREAD "${CMAKE_USE_PTHREADS_INIT}")
list(APPEND CMAKE_REQUIRED_LIBRARIES Threads::Threads)
list(APPEND FOLLY_LINK_LIBRARIES Threads::Threads)

set(FOLLY_MOBILE 0)
set(FOLLY_HAVE_PTHREAD_ATFORK 1)
set(FOLLY_HAVE_LIBGFLAGS 1)
set(FOLLY_GFLAGS_NAMESPACE gflags)
set(FOLLY_HAVE_LIBGLOG 1)
set(FOLLY_HAVE_GETRANDOM 0)
set(FOLLY_HAVE_PREADV 1)
set(FOLLY_HAVE_PWRITEV 1)
set(FOLLY_HAVE_CLOCK_GETTIME 1)
set(FOLLY_HAVE_STD__IS_TRIVIALLY_COPYABLE 1)
set(FOLLY_HAVE_UNALIGNED_ACCESS 1)
set(FOLLY_HAVE_VLA 1)
set(FOLLY_HAVE_SWAPCONTEXT 1)
set(FOLLY_HAVE_BACKTRACE 1)
set(FOLLY_HAVE_WCHAR_SUPPORT 1)
set(FOLLY_DEMANGLE_MAX_SYMBOL_SIZE 1024)
set(FOLLY_HAVE_LIBLZ4 1)
set(FOLLY_HAVE_LIBSNAPPY 1)
set(FOLLY_HAVE_LIBZ 1)
set(FOLLY_HAVE_LIBZSTD 1)
set(FOLLY_HAVE_LIBBZ2 1)
set(FOLLY_LIBRARY_SANITIZE_ADDRESS 0)
set(FOLLY_HAVE_LIBRT 0)
if(${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
  set(FOLLY_HAVE_SENDMMSG 1)
  set(FOLLY_HAVE_RECVMMSG 1)
  set(FOLLY_HAVE_LIBRT 1)
endif()

configure_file(
  ${FOLLY_SOURCE_DIR}/CMake/folly-config.h.cmake
  ${CMAKE_CURRENT_BINARY_DIR}/folly/folly-config.h
)

auto_sources(files "*.cpp" "RECURSE" "${FOLLY_DIR}")
auto_sources(hfiles "*.h" "RECURSE" "${FOLLY_DIR}")

remove_matches_from_lists(
  files hfiles
  MATCHES
  "^${FOLLY_DIR}/build/"
  "^${FOLLY_DIR}/logging/example/"
  "^${FOLLY_DIR}/(.*/)?test/"
  "^${FOLLY_DIR}/tools/"
  "Benchmark.cpp$"
  "Test.cpp$"
)

# Exclude exception tracer, which is necessary to statically link libstdc++
if(${FOLLY_NO_EXCEPTION_TRACER})
  remove_matches_from_lists(
    files hfiles
    MATCHES
    "^${FOLLY_DIR}/experimental/exception_tracer/"
  )
endif()

list(
  REMOVE_ITEM files
  ${FOLLY_DIR}/experimental/JSONSchemaTester.cpp
  ${FOLLY_DIR}/experimental/io/HugePageUtil.cpp
  ${FOLLY_DIR}/python/error.cpp
  ${FOLLY_DIR}/python/executor.cpp
  ${FOLLY_DIR}/python/fibers.cpp
  ${FOLLY_DIR}/python/GILAwareManualExecutor.cpp
  ${FOLLY_DIR}/experimental/symbolizer/Addr2Line.cpp
)
list(
  REMOVE_ITEM hfiles
  ${FOLLY_DIR}/python/fibers.h
  ${FOLLY_DIR}/python/GILAwareManualExecutor.h
)

# Explicitly include utility library code from inside
# test subdirs
list(
  APPEND files
  ${FOLLY_DIR}/io/async/test/ScopedBoundPort.cpp
  ${FOLLY_DIR}/io/async/test/SocketPair.cpp
  ${FOLLY_DIR}/io/async/test/TimeUtil.cpp
)

list(
  APPEND hfiles
  ${FOLLY_DIR}/container/test/F14TestUtil.h
  ${FOLLY_DIR}/container/test/TrackingTypes.h
  ${FOLLY_DIR}/io/async/test/AsyncSSLSocketTest.h
  ${FOLLY_DIR}/io/async/test/AsyncSocketTest.h
  ${FOLLY_DIR}/io/async/test/AsyncSocketTest2.h
  ${FOLLY_DIR}/io/async/test/BlockingSocket.h
  ${FOLLY_DIR}/io/async/test/MockAsyncSocket.h
  ${FOLLY_DIR}/io/async/test/MockAsyncServerSocket.h
  ${FOLLY_DIR}/io/async/test/MockAsyncSSLSocket.h
  ${FOLLY_DIR}/io/async/test/MockAsyncTransport.h
  ${FOLLY_DIR}/io/async/test/MockAsyncUDPSocket.h
  ${FOLLY_DIR}/io/async/test/MockTimeoutManager.h
  ${FOLLY_DIR}/io/async/test/ScopedBoundPort.h
  ${FOLLY_DIR}/io/async/test/SocketPair.h
  ${FOLLY_DIR}/io/async/test/TestSSLServer.h
  ${FOLLY_DIR}/io/async/test/TimeUtil.h
  ${FOLLY_DIR}/io/async/test/UndelayedDestruction.h
  ${FOLLY_DIR}/io/async/test/Util.h
  ${FOLLY_DIR}/synchronization/test/Semaphore.h
  ${FOLLY_DIR}/test/DeterministicSchedule.h
  ${FOLLY_DIR}/test/JsonTestUtil.h
  ${FOLLY_DIR}/test/TestUtils.h
)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  check_cxx_compiler_flag(-fcoroutines COMPILER_HAS_F_COROUTINES)
  if(COMPILER_HAS_F_COROUTINES)
    message(
      STATUS
      "GCC has support for C++ coroutines, setting flag for Folly build."
    )
    add_compile_options(-fcoroutines)
  else()
    message(
      STATUS
      "GCC does not have support for C++ coroutines, "
      "disabling Folly coroutine support."
    )
  endif()
endif()
set(LIBSODIUM_FOUND TRUE)
if(${LIBSODIUM_FOUND})
  string(FIND "${CMAKE_LIBRARY_ARCHITECTURE}" "x86_64" IS_X86_64_ARCH)
  if(${IS_X86_64_ARCH} STREQUAL "-1")
    message(
      STATUS
      "arch ${CMAKE_LIBRARY_ARCHITECTURE} does not match x86_64, "
      "skipping setting SSE2/AVX2 compile flags for LtHash SIMD code"
    )
  else()
    message(
      STATUS
      "arch ${CMAKE_LIBRARY_ARCHITECTURE} matches x86_64, "
      "setting SSE2/AVX2 compile flags for LtHash SIMD code"
    )
    set_source_files_properties(
      ${FOLLY_DIR}/experimental/crypto/detail/MathOperation_AVX2.cpp
      PROPERTIES
      COMPILE_FLAGS
      -mavx -mavx2 -msse2
    )
    set_source_files_properties(
      ${FOLLY_DIR}/experimental/crypto/detail/MathOperation_Simple.cpp
      PROPERTIES
      COMPILE_FLAGS
      -mno-avx -mno-avx2 -mno-sse2
    )
    set_source_files_properties(
      ${FOLLY_DIR}/experimental/crypto/detail/MathOperation_SSE2.cpp
      PROPERTIES
      COMPILE_FLAGS
      -mno-avx -mno-avx2 -msse2
    )
  endif()
else()
  list(
    REMOVE_ITEM files
    ${FOLLY_DIR}/experimental/crypto/Blake2xb.cpp
    ${FOLLY_DIR}/experimental/crypto/detail/MathOperation_AVX2.cpp
    ${FOLLY_DIR}/experimental/crypto/detail/MathOperation_Simple.cpp
    ${FOLLY_DIR}/experimental/crypto/detail/MathOperation_SSE2.cpp
    ${FOLLY_DIR}/experimental/crypto/LtHash.cpp
  )
  list(
    REMOVE_ITEM hfiles
    ${FOLLY_DIR}/experimental/crypto/Blake2xb.h
    ${FOLLY_DIR}/experimental/crypto/detail/LtHashInternal.h
    ${FOLLY_DIR}/experimental/crypto/LtHash-inl.h
    ${FOLLY_DIR}/experimental/crypto/LtHash.h
  )
endif()
if(NOT ${LIBGFLAGS_FOUND})
  list(
    REMOVE_ITEM files
    ${FOLLY_DIR}/experimental/NestedCommandLineApp.cpp
    ${FOLLY_DIR}/experimental/ProgramOptions.cpp
  )
  list(
    REMOVE_ITEM hfiles
    ${FOLLY_DIR}/experimental/NestedCommandLineApp.h
    ${FOLLY_DIR}/experimental/ProgramOptions.h
  )
endif()
if(CMAKE_SYSTEM_NAME STREQUAL "Windows")
  list(
    REMOVE_ITEM files
    ${FOLLY_DIR}/Subprocess.cpp
  )
endif()

set(
  PCLMUL_FILES
  ${FOLLY_DIR}/hash/detail/ChecksumDetail.cpp
  ${FOLLY_DIR}/hash/detail/Crc32CombineDetail.cpp
  ${FOLLY_DIR}/hash/detail/Crc32cDetail.cpp
)
check_cxx_compiler_flag(-mpclmul COMPILER_HAS_M_PCLMUL)
if(COMPILER_HAS_M_PCLMUL)
  message(
    STATUS
    "compiler has flag pclmul, setting compile flag for ${PCLMUL_FILES}"
  )
  set_source_files_properties(
    ${PCLMUL_FILES}
    PROPERTIES
    COMPILE_OPTIONS
    -mpclmul
  )
else()
  message(
    STATUS
    "compiler does not have flag pclmul, skipping setting compile flags for ${PCLMUL_FILES}"
  )
endif()

list(
  APPEND folly_base_files
  ${files} ${hfiles}
)

if(NOT MSVC)
  set_property(
    SOURCE
    ${FOLLY_DIR}/memcpy.S
    APPEND PROPERTY COMPILE_OPTIONS "-x" "assembler-with-cpp"
  )
  list(
    APPEND folly_base_files
    ${FOLLY_DIR}/memcpy.S
  )
endif()

add_library(
  folly STATIC
  ${folly_base_files}
  ${CMAKE_CURRENT_BINARY_DIR}/folly/folly-config.h
)
#target_compile_options(
#  folly
#  PRIVATE
#  -g
#  -std=c++17
#  -finput-charset=UTF-8
#  -fsigned-char
#  -Wall
#  -Wno-deprecated
#  -Wno-deprecated-declarations
#  -Wno-sign-compare
#  -Wno-unused
#  -Wuninitialized
#  -Wunused-label
#  -Wunused-result
#  ${FOLLY_CXX_FLAGS}
#)

target_compile_definitions(
  folly
  PUBLIC
  -DFOLLY_MOBILE=0
)
target_link_libraries(
  folly
  fmt
  boost::context
  boost::filesystem
  boost::program_options
  boost::regex
  boost::thread
  boost::system
  gflags
  glog
  event
  lz4
  lzo
  zstd
  zlib
  snappy
  bzip2
  sodium
  double-conversion
  ssl
  # crypto
)

if(${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
  target_link_libraries(
    folly
    uring
    iberty
  )
endif()

if(APPLE)
  target_link_libraries(
    folly
    c++abi
  )
endif()

target_include_directories(
  folly
  SYSTEM
  PUBLIC
  ${FOLLY_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
)

add_library(Folly::folly ALIAS folly)
