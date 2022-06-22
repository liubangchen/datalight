set(BOOST_VERSION "1_76_0")

option(BOOST_DISABLE_TESTS "Do not build test targets, even if building standalone" OFF)

set(BOOST_URL "https://boostorg.jfrog.io/artifactory/main/release/1.76.0/source/boost_1_76_0.tar.gz" CACHE STRING "Boost download URL")
set(BOOST_URL_SHA256 "7bd7ddceec1a1dfdcbdb3e609b60d01739c38390a5f956385a12f3122049f0ca" CACHE STRING "Boost download URL SHA256 checksum")
fetchcontent_declare(
  Boost
  URL ${BOOST_URL}
  URL_HASH SHA256=${BOOST_URL_SHA256}
  SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/boost
)

fetchcontent_getproperties(Boost)

if(NOT Boost_POPULATED)
  message(STATUS "Fetching Boost")
  fetchcontent_populate(Boost)
  message(STATUS "Fetching Boost - done")
  set(BOOST_SOURCE_DIR "${PROJECT_SOURCE_DIR}/contrib/boost")
  set(BOOST_SOURCE ${BOOST_SOURCE_DIR})
endif()

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules)
include(CheckBoostVersion)

message(STATUS "Boost found: ${BOOST_VERSION} ${BOOST_SOURCE}")

include(StandaloneBuild)
include(PlatformDetect)
include(AddBoostLib)
include(AddBoostTest)

set_property(GLOBAL PROPERTY USE_FOLDERS TRUE)

set(
  BOOST_LIBS_REQUIRED
  # Header only libs
  header
)
set(
  BOOST_LIBS_OPTIONAL
  # Compiled libs
  atomic
  chrono
  container
  context
  coroutine
  date_time
  exception
  fiber
  filesystem
  graph
  iostreams
  locale
  log
  math
  mpi
  graph_parallel # depends on mpi, so needs to be put after it
  program_options
  #python # complex module
  random
  regex
  serialization
  system
  test
  thread
  timer
  type_erasure
  wave
  CACHE STRING "Boost libs to be compiled"
)

foreach(lib ${BOOST_LIBS_REQUIRED})
  include("libs/${lib}.cmake")
endforeach()

foreach(lib ${BOOST_LIBS_OPTIONAL})
  # In case only a subset of modules is available (eg. after using bcp)
  if(EXISTS "${BOOST_SOURCE}/libs/${lib}")
    include("libs/${lib}.cmake")
  endif()
endforeach()

# TODO: Move those to option() calls in the right file
if(NOT BOOST_STANDALONE)
  # Compilation options required by all platforms
  target_compile_definitions(
    Boost::boost INTERFACE
    $<$<CONFIG:Release>:BOOST_DISABLE_ASSERT>
    BOOST_SYSTEM_NO_DEPRECATED
    BOOST_THREAD_VERSION=4
    BOOST_THREAD_USES_CHRONO
    BOOST_THREAD_PROVIDES_EXECUTORS
  )
endif()
