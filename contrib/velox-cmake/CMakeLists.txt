fetchcontent_declare(
  velox
  GIT_REPOSITORY https://github.com/facebookincubator/velox.git
  GIT_TAG main
  GIT_PROGRESS TRUE
  SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/velox
)

fetchcontent_getproperties(velox)

if(NOT velox_POPULATED)
  message(STATUS "Fetching velox")
  fetchcontent_populate(velox)
  message(STATUS "Fetching velox - done")
  set(VELOX_SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/velox)
endif()

message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
option(
  VELOX_BUILD_TESTING
  "Enable Velox tests. This will enable all other build options automatically."
  OFF
)
option(
  VELOX_BUILD_MINIMAL
  "Build a minimal set of components only. This will override other build options."
  OFF
)
option(VELOX_ENABLE_DUCKDB "Build duckDB to enable differential testing." ON)
option(VELOX_ENABLE_EXEC "Build exec." ON)
option(VELOX_ENABLE_AGGREGATES "Build aggregates." ON)
option(VELOX_ENABLE_HIVE_CONNECTOR "Build Hive connector." ON)
option(VELOX_ENABLE_TPCH_CONNECTOR "Build TPC-H connector." ON)
option(VELOX_ENABLE_PRESTO_FUNCTIONS "Build Presto SQL functions." ON)
option(VELOX_ENABLE_SPARK_FUNCTIONS "Build Spark SQL functions." ON)
option(VELOX_ENABLE_EXPRESSION "Build expression." ON)
option(VELOX_ENABLE_PARSE "Build parser used for unit tests." ON)
option(
  VELOX_ENABLE_EXAMPLES
  "Build examples. This will enable VELOX_ENABLE_EXPRESSION automatically."
  OFF
)
option(VELOX_ENABLE_SUBSTRAIT "Buid Substrait-to-Velox converter." OFF)
option(VELOX_ENABLE_BENCHMARKS "Build velox top level benchmarks." OFF)
option(VELOX_ENABLE_BENCHMARKS_BASIC "Build velox basic benchmarks." OFF)
option(VELOX_ENABLE_S3 "Build S3 Connector" OFF)
option(VELOX_ENABLE_HDFS "Build Hdfs Connector" OFF)
option(VELOX_ENABLE_PARQUET "Enable Parquet support" OFF)
option(VELOX_BUILD_TEST_UTILS "Enable Velox test utilities" OFF)


if(VELOX_ENABLE_S3)
  # Set AWS_ROOT_DIR if you have a custom install location of AWS SDK CPP.
  if(AWSSDK_ROOT_DIR)
    set(CMAKE_PREFIX_PATH ${AWSSDK_ROOT_DIR})
  endif()
  find_package(AWSSDK REQUIRED COMPONENTS s3;identity-management)
  add_definitions(-DVELOX_ENABLE_S3)
endif()

if(VELOX_ENABLE_HDFS)
  find_library(
    LIBHDFS3
    NAMES libhdfs3.so libhdfs3.dylib
    HINTS "${CMAKE_SOURCE_DIR}/hawq/depends/libhdfs3/_build/src/" REQUIRED
  )
  add_definitions(-DVELOX_ENABLE_HDFS3)
endif()

if(VELOX_ENABLE_PARQUET)
  add_definitions(-DVELOX_ENABLE_PARQUET)
endif()

if(
  (NOT DEFINED VELOX_CODEGEN_SUPPORT)
  AND (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  AND NOT (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
)
  message(STATUS "Enabling Codegen")
  set(VELOX_CODEGEN_SUPPORT True)
else()
  message(STATUS "Disabling Codegen")
  set(VELOX_CODEGEN_SUPPORT False)
endif()

# define processor variable for conditional compilation
if(${VELOX_CODEGEN_SUPPORT})
  add_compile_definitions(CODEGEN_ENABLED=1)
endif()

if(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  add_link_options("-Wl,-flat_namespace")
endif()

if(UNIX AND NOT APPLE)
  # codegen linker flags, -export-dynamic for rtti
  add_link_options("-Wl,-export-dynamic")
endif()

if(
  ${CMAKE_SYSTEM_PROCESSOR} MATCHES "arm64" AND ${CMAKE_SYSTEM_NAME} MATCHES
  "Darwin"
)
  set(ON_APPLE_M1 True)
endif()


# Required so velox code can be used in a dynamic library
set(POSITION_INDEPENDENT_CODE True)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

execute_process(
  COMMAND
  bash -c
  "( source ${VELOX_SOURCE_DIR}/scripts/setup-helper-functions.sh && echo -n $(get_cxx_flags $ENV{CPU_TARGET}))"
  OUTPUT_VARIABLE SCRIPT_CXX_FLAGS
  RESULT_VARIABLE COMMAND_STATUS
)

if(COMMAND_STATUS EQUAL "1")
  message(FATAL_ERROR "Unable to determine compiler flags!")
endif()
message("Setting CMAKE_CXX_FLAGS=${SCRIPT_CXX_FLAGS}")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SCRIPT_CXX_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D USE_VELOX_COMMON_BASE")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D HAS_UNCAUGHT_EXCEPTIONS")

if("${MAX_HIGH_MEM_JOBS}")
  set_property(
    GLOBAL PROPERTY JOB_POOLS
    "high_memory_pool=${MAX_HIGH_MEM_JOBS}"
  )
else()
  set_property(GLOBAL PROPERTY JOB_POOLS high_memory_pool=1000)
endif()

if("${MAX_LINK_JOBS}")
  set_property(
    GLOBAL APPEND PROPERTY JOB_POOLS
    "link_job_pool=${MAX_LINK_JOBS}"
  )
  set(CMAKE_JOB_POOL_LINK link_job_pool)
endif()

if("${TREAT_WARNINGS_AS_ERRORS}")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
endif()

if("${ENABLE_ALL_WARNINGS}")
  if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    set(
      KNOWN_COMPILER_SPECIFIC_WARNINGS
      "-Wno-range-loop-analysis -Wno-mismatched-tags"
    )
  elseif(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set(
      KNOWN_COMPILER_SPECIFIC_WARNINGS
      "-Wno-implicit-fallthrough \
         -Wno-empty-body \
         -Wno-class-memaccess \
         -Wno-comment \
         -Wno-int-in-bool-context \
         -Wno-redundant-move \
         -Wno-array-bounds \
         -Wno-maybe-uninitialized \
         -Wno-unused-result \
         -Wno-format-overflow \
         -Wno-strict-aliasing \
         -Wno-type-limits"
    )
  endif()

  set(
    KNOWN_WARNINGS
    "-Wno-unused \
       -Wno-unused-parameter \
       -Wno-sign-compare \
       -Wno-ignored-qualifiers \
       ${KNOWN_COMPILER_SPECIFIC_WARNINGS}"
  )

  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra ${KNOWN_WARNINGS}")
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(Boost_USE_MULTITHREADED TRUE)

# GCC needs to link a library to enable std::filesystem.
if("${CMAKE_CXX_COMPILER_ID}" MATCHES "GNU")
  set(FILESYSTEM "stdc++fs")

  # Ensure we have gcc at least 8+.
  if(CMAKE_CXX_COMPILER_VERSION LESS 8.0)
    message(FATAL_ERROR "VELOX requires gcc > 8. Found ${CMAKE_CXX_COMPILER_VERSION}")
  endif()
else()
  set(FILESYSTEM "")
endif()

if(CMAKE_HOST_SYSTEM_NAME MATCHES "Darwin")
  execute_process(
    COMMAND brew --prefix bison
    RESULT_VARIABLE BREW_BISON
    OUTPUT_VARIABLE BREW_BISON_PREFIX
    OUTPUT_STRIP_TRAILING_WHITESPACE
  )
  if(BREW_BISON EQUAL 0 AND EXISTS "${BREW_BISON_PREFIX}")
    message(STATUS "Found Bison keg installed by Homebrew at ${BREW_BISON_PREFIX}")
    set(BISON_EXECUTABLE "${BREW_BISON_PREFIX}/bin/bison")
  endif()

  execute_process(
    COMMAND brew --prefix flex
    RESULT_VARIABLE BREW_FLEX
    OUTPUT_VARIABLE BREW_FLEX_PREFIX
    OUTPUT_STRIP_TRAILING_WHITESPACE
  )
  if(BREW_FLEX EQUAL 0 AND EXISTS "${BREW_FLEX_PREFIX}")
    message(STATUS "Found Flex keg installed by Homebrew at ${BREW_FLEX_PREFIX}")
    set(FLEX_EXECUTABLE "${BREW_FLEX_PREFIX}/bin/flex")
    set(FLEX_INCLUDE_DIR "${BREW_FLEX_PREFIX}/include")
  endif()
endif()
find_package(BISON 3.0.4 REQUIRED)
find_package(FLEX 2.5.13 REQUIRED)

include_directories(SYSTEM ${VELOX_SOURCE_DIR})
include_directories(SYSTEM ${VELOX_SOURCE_DIR}/velox)
include_directories(SYSTEM ${VELOX_SOURCE_DIR}/velox/external)
include_directories(SYSTEM ${VELOX_SOURCE_DIR}/velox/external/duckdb)
include_directories(SYSTEM ${VELOX_SOURCE_DIR}/velox/external/duckdb/tpch/dbgen/include)

add_subdirectory(${VELOX_SOURCE_DIR}/third_party "${CMAKE_BINARY_DIR}/third_party/")
add_subdirectory(${VELOX_SOURCE_DIR}/velox "${CMAKE_BINARY_DIR}/velox/")

target_link_libraries(
  velox_type_calculation
  fmt
  glog
  folly
)


target_link_libraries(
  velox_process
  fmt
  glog
  folly
)
