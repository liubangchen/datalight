set(VELOX_SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/velox)
include_directories(SYSTEM ${VELOX_SOURCE_DIR})
include_directories(SYSTEM ${VELOX_SOURCE_DIR}/velox)
include_directories(SYSTEM ${VELOX_SOURCE_DIR}/velox/external)
include_directories(SYSTEM ${VELOX_SOURCE_DIR}/velox/external/duckdb)
include_directories(SYSTEM ${VELOX_SOURCE_DIR}/velox/external/duckdb/tpch/dbgen/include)

include_directories(SYSTEM ${PROJECT_SOURCE_DIR}/src/trino)

add_subdirectory(http)
add_subdirectory(server)
add_executable(trino-server TrinoMain.cpp)

target_link_libraries(
  trino-server
  glog
  folly
  gflags
  velox_common_base
  datalight_server
)
