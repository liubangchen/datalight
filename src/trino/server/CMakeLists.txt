file(GLOB DATALIGHT_SERVER_SRCS ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)
add_library(datalight_server ${DATALIGHT_SERVER_SRCS})
target_link_libraries(
  datalight_server
  velox_core
  velox_vector
  velox_exec
  velox_time
  velox_type
  velox_type_tz
  velox_serialization
  velox_encode
  velox_dwio_common_exception
  velox_functions_lib
  velox_hive_partition_function
  datalight_http
  nlohmann_json
  ssl
  crypto
  hashlibrary
  trino_protocol
  trino_common
)
