file(GLOB DATALIGHT_SERVER_SRCS ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)
add_library(datalight_server ${DATALIGHT_SERVER_SRCS})
target_link_libraries(
  datalight_server
  datalight_http
  nlohmann_json
  ssl
  crypto
  hashlibrary
  trino_protocol
)
