file(GLOB DATALIGHT_HTTP_SRCS ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)

add_library(datalight_http ${DATALIGHT_HTTP_SRCS})
target_link_libraries(
  datalight_http
  re2
  fmt
  folly
  wangle
  proxygen
  proxygenhttpserver
)
