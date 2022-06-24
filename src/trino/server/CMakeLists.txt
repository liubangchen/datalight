add_library(datalight_server TrinoServer.cpp)
target_link_libraries(
  datalight_server
  datalight_http
)
