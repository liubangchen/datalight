add_executable(proxygen-echo-server EchoServer.cpp)
target_link_libraries(
  proxygen-echo-server
  wangle
  proxygen
)
