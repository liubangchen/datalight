add_executable(fmtbin FmtDemo.cpp)
target_link_libraries(fmtbin fmt)


add_executable(gfbin GflagDemo.cpp)
target_link_libraries(gfbin gflags)



add_executable(glogbin GlogDemo.cpp)
target_link_libraries(glogbin glog)


add_executable(eventbin EventDemo.cpp)
target_link_libraries(eventbin event_core)

add_executable(doubleconv DoubleConvDemo.cpp)
target_link_libraries(doubleconv double-conversion)


add_executable(compressbin CompressDemo.cpp)
target_link_libraries(
  compressbin
  lz4
  lzo
  zstd
  zlib
  snappy
  bzip2
)

add_executable(follybin FollyDemo.cpp)
target_link_libraries(follybin folly)


add_executable(veloxdemo VeloxDemo.cpp)
target_link_libraries(
  veloxdemo
  velox_type
  velox_vector
  velox_exec
  velox_exec_test_util
  velox_hive_connector
  velox_memory
)

add_executable(echo-server EchoServer.cpp)
target_link_libraries(
  echo-server
  wangle
)

add_executable(range-demo Rangev3Demo.cpp)
target_link_libraries(
  range-demo
  range-v3
)

add_executable(icu-demo IcuDemo.cpp)
target_link_libraries(
  icu-demo
  PRIVATE
  _icuc
  _icui18n
  _icudata
)

add_executable(antlr-demo AntlrDemo.cpp)
target_link_libraries(
  antlr-demo
  PRIVATE
  antlr4
)

add_executable(json-demo RapidjsonDemo.cpp)
target_link_libraries(
  json-demo
  PRIVATE
  rapidjson
  fmt
)

add_subdirectory(proxygen)
