add_library(
  trino_protocol
  OBJECT
  TrinoProtocol.cpp
  Base64Util.cpp
  DataSize.cpp
  Duration.cpp
  Connectors.cpp
)

target_link_libraries(
  trino_protocol
  velox_type
  re2
  nlohmann_json
)
