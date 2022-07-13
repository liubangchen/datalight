file(GLOB TRINO_CONFIG_SRCS ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)
add_library(trino_config ${TRINO_CONFIG_SRCS})
target_link_libraries(
  trino_config
  velox_core
)
