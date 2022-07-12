file(GLOB TRINO_COMMON_SRCS ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)
add_library(trino_common ${TRINO_COMMON_SRCS})
target_link_libraries(
  trino_common
  trino_protocol
)
