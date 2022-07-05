file(GLOB HASHLIBRARY_SRCS ${CMAKE_CURRENT_SOURCE_DIR}/src/hash-library/*.cpp)
add_library(hashlibrary STATIC ${HASHLIBRARY_SRCS})
if(APPLE)
  add_definitions(-D__MACOS__)
endif()
target_include_directories(
  hashlibrary
  SYSTEM
  PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/src
)
