fetchcontent_declare(
  protobuf
  GIT_REPOSITORY https://github.com/protocolbuffers/protobuf.git
  GIT_TAG v3.16.1
  SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/protobuf
)

fetchcontent_getproperties(protobuf)

if(NOT protobuf_POPULATED)
  message(STATUS "Fetching protobuf")
  fetchcontent_populate(protobuf)
  message(STATUS "Fetching protobuf - done")
  set(PROTOBUF_SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/protobuf)
endif()

set(protobuf_SOURCE_DIR "${PROTOBUF_SOURCE_DIR}")

set(protobuf_WITH_ZLIB 0 CACHE INTERNAL "" FORCE) # actually will use zlib, but skip find
set(protobuf_BUILD_TESTS OFF CACHE INTERNAL "" FORCE)
set(protobuf_BUILD_SHARED_LIBS OFF CACHE INTERNAL "" FORCE)
set(protobuf_BUILD_TESTS OFF)
set(BUILD_SHARED_LIBS OFF)

add_subdirectory("${protobuf_SOURCE_DIR}/cmake" "${CMAKE_BINARY_DIR}/protobuf/")

# We don't want to stop compilation on warnings in protobuf's headers.
# The following line overrides the value assigned by the command target_include_directories() in libprotobuf.cmake
set_property(TARGET libprotobuf PROPERTY INTERFACE_SYSTEM_INCLUDE_DIRECTORIES ${protobuf_SOURCE_DIR}/src)

set(Protobuf_INCLUDE_DIR "${PROTOBUF_SOURCE_DIR}/contrib/protobuf/src")
set(Protobuf_LIBRARY libprotobuf)
set(Protobuf_PROTOC_EXECUTABLE "$<TARGET_FILE:protoc>")
set(Protobuf_PROTOC_LIBRARY libprotoc)

include("${PROJECT_SOURCE_DIR}/contrib/protobuf-cmake/protobuf_generate.cmake")
