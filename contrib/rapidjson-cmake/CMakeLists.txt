fetchcontent_declare(
  rapidjson
  GIT_REPOSITORY https://github.com/Tencent/rapidjson.git
  GIT_TAG v1.1.0
  GIT_PROGRESS TRUE
  SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/rapidjson
)

fetchcontent_getproperties(rapidjson)
if(NOT rapidjson_POPULATED)
  message(STATUS "Fetching rapidjson")
  fetchcontent_populate(rapidjson)
  message(STATUS "Fetching rapidjson - done")
  set(RAPIDJSON_SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/rapidjson)
endif()

set(LIB_VERSION_STRING "1.1.0")

option(RAPIDJSON_HAS_STDSTRING "" OFF)
if(RAPIDJSON_HAS_STDSTRING)
  add_definitions(-DRAPIDJSON_HAS_STDSTRING)
endif()

find_program(CCACHE_FOUND ccache)
if(CCACHE_FOUND)
  set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
  set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
  if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Qunused-arguments -fcolor-diagnostics")
  endif()
endif(CCACHE_FOUND)

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -Wall -Wextra -Werror")
  if(RAPIDJSON_BUILD_CXX11)
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS "4.7.0")
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x")
    else()
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
    endif()
  endif()
  if(RAPIDJSON_BUILD_ASAN)
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS "4.8.0")
      message(FATAL_ERROR "GCC < 4.8 doesn't support the address sanitizer")
    else()
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
    endif()
  endif()
  if(RAPIDJSON_BUILD_UBSAN)
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS "4.9.0")
      message(FATAL_ERROR "GCC < 4.9 doesn't support the undefined behavior sanitizer")
    else()
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined")
    endif()
  endif()
elseif(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -Wall -Wextra -Werror -Wno-missing-field-initializers")
  if(RAPIDJSON_BUILD_CXX11)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
  endif()
  if(RAPIDJSON_BUILD_ASAN)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
  endif()
  if(RAPIDJSON_BUILD_UBSAN)
    if(CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang")
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined-trap -fsanitize-undefined-trap-on-error")
    else()
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined")
    endif()
  endif()
elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
  add_definitions(-D_CRT_SECURE_NO_WARNINGS=1)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /EHsc")
endif()



add_library(rapidjson INTERFACE)

target_include_directories(
  rapidjson
  SYSTEM
  INTERFACE
  ${RAPIDJSON_SOURCE_DIR}/include
)
