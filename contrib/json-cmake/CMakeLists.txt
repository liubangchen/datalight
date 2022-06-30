fetchcontent_declare(
  nlohmannjson
  GIT_REPOSITORY https://github.com/nlohmann/json.git
  GIT_TAG v3.10.5
  GIT_PROGRESS TRUE
  SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/nlohmannjson
)
fetchcontent_getproperties(nlohmannjson)
if(NOT nlohmannjson_POPULATED)
  message(STATUS "Fetching nlohmannjson")
  fetchcontent_populate(nlohmannjson)
  message(STATUS "Fetching nlohmannjson - done")
  set(JSON_SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/nlohmannjson)
endif()

option(JSON_Diagnostics "Use extended diagnostic messages." OFF)
option(JSON_ImplicitConversions "Enable implicit conversions." ON)
option(JSON_SystemInclude "Include as system headers (skip for clang-tidy)." OFF)

set(NLOHMANN_JSON_INCLUDE_BUILD_DIR "${JSON_SOURCE_DIR}/single_include/")
message(STATUS "Using the single-header code from ${NLOHMANN_JSON_INCLUDE_BUILD_DIR}")

if(NOT JSON_ImplicitConversions)
  message(STATUS "Implicit conversions are disabled")
endif()

if(JSON_Diagnostics)
  message(STATUS "Diagnostics enabled")
endif()

if(JSON_SystemInclude)
  set(NLOHMANN_JSON_SYSTEM_INCLUDE "SYSTEM")
endif()

add_library(nlohmann_json INTERFACE)
add_library(nlohmann_json::nlohmann_json ALIAS nlohmann_json)
target_compile_features(nlohmann_json INTERFACE cxx_std_11)
target_compile_definitions(
  nlohmann_json
  INTERFACE
  JSON_USE_IMPLICIT_CONVERSIONS=$<BOOL:${JSON_ImplicitConversions}>
  JSON_DIAGNOSTICS=$<BOOL:${JSON_Diagnostics}>
)

target_include_directories(
  nlohmann_json
  ${NLOHMANN_JSON_SYSTEM_INCLUDE} INTERFACE
  $<BUILD_INTERFACE:${NLOHMANN_JSON_INCLUDE_BUILD_DIR}>
  $<INSTALL_INTERFACE:include>
)
