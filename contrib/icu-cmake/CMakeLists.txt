fetchcontent_declare(
  icu4c
  GIT_REPOSITORY https://github.com/unicode-org/icu.git
  GIT_TAG release-71-1
  GIT_PROGRESS TRUE
  SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/icu4c
)

fetchcontent_getproperties(icu4c)

if(NOT icu4c_POPULATED)
  message(STATUS "Fetching icu4c")
  fetchcontent_populate(icu4c)
  message(STATUS "Fetching icu4c - done")
  set(ICU4C_SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/icu4c)
endif()

set(ICU_SOURCE_DIR "${ICU4C_SOURCE_DIR}/icu4c/source")
#set(ICUDATA_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/icudata/")

file(GLOB ICU_COMMON_SRCS ${ICU_SOURCE_DIR}/common/*.cpp)

file(GLOB ICU_I18N_SRCS ${ICU_SOURCE_DIR}/i18n/*.cpp)


add_library(_icuc STATIC ${ICU_COMMON_SRCS})
target_compile_definitions(
  _icuc PUBLIC
  -DU_DEFINE_FALSE_AND_TRUE
  -DU_COMMON_IMPLEMENTATION
)

target_include_directories(
  _icuc PRIVATE
  ${ICU_SOURCE_DIR}/common/
)

target_include_directories(
  _icuc
  SYSTEM
  PUBLIC
  ${ICU_SOURCE_DIR}/common/
)


add_library(_icui18n STATIC ${ICU_I18N_SRCS})
target_compile_definitions(
  _icui18n PUBLIC
  -DU_DEFINE_FALSE_AND_TRUE
  -DU_I18N_IMPLEMENTATION
  #  -DU_COMMON_IMPLEMENTATION
)

target_include_directories(
  _icui18n
  PRIVATE
  ${ICU_SOURCE_DIR}/common/
  ${ICU_SOURCE_DIR}/i18n/
)

target_include_directories(
  _icui18n
  SYSTEM
  PUBLIC
  ${ICU_SOURCE_DIR}/common/
  ${ICU_SOURCE_DIR}/i18n/
)
