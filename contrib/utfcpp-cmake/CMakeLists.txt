fetchcontent_declare(
  utfcpp
  GIT_REPOSITORY https://github.com/nemtrif/utfcpp.git
  GIT_TAG v3.1.1
  GIT_PROGRESS TRUE
  SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/utfcpp
)

fetchcontent_getproperties(utfcpp)
if(NOT utfcpp_POPULATED)
  message(STATUS "Fetching utfcpp")
  fetchcontent_populate(utfcpp)
  message(STATUS "Fetching utfcpp - done")
  set(UTFCPP_SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/utfcpp)
endif()


add_library(utf8cpp INTERFACE)

target_include_directories(
  utf8cpp
  SYSTEM
  INTERFACE
  ${UTFCPP_SOURCE_DIR}/source
)

add_library(utf8::cpp ALIAS utf8cpp)
