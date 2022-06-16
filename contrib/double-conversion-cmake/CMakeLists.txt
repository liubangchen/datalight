fetchcontent_declare(
  doubleconversion
  GIT_REPOSITORY https://github.com/google/double-conversion.git
  GIT_TAG v3.1.5
  GIT_PROGRESS TRUE
  SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/double-conversion
)

fetchcontent_getproperties(doubleconversion)
if(NOT doubleconversion_POPULATED)
  message(STATUS "Fetching double-conversion")
  fetchcontent_populate(doubleconversion)
  message(STATUS "Fetching double-conversion - done")
  set(DOUBLE_CONVERSION_SOURCE_DIR ${${PROJECT_SOURCE_DIR}/contrib/double-conversion})
endif()

set(
  headers
  ${DOUBLE_CONVERSION_SOURCE_DIR}/double-conversion/bignum.h
  ${DOUBLE_CONVERSION_SOURCE_DIR}/double-conversion/cached-powers.h
  ${DOUBLE_CONVERSION_SOURCE_DIR}/double-conversion/diy-fp.h
  ${DOUBLE_CONVERSION_SOURCE_DIR}/double-conversion/double-conversion.h
  ${DOUBLE_CONVERSION_SOURCE_DIR}/double-conversion/fast-dtoa.h
  ${DOUBLE_CONVERSION_SOURCE_DIR}/double-conversion/fixed-dtoa.h
  ${DOUBLE_CONVERSION_SOURCE_DIR}/double-conversion/ieee.h
  ${DOUBLE_CONVERSION_SOURCE_DIR}/double-conversion/strtod.h
  ${DOUBLE_CONVERSION_SOURCE_DIR}/double-conversion/utils.h
)

add_library(
  double-conversion
  ${DOUBLE_CONVERSION_SOURCE_DIR}/double-conversion/bignum.cc
  ${DOUBLE_CONVERSION_SOURCE_DIR}/double-conversion/bignum-dtoa.cc
  ${DOUBLE_CONVERSION_SOURCE_DIR}/double-conversion/cached-powers.cc
  ${DOUBLE_CONVERSION_SOURCE_DIR}/double-conversion/diy-fp.cc
  ${DOUBLE_CONVERSION_SOURCE_DIR}/double-conversion/double-conversion.cc
  ${DOUBLE_CONVERSION_SOURCE_DIR}/double-conversion/fast-dtoa.cc
  ${DOUBLE_CONVERSION_SOURCE_DIR}/double-conversion/fixed-dtoa.cc
  ${DOUBLE_CONVERSION_SOURCE_DIR}/double-conversion/strtod.cc
  ${headers}
)

set_target_properties(double-conversion PROPERTIES VERSION "3.1.5" SOVERSION 3)

target_include_directories(
  double-conversion
  SYSTEM BEFORE INTERFACE
  ${DOUBLE_CONVERSION_SOURCE_DIR}
)
