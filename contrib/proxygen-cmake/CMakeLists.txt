fetchcontent_declare(
  proxygen
  GIT_REPOSITORY https://github.com/facebook/proxygen.git
  GIT_TAG ${FB_LIB_VERSION}
  GIT_PROGRESS TRUE
  SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/proxygen
)

fetchcontent_getproperties(proxygen)

if(NOT proxygen_POPULATED)
  message(STATUS "Fetching proxygen")
  fetchcontent_populate(proxygen)
  message(STATUS "Fetching proxygen - done")
  set(PROXYGEN_SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/proxygen)
endif()

function(proxygen_add_test)
  return()
endfunction()

#mac os
#/bin/env --> /usr/bin/env
#
set(PROXYGEN_GENERATED_ROOT "${CMAKE_BINARY_DIR}/proxygen")
set(PROXYGEN_FBCODE_ROOT ${PROXYGEN_SOURCE_DIR})


add_subdirectory(${PROXYGEN_SOURCE_DIR}/proxygen "${CMAKE_BINARY_DIR}/proxygen/")
#disbale null waring message
target_compile_options(
  proxygen
  PRIVATE
  "-Wno-nullability-completeness"
  "-Wno-implicit-const-int-float-conversion"
)

if(COMPILER_CLANG)
  target_compile_options(
    proxygen
    PRIVATE
    "-Wno-deprecated-register"
    "-Wno-error=register"
  )
endif()
