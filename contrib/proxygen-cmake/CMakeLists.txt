fetchcontent_declare(
  proxygen
  GIT_REPOSITORY https://github.com/facebook/proxygen.git
  GIT_TAG v2022.05.30.00
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
