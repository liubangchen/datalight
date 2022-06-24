fetchcontent_declare(
  re2
  GIT_REPOSITORY https://github.com/google/re2.git
  GIT_TAG 2021-11-01
  GIT_PROGRESS TRUE
  SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/re2
)

fetchcontent_getproperties(re2)

if(NOT re2_POPULATED)
  message(STATUS "Fetching re2")
  fetchcontent_populate(re2)
  message(STATUS "Fetching re2 - done")
  set(RE2_SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/re2)
endif()

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

set(
  RE2_SOURCES
  ${RE2_SOURCE_DIR}/re2/bitstate.cc
  ${RE2_SOURCE_DIR}/re2/compile.cc
  ${RE2_SOURCE_DIR}/re2/dfa.cc
  ${RE2_SOURCE_DIR}/re2/filtered_re2.cc
  ${RE2_SOURCE_DIR}/re2/mimics_pcre.cc
  ${RE2_SOURCE_DIR}/re2/nfa.cc
  ${RE2_SOURCE_DIR}/re2/onepass.cc
  ${RE2_SOURCE_DIR}/re2/parse.cc
  ${RE2_SOURCE_DIR}/re2/perl_groups.cc
  ${RE2_SOURCE_DIR}/re2/prefilter.cc
  ${RE2_SOURCE_DIR}/re2/prefilter_tree.cc
  ${RE2_SOURCE_DIR}/re2/prog.cc
  ${RE2_SOURCE_DIR}/re2/re2.cc
  ${RE2_SOURCE_DIR}/re2/regexp.cc
  ${RE2_SOURCE_DIR}/re2/set.cc
  ${RE2_SOURCE_DIR}/re2/simplify.cc
  ${RE2_SOURCE_DIR}/re2/stringpiece.cc
  ${RE2_SOURCE_DIR}/re2/tostring.cc
  ${RE2_SOURCE_DIR}/re2/unicode_casefold.cc
  ${RE2_SOURCE_DIR}/re2/unicode_groups.cc
  ${RE2_SOURCE_DIR}/util/rune.cc
  ${RE2_SOURCE_DIR}/util/strutil.cc
)

add_library(re2 ${RE2_SOURCES})

target_include_directories(
  re2
  SYSTEM
  PUBLIC
  ${RE2_SOURCE_DIR}
)
add_library(re2::re2 ALIAS re2)

if(UNIX)
  target_link_libraries(re2 PUBLIC Threads::Threads)
endif()
