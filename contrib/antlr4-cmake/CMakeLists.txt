fetchcontent_declare(
  antlr4
  GIT_REPOSITORY https://github.com/antlr/antlr4.git
  GIT_TAG 4.9.3
  GIT_PROGRESS TRUE
  SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/antlr4
)

fetchcontent_getproperties(antlr4)
if(NOT antlr4_POPULATED)
  message(STATUS "Fetching antlr4")
  fetchcontent_populate(antlr4)
  message(STATUS "Fetching antlr4 - done")
  set(ANTLR4_SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/antlr4/runtime/Cpp)
endif()

if(
  CMAKE_VERSION VERSION_EQUAL "3.0.0" OR
  CMAKE_VERSION VERSION_GREATER "3.0.0"
)
  cmake_policy(SET CMP0026 NEW)
  cmake_policy(SET CMP0054 OLD)
  cmake_policy(SET CMP0045 OLD)
  cmake_policy(SET CMP0042 OLD)
endif()

if(
  CMAKE_VERSION VERSION_EQUAL "3.3.0" OR
  CMAKE_VERSION VERSION_GREATER "3.3.0"
)
  cmake_policy(SET CMP0059 OLD)
  cmake_policy(SET CMP0054 OLD)
endif()

if(CMAKE_SYSTEM_NAME MATCHES "Linux")
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(UUID REQUIRED uuid)
endif()

if(APPLE)
  find_library(COREFOUNDATION_LIBRARY CoreFoundation)
endif()

set(ANTLR_VERSION "4.9.3")

# Initialize CXXFLAGS.
if("${CMAKE_VERSION}" VERSION_GREATER 3.1.0)
  if(NOT DEFINED CMAKE_CXX_STANDARD)
    # only set CMAKE_CXX_STANDARD if not already set
    # this allows the standard to be set by the caller, for example with -DCMAKE_CXX_STANDARD:STRING=17
    set(CMAKE_CXX_STANDARD 11)
  endif()
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
  set(CMAKE_CXX_EXTENSIONS OFF)
else()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -std=c++11")
  set(CMAKE_CXX_FLAGS_MINSIZEREL "${CMAKE_CXX_FLAGS_MINSIZEREL} -std=c++11")
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -std=c++11")
  set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -std=c++11")
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${MY_CXX_WARNING_FLAGS}")
if(MSVC_VERSION)
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /Od /Zi /MP ${MY_CXX_WARNING_FLAGS}")
  set(CMAKE_CXX_FLAGS_MINSIZEREL "${CMAKE_CXX_FLAGS_MINSIZEREL} /O1 /Oi /Ob2 /Gy /MP /DNDEBUG ${MY_CXX_WARNING_FLAGS}")
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /O2 /Oi /Ob2 /Gy /MP /DNDEBUG ${MY_CXX_WARNING_FLGAS}")
  set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} /O2 /Oi /Ob2 /Gy /MP /Zi ${MY_CXX_WARNING_FLAGS}")
else()
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g ${MY_CXX_WARNING_FLAGS}")
  set(CMAKE_CXX_FLAGS_MINSIZEREL "${CMAKE_CXX_FLAGS_MINSIZEREL} -Os -DNDEBUG ${MY_CXX_WARNING_FLAGS}")
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -DNDEBUG ${MY_CXX_WARNING_FLGAS}")
  set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -O2 -g ${MY_CXX_WARNING_FLAGS}")
endif()

# Compiler-specific C++11 activation.
if("${CMAKE_CXX_COMPILER_ID}" MATCHES "GNU" OR "${CMAKE_CXX_COMPILER_ID}" MATCHES "Intel")
  execute_process(COMMAND ${CMAKE_CXX_COMPILER} -dumpversion OUTPUT_VARIABLE GCC_VERSION)
  # Just g++-5.0 and greater contain <codecvt> header. (test in ubuntu)
  if(NOT (GCC_VERSION VERSION_GREATER 5.0 OR GCC_VERSION VERSION_EQUAL 5.0))
    message(FATAL_ERROR "${PROJECT_NAME} requires g++ 5.0 or greater.")
  endif()
elseif("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang" AND ANDROID)
  # Need -Os cflag and cxxflags here to work with exception handling on armeabi.
  # see  https://github.com/android-ndk/ndk/issues/573
  # and without -stdlib=libc++ cxxflags
elseif("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang" AND APPLE)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -stdlib=libc++")
elseif("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang" AND ( CMAKE_SYSTEM_NAME MATCHES "Linux" OR CMAKE_SYSTEM_NAME MATCHES "FreeBSD"))
  execute_process(COMMAND ${CMAKE_CXX_COMPILER} -dumpversion OUTPUT_VARIABLE CLANG_VERSION)
  if(NOT (CLANG_VERSION VERSION_GREATER 4.2.1 OR CLANG_VERSION VERSION_EQUAL 4.2.1))
    message(FATAL_ERROR "${PROJECT_NAME} requires clang 4.2.1 or greater.")
  endif()
  # You can use libc++ to compile this project when g++ is NOT greater than or equal to 5.0.
  if(WITH_LIBCXX)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++")
  endif()
elseif(MSVC_VERSION GREATER 1800 OR MSVC_VERSION EQUAL 1800)
  # Visual Studio 2012+ supports c++11 features
elseif(CMAKE_SYSTEM_NAME MATCHES "Emscripten")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -stdlib=libc++")
else()
  message(FATAL_ERROR "Your C++ compiler does not support C++11.")
endif()

set(
  ANTLR4_INCLUDE_DIR
  ${ANTLR4_SOURCE_DIR}/runtime/src
  ${ANTLR4_SOURCE_DIR}/runtime/src/atn
  ${ANTLR4_SOURCE_DIR}/runtime/src/dfa
  ${ANTLR4_SOURCE_DIR}/runtime/src/misc
  ${ANTLR4_SOURCE_DIR}/runtime/src/support
  ${ANTLR4_SOURCE_DIR}/runtime/src/tree
  ${ANTLR4_SOURCE_DIR}/runtime/src/tree/pattern
  ${ANTLR4_SOURCE_DIR}/runtime/src/tree/xpath
)

file(
  GLOB libantlrcpp_SRC
  "${ANTLR4_SOURCE_DIR}/runtime/src/*.cpp"
  "${ANTLR4_SOURCE_DIR}/runtime/src/atn/*.cpp"
  "${ANTLR4_SOURCE_DIR}/runtime/src/dfa/*.cpp"
  "${ANTLR4_SOURCE_DIR}/runtime/src/misc/*.cpp"
  "${ANTLR4_SOURCE_DIR}/runtime/src/support/*.cpp"
  "${ANTLR4_SOURCE_DIR}/runtime/src/tree/*.cpp"
  "${ANTLR4_SOURCE_DIR}/runtime/src/tree/pattern/*.cpp"
  "${ANTLR4_SOURCE_DIR}/runtime/src/tree/xpath/*.cpp"
)

add_library(antlr4_static STATIC ${libantlrcpp_SRC})
target_include_directories(
  antlr4_static
  PRIVATE
  ${ANTLR4_INCLUDE_DIR}
)
target_include_directories(
  antlr4_static
  SYSTEM
  PUBLIC
  ${ANTLR4_INCLUDE_DIR}
)
target_link_libraries(antlr4_static utf8cpp)
add_library(antlr4 ALIAS antlr4_static)
