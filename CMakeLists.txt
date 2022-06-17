cmake_minimum_required(VERSION 3.21)

foreach(
  policy
  CMP0023
  CMP0042
  CMP0048 # CMake 3.0
  CMP0074 # CMake 3.12
  CMP0077
  CMP0079
)
  if(POLICY ${policy})
    cmake_policy(SET ${policy} NEW)
  endif()
endforeach()

# set default policy
foreach(
  default_policy_var_name
  CMAKE_POLICY_DEFAULT_CMP0077
  CMAKE_POLICY_DEFAULT_CMP0022
)
  set(${default_policy_var_name} NEW)
endforeach()

project(datalight LANGUAGES C CXX ASM)
set(CMAKE_MACOSX_RPATH 1)
#set(CMAKE_POSITION_INDEPENDENT_CODE OFF)
#set(CMAKE_LINK_LIBRARIES_ONLY_TARGETS ON)

option(UNBUNDLED "Use system libraries instead of ones in contrib/" OFF)


set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake/modules/")

message(
  STATUS
  "Building for: ${CMAKE_SYSTEM} ${CMAKE_SYSTEM_PROCESSOR} ${CMAKE_LIBRARY_ARCHITECTURE} ;
    USE_STATIC_LIBRARIES=${USE_STATIC_LIBRARIES}
    MAKE_STATIC_LIBRARIES=${MAKE_STATIC_LIBRARIES}
    SPLIT_SHARED=${SPLIT_SHARED_LIBRARIES}
    UNBUNDLED=${UNBUNDLED}
    CCACHE=${CCACHE_FOUND} ${CCACHE_VERSION}"
)

include(cmake/arch.cmake)
include(cmake/target.cmake)
include(cmake/tools.cmake)
include(cmake/analysis.cmake)
include(cmake/common.cmake)

message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
set(CMAKE_EXPORT_COMPILE_COMMANDS 1)
set(CMAKE_LINK_DEPENDS_NO_SHARED 1) # Do not relink all depended targets on .so
set(CMAKE_CONFIGURATION_TYPES "RelWithDebInfo;Debug;Release;MinSizeRel" CACHE STRING "" FORCE)
set(CMAKE_DEBUG_POSTFIX "d" CACHE STRING "Generate debug library name with a postfix.")    # To be consistent with CMakeLists from contrib libs.

#static libraries path
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/thirdparty/lib)
#shared libraries path
set(CMAKE_INSTALL_CMAKEDIR ${CMAKE_BINARY_DIR}/cmake)
set(CMAKE_THIRDPARTY_HEADER_DIR ${CMAKE_BINARY_DIR}/thirdparty/include)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/thirdparty/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)




# cmake < 3.12 doesn't support 20. We'll set CMAKE_CXX_FLAGS for now
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_C_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=gnu++17")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=c17")

if(COMPILER_CLANG)
  # clang: warning: argument unused during compilation: '-specs=/usr/share/dpkg/no-pie-compile.specs' [-Wunused-command-line-argument]
  set(COMMON_WARNING_FLAGS "${COMMON_WARNING_FLAGS} -Wno-unused-command-line-argument")
  # generate ranges for fast "addr2line" search
  if(NOT CMAKE_BUILD_TYPE_UC STREQUAL "RELEASE")
    set(COMPILER_FLAGS "${COMPILER_FLAGS} -gdwarf-aranges")
  endif()

  if(OS_DARWIN)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-U,_inside_main")
  endif()

  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fdiagnostics-absolute-paths")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fdiagnostics-absolute-paths")

  if(NOT ENABLE_TESTS AND NOT SANITIZE)
    # https://clang.llvm.org/docs/ThinLTO.html
    # Applies to clang only.
    # Disabled when building with tests or sanitizers.
    option(ENABLE_THINLTO "Clang-specific link time optimization" ON)
  endif()

  # Set new experimental pass manager, it's a performance, build time and binary size win.
  # Can be removed after https://reviews.llvm.org/D66490 merged and released to at least two versions of clang.
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fexperimental-new-pass-manager")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fexperimental-new-pass-manager")

  # We cannot afford to use LTO when compiling unit tests, and it's not enough
  # to only supply -fno-lto at the final linking stage. So we disable it
  # completely.
  if(ENABLE_THINLTO AND NOT ENABLE_TESTS AND NOT SANITIZE)
    # Link time optimization
    set(CMAKE_C_FLAGS_RELWITHDEBINFO "${CMAKE_C_FLAGS_RELWITHDEBINFO} -flto=thin")
    set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -flto=thin")
    set(CMAKE_EXE_LINKER_FLAGS_RELWITHDEBINFO "${CMAKE_EXE_LINKER_FLAGS_RELWITHDEBINFO} -flto=thin")
  elseif(ENABLE_THINLTO)
    message(${RECONFIGURE_MESSAGE_LEVEL} "Cannot enable ThinLTO")
  endif()


elseif(ENABLE_THINLTO)
  message(${RECONFIGURE_MESSAGE_LEVEL} "ThinLTO is only available with CLang")
endif()

add_subdirectory(contrib EXCLUDE_FROM_ALL)

#find_package(
#  Boost
#  1.66.0
#  REQUIRED
#  program_options
#  context
#  filesystem
#  regex
#  thread
#  system
#  date_time
#  atomic
#)

#find_package(gflags)
#find_package(glog)
#find_library(FMT fmt)
#find_library(EVENT event)

#find_library(DOUBLE_CONVERSION double-conversion)

add_subdirectory(src/demo)
