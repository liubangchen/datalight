set(BOOST_VERSION "1_76_0")

option(BOOST_DISABLE_TESTS "Do not build test targets, even if building standalone" OFF)

set(BOOST_URL "https://boostorg.jfrog.io/artifactory/main/release/1.76.0/source/boost_1_76_0.tar.gz" CACHE STRING "Boost download URL")
set(BOOST_URL_SHA256 "7bd7ddceec1a1dfdcbdb3e609b60d01739c38390a5f956385a12f3122049f0ca" CACHE STRING "Boost download URL SHA256 checksum")
fetchcontent_declare(
  Boost
  URL ${BOOST_URL}
  URL_HASH SHA256=${BOOST_URL_SHA256}
  SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/boost
)

fetchcontent_getproperties(Boost)

if(NOT Boost_POPULATED)
  message(STATUS "Fetching Boost")
  fetchcontent_populate(Boost)
  message(STATUS "Fetching Boost - done")
  set(BOOST_SOURCE_DIR "${PROJECT_SOURCE_DIR}/contrib/boost")
endif()

#message(STATUS "Boost found: ${BOOST_VERSION} ${BOOST_SOURCE}")
# filesystem
#execute_process

set(
  SRCS_FILESYSTEM
  "${BOOST_SOURCE_DIR}/libs/filesystem/src/codecvt_error_category.cpp"
  "${BOOST_SOURCE_DIR}/libs/filesystem/src/directory.cpp"
  "${BOOST_SOURCE_DIR}/libs/filesystem/src/exception.cpp"
  "${BOOST_SOURCE_DIR}/libs/filesystem/src/operations.cpp"
  "${BOOST_SOURCE_DIR}/libs/filesystem/src/path.cpp"
  "${BOOST_SOURCE_DIR}/libs/filesystem/src/path_traits.cpp"
  "${BOOST_SOURCE_DIR}/libs/filesystem/src/portability.cpp"
  "${BOOST_SOURCE_DIR}/libs/filesystem/src/unique_path.cpp"
  "${BOOST_SOURCE_DIR}/libs/filesystem/src/utf8_codecvt_facet.cpp"
  "${BOOST_SOURCE_DIR}/libs/filesystem/src/windows_file_codecvt.cpp"
)

add_library(_boost_filesystem ${SRCS_FILESYSTEM})
add_library(boost::filesystem ALIAS _boost_filesystem)
add_library(Boost::filesystem ALIAS _boost_filesystem)
target_include_directories(_boost_filesystem SYSTEM BEFORE PUBLIC ${BOOST_SOURCE_DIR})

# headers-only

add_library(_boost_headers_only INTERFACE)
add_library(boost::headers_only ALIAS _boost_headers_only)
target_include_directories(_boost_headers_only SYSTEM BEFORE INTERFACE ${BOOST_SOURCE_DIR})

# asio
target_compile_definitions(
  _boost_headers_only INTERFACE
  BOOST_ASIO_STANDALONE=1
  # Avoid using of deprecated in c++ > 17 std::result_of
  BOOST_ASIO_HAS_STD_INVOKE_RESULT=1
)

# iostreams
set(
  SRCS_IOSTREAMS
  "${BOOST_SOURCE_DIR}/libs/iostreams/src/file_descriptor.cpp"
  "${BOOST_SOURCE_DIR}/libs/iostreams/src/gzip.cpp"
  "${BOOST_SOURCE_DIR}/libs/iostreams/src/mapped_file.cpp"
  "${BOOST_SOURCE_DIR}/libs/iostreams/src/zlib.cpp"
)

add_library(_boost_iostreams ${SRCS_IOSTREAMS})
add_library(boost::iostreams ALIAS _boost_iostreams)
add_library(Boost::iostreams ALIAS _boost_iostreams)
target_include_directories(_boost_iostreams PRIVATE ${BOOST_SOURCE_DIR})
#target_link_libraries(_boost_iostreams PRIVATE ch_contrib::zlib)

# program_options

set(
  SRCS_PROGRAM_OPTIONS
  "${BOOST_SOURCE_DIR}/libs/program_options/src/cmdline.cpp"
  "${BOOST_SOURCE_DIR}/libs/program_options/src/config_file.cpp"
  "${BOOST_SOURCE_DIR}/libs/program_options/src/convert.cpp"
  "${BOOST_SOURCE_DIR}/libs/program_options/src/options_description.cpp"
  "${BOOST_SOURCE_DIR}/libs/program_options/src/parsers.cpp"
  "${BOOST_SOURCE_DIR}/libs/program_options/src/positional_options.cpp"
  "${BOOST_SOURCE_DIR}/libs/program_options/src/split.cpp"
  "${BOOST_SOURCE_DIR}/libs/program_options/src/utf8_codecvt_facet.cpp"
  "${BOOST_SOURCE_DIR}/libs/program_options/src/value_semantic.cpp"
  "${BOOST_SOURCE_DIR}/libs/program_options/src/variables_map.cpp"
  "${BOOST_SOURCE_DIR}/libs/program_options/src/winmain.cpp"
)

add_library(_boost_program_options ${SRCS_PROGRAM_OPTIONS})
add_library(boost::program_options ALIAS _boost_program_options)
add_library(Boost::program_options ALIAS _boost_program_options)
target_include_directories(_boost_program_options SYSTEM BEFORE PUBLIC ${BOOST_SOURCE_DIR} ${BOOST_SOURCE_DIR}/libs/program_options/include)

# regex

set(
  SRCS_REGEX
  "${BOOST_SOURCE_DIR}/libs/regex/src/posix_api.cpp"
  "${BOOST_SOURCE_DIR}/libs/regex/src/regex_debug.cpp"
  "${BOOST_SOURCE_DIR}/libs/regex/src/regex.cpp"
  "${BOOST_SOURCE_DIR}/libs/regex/src/static_mutex.cpp"
  "${BOOST_SOURCE_DIR}/libs/regex/src/wide_posix_api.cpp"
)

add_library(_boost_regex ${SRCS_REGEX})
add_library(boost::regex ALIAS _boost_regex)
add_library(Boost::regex ALIAS _boost_regex)
target_include_directories(_boost_regex PRIVATE ${BOOST_SOURCE_DIR})

#thread
set(
  SRCS_THREAD
  "${BOOST_SOURCE_DIR}/libs/thread/src/future.cpp"
  "${BOOST_SOURCE_DIR}/libs/thread/src/tss_null.cpp"
  "${BOOST_SOURCE_DIR}/libs/thread/src/pthread/once.cpp"
  "${BOOST_SOURCE_DIR}/libs/thread/src/pthread/once_atomic.cpp"
  "${BOOST_SOURCE_DIR}/libs/thread/src/pthread/thread.cpp"
)

add_library(_boost_thread ${SRCS_THREAD})
add_library(boost::thread ALIAS _boost_thread)
add_library(Boost::thread ALIAS _boost_thread)
target_include_directories(_boost_thread PRIVATE ${BOOST_SOURCE_DIR})


#date_time
set(
  SRCS_DATETIME
  #  "${BOOST_SOURCE_DIR}/libs/date_time/src/gregorian/date_generators.cpp"
  #  "${BOOST_SOURCE_DIR}/libs/date_time/src/gregorian/greg_month.cpp"
  #  "${BOOST_SOURCE_DIR}/libs/date_time/src/gregorian/greg_names.hpp"
  #  "${BOOST_SOURCE_DIR}/libs/date_time/src/gregorian/greg_weekday.cpp"
  #  "${BOOST_SOURCE_DIR}/libs/date_time/src/gregorian/gregorian_types.cpp"
  "${BOOST_SOURCE_DIR}/libs/date_time/src/posix_time/posix_time_types.cpp"
)

add_library(_boost_date_time ${SRCS_DATETIME})
add_library(boost::date_time ALIAS _boost_date_time)
add_library(Boost::date_time ALIAS _boost_date_time)
target_include_directories(_boost_date_time PRIVATE ${BOOST_SOURCE_DIR})

#atomic
set(
  SRCS_ATOMIC
  "${BOOST_SOURCE_DIR}/libs/atomic/src/bit_operation_tools.hpp"
  "${BOOST_SOURCE_DIR}/libs/atomic/src/cpuid.hpp"
  "${BOOST_SOURCE_DIR}/libs/atomic/src/find_address.hpp"
  "${BOOST_SOURCE_DIR}/libs/atomic/src/find_address_sse2.cpp"
  "${BOOST_SOURCE_DIR}/libs/atomic/src/find_address_sse41.cpp"
  "${BOOST_SOURCE_DIR}/libs/atomic/src/lock_pool.cpp"
  #"${BOOST_SOURCE_DIR}/libs/atomic/src/lock_pool_init1.ipp"
  #"${BOOST_SOURCE_DIR}/libs/atomic/src/lock_pool_init256.ipp"
  #"${BOOST_SOURCE_DIR}/libs/atomic/src/wait_ops_windows.cpp"
  "${BOOST_SOURCE_DIR}/libs/atomic/src/x86_vector_tools.hpp"
)

add_library(_boost_atomic ${SRCS_ATOMIC})
add_library(boost::atomic ALIAS _boost_atomic)
add_library(Boost::atomic ALIAS _boost_atomic)
target_include_directories(_boost_atomic PRIVATE ${BOOST_SOURCE_DIR})
target_include_directories(_boost_atomic PRIVATE ${BOOST_SOURCE_DIR}/libs/atomic/src)

# system
set(
  SRCS_SYSTEM
  "${BOOST_SOURCE_DIR}/libs/system/src/error_code.cpp"
)

add_library(_boost_system ${SRCS_SYSTEM})
add_library(boost::system ALIAS _boost_system)
add_library(Boost::system ALIAS _boost_system)
target_include_directories(_boost_system PRIVATE ${BOOST_SOURCE_DIR})

# context
enable_language(ASM)
set(ASM_OPTIONS "-x assembler-with-cpp")

set(
  SRCS_CONTEXT
  "${BOOST_SOURCE_DIR}/libs/context/src/dummy.cpp"
  "${BOOST_SOURCE_DIR}/libs/context/src/posix/stack_traits.cpp"
)

if(SANITIZE AND (SANITIZE STREQUAL "address" OR SANITIZE STREQUAL "thread"))
  add_compile_definitions(BOOST_USE_UCONTEXT)

  if(SANITIZE STREQUAL "address")
    add_compile_definitions(BOOST_USE_ASAN)
  elseif(SANITIZE STREQUAL "thread")
    add_compile_definitions(BOOST_USE_TSAN)
  endif()

  set(
    SRCS_CONTEXT ${SRCS_CONTEXT}
    "${BOOST_SOURCE_DIR}/libs/context/src/fiber.cpp"
    "${BOOST_SOURCE_DIR}/libs/context/src/continuation.cpp"
  )
endif()
message("plantform(${CMAKE_SYSTEM_PROCESSOR}): ARCH_AARCH64 : ${ARCH_AARCH64}\t ARCH_PPC64LE : ${ARCH_PPC64LE} \t ARCH_RISCV64 : ${ARCH_RISCV64} \t OS_DARWIN : ${OS_DARWIN}")

if(ARCH_AARCH64)
  if(OS_DARWIN)
    set(
      SRCS_CONTEXT ${SRCS_CONTEXT}
      "${BOOST_SOURCE_DIR}/libs/context/src/asm/jump_arm64_aapcs_macho_gas.S"
      "${BOOST_SOURCE_DIR}/libs/context/src/asm/make_arm64_aapcs_macho_gas.S"
      "${BOOST_SOURCE_DIR}/libs/context/src/asm/ontop_arm64_aapcs_macho_gas.S"
    )
  else()
    set(
      SRCS_CONTEXT ${SRCS_CONTEXT}
      "${BOOST_SOURCE_DIR}/libs/context/src/asm/jump_arm64_aapcs_elf_gas.S"
      "${BOOST_SOURCE_DIR}/libs/context/src/asm/make_arm64_aapcs_elf_gas.S"
      "${BOOST_SOURCE_DIR}/libs/context/src/asm/ontop_arm64_aapcs_elf_gas.S"
    )
  endif()
elseif(ARCH_PPC64LE)
  set(
    SRCS_CONTEXT ${SRCS_CONTEXT}
    "${BOOST_SOURCE_DIR}/libs/context/src/asm/jump_ppc64_sysv_elf_gas.S"
    "${BOOST_SOURCE_DIR}/libs/context/src/asm/make_ppc64_sysv_elf_gas.S"
    "${BOOST_SOURCE_DIR}/libs/context/src/asm/ontop_ppc64_sysv_elf_gas.S"
  )
elseif(ARCH_RISCV64)
  set(
    SRCS_CONTEXT ${SRCS_CONTEXT}
    "${BOOST_SOURCE_DIR}/libs/context/src/asm/jump_riscv64_sysv_elf_gas.S"
    "${BOOST_SOURCE_DIR}/libs/context/src/asm/make_riscv64_sysv_elf_gas.S"
    "${BOOST_SOURCE_DIR}/libs/context/src/asm/ontop_riscv64_sysv_elf_gas.S"
  )
elseif(OS_DARWIN)
  set(
    SRCS_CONTEXT ${SRCS_CONTEXT}
    "${BOOST_SOURCE_DIR}/libs/context/src/asm/jump_x86_64_sysv_macho_gas.S"
    "${BOOST_SOURCE_DIR}/libs/context/src/asm/make_x86_64_sysv_macho_gas.S"
    "${BOOST_SOURCE_DIR}/libs/context/src/asm/ontop_x86_64_sysv_macho_gas.S"
  )
else()
  set(
    SRCS_CONTEXT ${SRCS_CONTEXT}
    "${BOOST_SOURCE_DIR}/libs/context/src/asm/jump_x86_64_sysv_elf_gas.S"
    "${BOOST_SOURCE_DIR}/libs/context/src/asm/make_x86_64_sysv_elf_gas.S"
    "${BOOST_SOURCE_DIR}/libs/context/src/asm/ontop_x86_64_sysv_elf_gas.S"
  )
endif()

add_library(_boost_context ${SRCS_CONTEXT})
add_library(boost::context ALIAS _boost_context)
add_library(Boost::context ALIAS _boost_context)
target_include_directories(_boost_context PRIVATE ${BOOST_SOURCE_DIR})



# graph

set(
  SRCS_GRAPH
  "${BOOST_SOURCE_DIR}/libs/graph/src/graphml.cpp"
  "${BOOST_SOURCE_DIR}/libs/graph/src/read_graphviz_new.cpp"
)

add_library(_boost_graph ${SRCS_GRAPH})
add_library(boost::graph ALIAS _boost_graph)
add_library(Boost::graph ALIAS _boost_graph)
target_include_directories(_boost_graph PRIVATE ${BOOST_SOURCE_DIR})
target_link_libraries(_boost_graph PRIVATE _boost_regex)

# circular buffer
add_library(_boost_circular_buffer INTERFACE)
add_library(boost::circular_buffer ALIAS _boost_circular_buffer)
add_library(Boost::circular_buffer ALIAS _boost_circular_buffer)
target_include_directories(_boost_circular_buffer SYSTEM BEFORE INTERFACE ${BOOST_SOURCE_DIR})

# heap
add_library(_boost_heap INTERFACE)
add_library(boost::heap ALIAS _boost_heap)
add_library(Boost::heap ALIAS _boost_heap)
target_include_directories(_boost_heap SYSTEM BEFORE INTERFACE ${BOOST_SOURCE_DIR})
