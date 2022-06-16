fetchcontent_declare(
  glog
  GIT_REPOSITORY https://github.com/google/glog.git
  GIT_TAG v0.6.0
  GIT_PROGRESS TRUE
  SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/glog
)

fetchcontent_getproperties(glog)

if(NOT glog_POPULATED)
  message(STATUS "Fetching glog")
  fetchcontent_populate(glog)
  message(STATUS "Fetching glog - done")
  set(GLOG_SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/glog)
endif()


list(APPEND CMAKE_MODULE_PATH ${GLOG_SOURCE_DIR}/cmake)


include(CheckCXXCompilerFlag)
include(CheckCXXSourceCompiles)
include(CheckCXXSourceRuns)
include(CheckCXXSymbolExists)
include(CheckFunctionExists)
include(CheckIncludeFileCXX)
include(CheckLibraryExists)
include(CheckStructHasMember)
include(CheckTypeSize)
include(CMakeDependentOption)
include(CMakePackageConfigHelpers)
include(CMakePushCheckState)
include(CPack)
include(CTest)
include(DetermineGflagsNamespace)
include(GenerateExportHeader)
include(GetCacheVariables)
include(GNUInstallDirs)

option(WITH_THREADS "Enable multithreading support" ON)
option(WITH_TLS "Enable Thread Local Storage (TLS) support" ON)
option(WITH_UNWIND "Enable libunwind support" ON)

if(NOT WITH_UNWIND)
  set(CMAKE_DISABLE_FIND_PACKAGE_Unwind ON)
endif(NOT WITH_UNWIND)

if(NOT WITH_GTEST)
  set(CMAKE_DISABLE_FIND_PACKAGE_GTest ON)
endif(NOT WITH_GTEST)

if(NOT WITH_THREADS)
  set(CMAKE_DISABLE_FIND_PACKAGE_Threads ON)
endif(NOT WITH_THREADS)

set(CMAKE_C_VISIBILITY_PRESET hidden)
set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_VISIBILITY_INLINES_HIDDEN ON)

set(CMAKE_DEBUG_POSTFIX d)
set(CMAKE_THREAD_PREFER_PTHREAD 1)

find_package(Threads)

check_include_file_cxx(dlfcn.h HAVE_DLFCN_H)
check_include_file_cxx(execinfo.h HAVE_EXECINFO_H)
check_include_file_cxx(glob.h HAVE_GLOB_H)
check_include_file_cxx(inttypes.h HAVE_INTTYPES_H)
check_include_file_cxx(memory.h HAVE_MEMORY_H)
check_include_file_cxx(pwd.h HAVE_PWD_H)
check_include_file_cxx(stdint.h HAVE_STDINT_H)
check_include_file_cxx(strings.h HAVE_STRINGS_H)
check_include_file_cxx(sys/stat.h HAVE_SYS_STAT_H)
check_include_file_cxx(sys/syscall.h HAVE_SYS_SYSCALL_H)
check_include_file_cxx(sys/time.h HAVE_SYS_TIME_H)
check_include_file_cxx(sys/types.h HAVE_SYS_TYPES_H)
check_include_file_cxx(sys/utsname.h HAVE_SYS_UTSNAME_H)
check_include_file_cxx(sys/wait.h HAVE_SYS_WAIT_H)
check_include_file_cxx(syscall.h HAVE_SYSCALL_H)
check_include_file_cxx(syslog.h HAVE_SYSLOG_H)
check_include_file_cxx(ucontext.h HAVE_UCONTEXT_H)
check_include_file_cxx(unistd.h HAVE_UNISTD_H)

check_include_file_cxx("ext/hash_map" HAVE_EXT_HASH_MAP)
check_include_file_cxx("ext/hash_set" HAVE_EXT_HASH_SET)
check_include_file_cxx("ext/slist" HAVE_EXT_SLIST)
check_include_file_cxx("tr1/unordered_map" HAVE_TR1_UNORDERED_MAP)
check_include_file_cxx("tr1/unordered_set" HAVE_TR1_UNORDERED_SET)
check_include_file_cxx("unordered_map" HAVE_UNORDERED_MAP)
check_include_file_cxx("unordered_set" HAVE_UNORDERED_SET)

check_type_size("unsigned __int16" HAVE___UINT16 LANGUAGE CXX)
check_type_size(u_int16_t HAVE_U_INT16_T LANGUAGE CXX)
check_type_size(uint16_t HAVE_UINT16_T LANGUAGE CXX)

check_function_exists(dladdr HAVE_DLADDR)
check_function_exists(fcntl HAVE_FCNTL)
check_function_exists(pread HAVE_PREAD)
check_function_exists(pwrite HAVE_PWRITE)
check_function_exists(sigaction HAVE_SIGACTION)
check_function_exists(sigaltstack HAVE_SIGALTSTACK)

# NOTE gcc does not fail if you pass a non-existent -Wno-* option as an
# argument. However, it will happily fail if you pass the corresponding -W*
# option. So, we check whether options that disable warnings exist by testing
# the availability of the corresponding option that enables the warning. This
# eliminates the need to check for compiler for several (mainly Clang) options.

check_cxx_compiler_flag(-Wdeprecated HAVE_NO_DEPRECATED)
check_cxx_compiler_flag(
  -Wunnamed-type-template-args
  HAVE_NO_UNNAMED_TYPE_TEMPLATE_ARGS
)

cmake_push_check_state(RESET)

if(Threads_FOUND)
  set(CMAKE_REQUIRED_LIBRARIES Threads::Threads)
endif(Threads_FOUND)

check_cxx_symbol_exists(pthread_threadid_np "pthread.h" HAVE_PTHREAD_THREADID_NP)
cmake_pop_check_state()

# NOTE: Cannot use check_function_exists here since >=vc-14.0 can define
# snprintf as an inline function
check_cxx_symbol_exists(snprintf cstdio HAVE_SNPRINTF)

check_library_exists(dbghelp UnDecorateSymbolName "" HAVE_DBGHELP)

check_cxx_source_compiles("
#include <cstdlib>
static void foo(void) __attribute__ ((unused));
int main(void) { return 0; }
" HAVE___ATTRIBUTE__)

check_cxx_source_compiles("
#include <cstdlib>
static void foo(void) __attribute__ ((visibility(\"default\")));
int main(void) { return 0; }
" HAVE___ATTRIBUTE__VISIBILITY_DEFAULT)

check_cxx_source_compiles("
#include <cstdlib>
static void foo(void) __attribute__ ((visibility(\"hidden\")));
int main(void) { return 0; }
" HAVE___ATTRIBUTE__VISIBILITY_HIDDEN)

check_cxx_source_compiles("
int main(void) { if (__builtin_expect(0, 0)) return 1; return 0; }
" HAVE___BUILTIN_EXPECT)

check_cxx_source_compiles("
int main(void)
{
  int a; if (__sync_val_compare_and_swap(&a, 0, 1)) return 1; return 0;
}
" HAVE___SYNC_VAL_COMPARE_AND_SWAP)

cmake_push_check_state(RESET)
set(CMAKE_REQUIRED_LIBRARIES Threads::Threads)
check_cxx_source_compiles("
#define _XOPEN_SOURCE 500
#include <pthread.h>
int main(void)
{
  pthread_rwlock_t l;
  pthread_rwlock_init(&l, NULL);
  pthread_rwlock_rdlock(&l);
  return 0;
}
" HAVE_RWLOCK)
cmake_pop_check_state()

check_cxx_source_compiles("
__declspec(selectany) int a;
int main(void) { return 0; }
" HAVE___DECLSPEC)

check_cxx_source_compiles("
#include <vector>
vector<int> t; int main() { }
" STL_NO_NAMESPACE)

check_cxx_source_compiles("
#include <vector>
std::vector<int> t; int main() { }
" STL_STD_NAMESPACE)

check_cxx_source_compiles("
#include <iostream>
std::ostream& operator<<(std::ostream&, struct s);
using ::operator<<;
int main() { }
" HAVE_USING_OPERATOR)

check_cxx_source_compiles("
namespace Outer { namespace Inner { int i = 0; }}
using namespace Outer::Inner;;
int main() { return i; }
" HAVE_NAMESPACES)

check_cxx_source_compiles("
__thread int tls;
int main() { }
" HAVE_GCC_TLS)

check_cxx_source_compiles("
__declspec(thread) int tls;
int main() { }
" HAVE_MSVC_TLS)

check_cxx_source_compiles("
thread_local int tls;
int main() { }
" HAVE_CXX11_TLS)

check_cxx_source_compiles("
#include <type_traits>
std::aligned_storage<sizeof(char), alignof(char)>::type data;
int main() { }
" HAVE_ALIGNED_STORAGE)

check_cxx_source_compiles("
#include <atomic>
std::atomic<int> i;
int main() { }
" HAVE_CXX11_ATOMIC)

check_cxx_source_compiles("
constexpr int x = 0;
int main() { }
" HAVE_CXX11_CONSTEXPR)

check_cxx_source_compiles("
#include <chrono>
std::chrono::seconds s;
int main() { }
" HAVE_CXX11_CHRONO)

check_cxx_source_compiles("
#include <cstddef>
void foo(std::nullptr_t) {}
int main(void) { foo(nullptr); }
" HAVE_CXX11_NULLPTR_T)

if(WITH_TLS)
  # Cygwin does not support the thread attribute. Don't bother.
  if(HAVE_GCC_TLS)
    set(GLOG_THREAD_LOCAL_STORAGE "__thread")
  elseif(HAVE_MSVC_TLS)
    set(GLOG_THREAD_LOCAL_STORAGE "__declspec(thread)")
  elseif(HAVE_CXX11_TLS)
    set(GLOG_THREAD_LOCAL_STORAGE thread_local)
  endif(HAVE_GCC_TLS)
endif(WITH_TLS)

set(
  _PC_FIELDS
  "gregs[REG_PC]"
  "gregs[REG_EIP]"
  "gregs[REG_RIP]"
  "sc_ip"
  "uc_regs->gregs[PT_NIP]"
  "gregs[R15]"
  "arm_pc"
  "mc_eip"
  "mc_rip"
  "__gregs[REG_EIP]"
  "__gregs[REG_RIP]"
  "ss.eip"
  "__ss.__eip"
  "ss.rip"
  "__ss.__rip"
  "ss.srr0"
  "__ss.__srr0"
)

set(_PC_HEADERS ucontext.h signal.h)

if(HAVE_UCONTEXT_H AND NOT PC_FROM_UCONTEXT)
  foreach(_PC_FIELD ${_PC_FIELDS})
    foreach(_PC_HEADER ${_PC_HEADERS})
      set(
        _TMP
        ${CMAKE_CURRENT_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/CMakeTmp/uctfield.cpp
      )
      file(WRITE ${_TMP} "
#define _GNU_SOURCE 1
#include <${_PC_HEADER}>
int main(void)
{
  ucontext_t u;
  return u.${_PC_FIELD} == 0;
}
")
      try_compile(
        HAVE_PC_FROM_UCONTEXT ${CMAKE_CURRENT_BINARY_DIR} ${_TMP}
        COMPILE_DEFINITIONS _GNU_SOURCE=1
      )

      if(HAVE_PC_FROM_UCONTEXT)
        set(PC_FROM_UCONTEXT ${_PC_FIELD} CACHE)
      endif(HAVE_PC_FROM_UCONTEXT)
    endforeach(_PC_HEADER)
  endforeach(_PC_FIELD)
endif(HAVE_UCONTEXT_H AND NOT PC_FROM_UCONTEXT)

if(STL_STD_NAMESPACE)
  set(STL_NAMESPACE std)
else(STL_STD_NAMESPACE)
  set(STL_NAMESPACE "")
endif(STL_STD_NAMESPACE)

set(GOOGLE_NAMESPACE google)
set(_START_GOOGLE_NAMESPACE_ "namespace ${GOOGLE_NAMESPACE} {")
set(_END_GOOGLE_NAMESPACE_ "}")
set(ac_cv_have_glog_export 1)

if(HAVE___UINT16)
  set(ac_cv_have___uint16 1)
else(HAVE___UINT16)
  set(ac_cv_have___uint16 0)
endif(HAVE___UINT16)

if(HAVE_INTTYPES_H)
  set(ac_cv_have_inttypes_h 1)
else(HAVE_INTTYPES_H)
  set(ac_cv_have_inttypes_h 0)
endif(HAVE_INTTYPES_H)

set(ac_cv_have_libgflags 1)

if(HAVE_STDINT_H)
  set(ac_cv_have_stdint_h 1)
else(HAVE_STDINT_H)
  set(ac_cv_have_stdint_h 0)
endif(HAVE_STDINT_H)

if(HAVE_SYS_TYPES_H)
  set(ac_cv_have_systypes_h 1)
else(HAVE_SYS_TYPES_H)
  set(ac_cv_have_systypes_h 0)
endif(HAVE_SYS_TYPES_H)

if(HAVE_U_INT16_T)
  set(ac_cv_have_u_int16_t 1)
else(HAVE_U_INT16_T)
  set(ac_cv_have_u_int16_t 0)
endif(HAVE_U_INT16_T)

if(HAVE_UINT16_T)
  set(ac_cv_have_uint16_t 1)
else(HAVE_UINT16_T)
  set(ac_cv_have_uint16_t 0)
endif(HAVE_UINT16_T)

if(HAVE_UNISTD_H)
  set(ac_cv_have_unistd_h 1)
else(HAVE_UNISTD_H)
  set(ac_cv_have_unistd_h 0)
endif(HAVE_UNISTD_H)

set(ac_google_namespace ${GOOGLE_NAMESPACE})
set(ac_google_end_namespace ${_END_GOOGLE_NAMESPACE_})
set(ac_google_start_namespace ${_START_GOOGLE_NAMESPACE_})

if(HAVE___ATTRIBUTE__)
  set(ac_cv___attribute___noreturn "__attribute__((noreturn))")
  set(ac_cv___attribute___noinline "__attribute__((noinline))")
  set(ac_cv___attribute___printf_4_5 "__attribute__((__format__(__printf__, 4, 5)))")
elseif(HAVE___DECLSPEC)
  set(ac_cv___attribute___noreturn "__declspec(noreturn)")
  #set (ac_cv___attribute___noinline "__declspec(noinline)")
endif(HAVE___ATTRIBUTE__)

if(HAVE___BUILTIN_EXPECT)
  set(ac_cv_have___builtin_expect 1)
else(HAVE___BUILTIN_EXPECT)
  set(ac_cv_have___builtin_expect 0)
endif(HAVE___BUILTIN_EXPECT)

if(HAVE_USING_OPERATOR)
  set(ac_cv_cxx_using_operator 1)
else(HAVE_USING_OPERATOR)
  set(ac_cv_cxx_using_operator 0)
endif(HAVE_USING_OPERATOR)

if(HAVE_CXX11_CONSTEXPR)
  set(ac_cv_cxx11_constexpr 1)
else(HAVE_CXX11_CONSTEXPR)
  set(ac_cv_cxx11_constexpr 0)
endif(HAVE_CXX11_CONSTEXPR)

if(HAVE_CXX11_CHRONO)
  set(ac_cv_cxx11_chrono 1)
else(HAVE_CXX11_CHRONO)
  set(ac_cv_cxx11_chrono 0)
endif(HAVE_CXX11_CHRONO)

if(HAVE_CXX11_NULLPTR_T)
  set(ac_cv_cxx11_nullptr_t 1)
else(HAVE_CXX11_NULLPTR_T)
  set(ac_cv_cxx11_nullptr_t 0)
endif(HAVE_CXX11_NULLPTR_T)

if(HAVE_EXECINFO_H)
  set(HAVE_STACKTRACE 1)
endif(HAVE_EXECINFO_H)

if(HAVE_CXX11_ATOMIC)
  set(ac_cv_cxx11_atomic 1)
else(HAVE_CXX11_ATOMIC)
  set(ac_cv_cxx11_atomic 0)
endif(HAVE_CXX11_ATOMIC)

if(WITH_SYMBOLIZE)
  if(WIN32 OR CYGWIN)
    cmake_push_check_state(RESET)
    set(CMAKE_REQUIRED_LIBRARIES DbgHelp)

    check_cxx_source_runs([=[
      #include <windows.h>
      #include <dbghelp.h>
      #include <cstdlib>

      void foobar() { }

      int main()
      {
      HANDLE process = GetCurrentProcess();

      if (!SymInitialize(process, NULL, TRUE))
        return EXIT_FAILURE;

        char buf[sizeof(SYMBOL_INFO) + MAX_SYM_NAME];
        SYMBOL_INFO *symbol = reinterpret_cast<SYMBOL_INFO *>(buf);
        symbol->SizeOfStruct = sizeof(SYMBOL_INFO);
        symbol->MaxNameLen = MAX_SYM_NAME;

        void* const pc = reinterpret_cast<void*>(&foobar);
        BOOL ret = SymFromAddr(process, reinterpret_cast<DWORD64>(pc), 0, symbol);

        return ret ? EXIT_SUCCESS : EXIT_FAILURE;
        }
        ]=] HAVE_SYMBOLIZE)

    cmake_pop_check_state()

    if(HAVE_SYMBOLIZE)
      set(HAVE_STACKTRACE 1)
    endif(HAVE_SYMBOLIZE)
  elseif(UNIX OR (APPLE AND HAVE_DLADDR))
    set(HAVE_SYMBOLIZE 1)
  endif(WIN32 OR CYGWIN)
endif(WITH_SYMBOLIZE)


add_definitions(-DGLOG_NO_SYMBOLIZE_DETECTION)

configure_file(${GLOG_SOURCE_DIR}/src/config.h.cmake.in config.h)
configure_file(${GLOG_SOURCE_DIR}/src/glog/logging.h.in glog/logging.h @ONLY)
configure_file(${GLOG_SOURCE_DIR}/src/glog/raw_logging.h.in glog/raw_logging.h @ONLY)
configure_file(${GLOG_SOURCE_DIR}/src/glog/stl_logging.h.in glog/stl_logging.h @ONLY)
configure_file(${GLOG_SOURCE_DIR}/src/glog/vlog_is_on.h.in glog/vlog_is_on.h @ONLY)

add_compile_options($<$<AND:$<BOOL:${HAVE_NO_UNNAMED_TYPE_TEMPLATE_ARGS}>,$<NOT:$<CXX_COMPILER_ID:GNU>>>:-Wno-unnamed-type-template-args>)

set(
  GLOG_PUBLIC_H
  ${CMAKE_CURRENT_BINARY_DIR}/glog/export.h
  ${CMAKE_CURRENT_BINARY_DIR}/glog/logging.h
  ${CMAKE_CURRENT_BINARY_DIR}/glog/raw_logging.h
  ${CMAKE_CURRENT_BINARY_DIR}/glog/stl_logging.h
  ${CMAKE_CURRENT_BINARY_DIR}/glog/vlog_is_on.h
  ${GLOG_SOURCE_DIR}/src/glog/log_severity.h
  ${GLOG_SOURCE_DIR}/src/glog/platform.h
)

set(
  GLOG_SRCS
  ${GLOG_PUBLIC_H}
  ${GLOG_SOURCE_DIR}/src/base/commandlineflags.h
  ${GLOG_SOURCE_DIR}/src/base/googleinit.h
  ${GLOG_SOURCE_DIR}/src/base/mutex.h
  ${GLOG_SOURCE_DIR}/src/demangle.cc
  ${GLOG_SOURCE_DIR}/src/demangle.h
  ${GLOG_SOURCE_DIR}/src/logging.cc
  ${GLOG_SOURCE_DIR}/src/raw_logging.cc
  ${GLOG_SOURCE_DIR}/src/symbolize.cc
  ${GLOG_SOURCE_DIR}/src/symbolize.h
  ${GLOG_SOURCE_DIR}/src/utilities.cc
  ${GLOG_SOURCE_DIR}/src/utilities.h
  ${GLOG_SOURCE_DIR}/src/vlog_is_on.cc
)

set(HAVE_PTHREAD 1)

if(HAVE_PTHREAD OR WIN32 OR CYGWIN)
  list(APPEND GLOG_SRCS ${GLOG_SOURCE_DIR}/src/signalhandler.cc)
endif(HAVE_PTHREAD OR WIN32 OR CYGWIN)

if(CYGWIN OR WIN32)
  list(
    APPEND GLOG_SRCS
    ${GLOG_SOURCE_DIR}/src/windows/port.cc
    ${GLOG_SOURCE_DIR}/src/windows/port.h
  )
endif(CYGWIN OR WIN32)


add_library(
  glog
  STATIC
  ${GLOG_SRCS}
)
target_link_libraries(
  glog
  gflags
)
target_include_directories(
  glog
  PRIVATE ${CMAKE_CURRENT_BINARY_DIR}
  ${GLOG_SOURCE_DIR}/src/
)

target_compile_definitions(
  glog
  PRIVATE
  -DHAVE_PTHREAD
  -DHAVE_LIB_GFLAGS
)

target_include_directories(
  glog
  SYSTEM BEFORE INTERFACE
  ${CMAKE_THIRDPARTY_HEADER_DIR}
)

generate_export_header(
  glog
  EXPORT_MACRO_NAME GLOG_EXPORT
  EXPORT_FILE_NAME ${CMAKE_CURRENT_BINARY_DIR}/glog/export.h
)

add_library(glog::glog ALIAS glog)

foreach(headfile ${GLOG_PUBLIC_H})
  file(
    COPY
    ${headfile}
    DESTINATION ${CMAKE_THIRDPARTY_HEADER_DIR}/glog/
  )
endforeach()
