fetchcontent_declare(
  snappy
  GIT_REPOSITORY https://github.com/google/snappy.git
  GIT_TAG 1.1.9
  GIT_PROGRESS TRUE
  SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/snappy
)

fetchcontent_getproperties(snappy)

if(NOT snappy_POPULATED)
  message(STATUS "Fetching snappy")
  fetchcontent_populate(snappy)
  message(STATUS "Fetching snappy - done")
  set(SNAPPY_SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/snappy)
endif()


include(CheckIncludeFile)
check_include_file("sys/mman.h" HAVE_SYS_MMAN_H)
check_include_file("sys/resource.h" HAVE_SYS_RESOURCE_H)
check_include_file("sys/time.h" HAVE_SYS_TIME_H)
check_include_file("sys/uio.h" HAVE_SYS_UIO_H)
check_include_file("unistd.h" HAVE_UNISTD_H)
check_include_file("windows.h" HAVE_WINDOWS_H)

include(CheckCXXCompilerFlag)
check_cxx_compiler_flag("/arch:AVX" HAVE_VISUAL_STUDIO_ARCH_AVX)
check_cxx_compiler_flag("/arch:AVX2" HAVE_VISUAL_STUDIO_ARCH_AVX2)
check_cxx_compiler_flag("-mavx" HAVE_CLANG_MAVX)
check_cxx_compiler_flag("-mbmi2" HAVE_CLANG_MBMI2)
if(SNAPPY_REQUIRE_AVX2)
  if(HAVE_VISUAL_STUDIO_ARCH_AVX2)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /arch:AVX2")
  endif(HAVE_VISUAL_STUDIO_ARCH_AVX2)
  if(HAVE_CLANG_MAVX)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx")
  endif(HAVE_CLANG_MAVX)
  if(HAVE_CLANG_MBMI2)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mbmi2")
  endif(HAVE_CLANG_MBMI2)
elseif(SNAPPY_REQUIRE_AVX)
  if(HAVE_VISUAL_STUDIO_ARCH_AVX)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /arch:AVX")
  endif(HAVE_VISUAL_STUDIO_ARCH_AVX)
  if(HAVE_CLANG_MAVX)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx")
  endif(HAVE_CLANG_MAVX)
endif(SNAPPY_REQUIRE_AVX2)

# Used by googletest.
check_cxx_compiler_flag(
  -Wno-missing-field-initializers
  SNAPPY_HAVE_NO_MISSING_FIELD_INITIALIZERS
)

include(CheckCXXSourceCompiles)
check_cxx_source_compiles("
int main() {
  return __builtin_expect(0, 1);
}" HAVE_BUILTIN_EXPECT)

check_cxx_source_compiles("
int main() {
  return __builtin_ctzll(0);
}" HAVE_BUILTIN_CTZ)

check_cxx_source_compiles("
__attribute__((always_inline)) int zero() { return 0; }

int main() {
  return zero();
}" HAVE_ATTRIBUTE_ALWAYS_INLINE)

check_cxx_source_compiles("
#include <tmmintrin.h>

int main() {
  const __m128i *src = 0;
  __m128i dest;
  const __m128i shuffle_mask = _mm_load_si128(src);
  const __m128i pattern = _mm_shuffle_epi8(_mm_loadl_epi64(src), shuffle_mask);
  _mm_storeu_si128(&dest, pattern);
  return 0;
}" SNAPPY_HAVE_SSSE3)

check_cxx_source_compiles("
#include <immintrin.h>
int main() {
  return _bzhi_u32(0, 1);
}" SNAPPY_HAVE_BMI2)

include(CheckSymbolExists)
check_symbol_exists("mmap" "sys/mman.h" HAVE_FUNC_MMAP)
check_symbol_exists("sysconf" "unistd.h" HAVE_FUNC_SYSCONF)

configure_file(
  "${SNAPPY_SOURCE_DIR}/cmake/config.h.in"
  "${CMAKE_THIRDPARTY_HEADER_DIR}/snappy/config.h"
)

set(HAVE_SYS_UIO_H_01 ${HAVE_SYS_UIO_H})
if(NOT HAVE_SYS_UIO_H_01)
  set(HAVE_SYS_UIO_H_01 0)
endif(NOT HAVE_SYS_UIO_H_01)

if(SNAPPY_FUZZING_BUILD)
  if(NOT "${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
    message(WARNING "Fuzzing builds are only supported with Clang")
  endif(NOT "${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")

  if(NOT CMAKE_CXX_FLAGS MATCHES "-fsanitize=address")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
  endif(NOT CMAKE_CXX_FLAGS MATCHES "-fsanitize=address")

  if(NOT CMAKE_CXX_FLAGS MATCHES "-fsanitize=fuzzer-no-link")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=fuzzer-no-link")
  endif(NOT CMAKE_CXX_FLAGS MATCHES "-fsanitize=fuzzer-no-link")
endif(SNAPPY_FUZZING_BUILD)

configure_file(
  "${SNAPPY_SOURCE_DIR}/snappy-stubs-public.h.in"
  "${CMAKE_THIRDPARTY_HEADER_DIR}/snappy/snappy-stubs-public.h"
)

set(
  snappy_sources
  "${SNAPPY_SOURCE_DIR}/snappy-internal.h"
  "${SNAPPY_SOURCE_DIR}/snappy-stubs-internal.h"
  "${SNAPPY_SOURCE_DIR}/snappy-c.cc"
  "${SNAPPY_SOURCE_DIR}/snappy-sinksource.cc"
  "${SNAPPY_SOURCE_DIR}/snappy-stubs-internal.cc"
  "${SNAPPY_SOURCE_DIR}/snappy-internal.h"
  "${SNAPPY_SOURCE_DIR}/snappy.cc"
)
add_library(snappy STATIC ${snappy_sources})
target_include_directories(
  snappy
  PRIVATE
  ${CMAKE_THIRDPARTY_HEADER_DIR}/snappy/
  ${CMAKE_THIRDPARTY_HEADER_DIR}/snappy/include
)

target_include_directories(
  snappy
  SYSTEM
  PUBLIC
  ${CMAKE_THIRDPARTY_HEADER_DIR}/snappy/include
)

file(COPY ${SNAPPY_SOURCE_DIR}/snappy-c.h DESTINATION ${CMAKE_THIRDPARTY_HEADER_DIR}/snappy/include)
file(COPY ${SNAPPY_SOURCE_DIR}/snappy-sinksource.h DESTINATION ${CMAKE_THIRDPARTY_HEADER_DIR}/snappy/include)
file(COPY ${SNAPPY_SOURCE_DIR}/snappy.h DESTINATION ${CMAKE_THIRDPARTY_HEADER_DIR}/snappy/include)
file(COPY ${CMAKE_THIRDPARTY_HEADER_DIR}/snappy/snappy-stubs-public.h DESTINATION ${CMAKE_THIRDPARTY_HEADER_DIR}/snappy/include)
