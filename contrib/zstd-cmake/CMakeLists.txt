fetchcontent_declare(
  zstd
  GIT_REPOSITORY https://github.com/facebook/zstd.git
  GIT_TAG v1.5.2
  SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/zstd
)

fetchcontent_getproperties(zstd)

if(NOT zstd_POPULATED)
  message(STATUS "Fetching zstd")
  fetchcontent_populate(zstd)
  message(STATUS "Fetching zstd - done")
  set(ZSTD_SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/zstd)
endif()

option(ZSTD_LEGACY_SUPPORT "LEGACY SUPPORT" OFF)

file(GLOB CommonSources ${ZSTD_SOURCE_DIR}/common/*.c)
file(GLOB CompressSources ${ZSTD_SOURCE_DIR}/compress/*.c)

if(MSVC)
  file(GLOB DecompressSources ${ZSTD_SOURCE_DIR}/decompress/*.c)
  add_compile_options(-DZSTD_DISABLE_ASM)
else()
  file(GLOB DecompressSources ${ZSTD_SOURCE_DIR}/decompress/*.c ${ZSTD_SOURCE_DIR}/decompress/*.S)
endif()
file(GLOB DictBuilderSources ${ZSTD_SOURCE_DIR}/dictBuilder/*.c)


set(
  ZSTD_Sources
  ${CommonSources}
  ${CompressSources}
  ${DecompressSources}
  ${DictBuilderSources}
)

file(GLOB CommonHeaders ${ZSTD_SOURCE_DIR}/common/*.h)
file(GLOB CompressHeaders ${ZSTD_SOURCE_DIR}/compress/*.h)
file(GLOB DecompressHeaders ${ZSTD_SOURCE_DIR}/decompress/*.h)
file(GLOB DictBuilderHeaders ${ZSTD_SOURCE_DIR}/dictBuilder/*.h)


set(
  ZSTD_Headers
  ${ZSTD_SOURCE_DIR}/zstd.h
  ${CommonHeaders}
  ${CompressHeaders}
  ${DecompressHeaders}
  ${DictBuilderHeaders}
)

if(ZSTD_LEGACY_SUPPORT)
  set(LIBRARY_LEGACY_DIR ${ZSTD_SOURCE_DIR}/legacy)
  include_directories(${LIBRARY_LEGACY_DIR})

  set(
    ZSTD_Sources ${ZSTD_Sources}
    ${LIBRARY_LEGACY_DIR}/zstd_v01.c
    ${LIBRARY_LEGACY_DIR}/zstd_v02.c
    ${LIBRARY_LEGACY_DIR}/zstd_v03.c
    ${LIBRARY_LEGACY_DIR}/zstd_v04.c
    ${LIBRARY_LEGACY_DIR}/zstd_v05.c
    ${LIBRARY_LEGACY_DIR}/zstd_v06.c
    ${LIBRARY_LEGACY_DIR}/zstd_v07.c
  )

  set(
    ZSTD_Headers ${ZSTD_Headers}
    ${LIBRARY_LEGACY_DIR}/zstd_legacy.h
    ${LIBRARY_LEGACY_DIR}/zstd_v01.h
    ${LIBRARY_LEGACY_DIR}/zstd_v02.h
    ${LIBRARY_LEGACY_DIR}/zstd_v03.h
    ${LIBRARY_LEGACY_DIR}/zstd_v04.h
    ${LIBRARY_LEGACY_DIR}/zstd_v05.h
    ${LIBRARY_LEGACY_DIR}/zstd_v06.h
    ${LIBRARY_LEGACY_DIR}/zstd_v07.h
  )
endif()

if(MSVC)
  set(MSVC_RESOURCE_DIR ${ZSTD_SOURCE_DIR}/build/VS2010/libzstd-dll)
  set(PlatformDependResources ${MSVC_RESOURCE_DIR}/libzstd-dll.rc)
endif()

add_library(libzstd_static STATIC ${ZSTD_Sources} ${ZSTD_Headers})
target_include_directories(
  libzstd_static
  SYSTEM BEFORE INTERFACE ${ZSTD_SOURCE_DIR} ${ZSTD_SOURCE_DIR}/common
)

add_library(zstd ALIAS libzstd_static)
add_library(Zstd ALIAS libzstd_static)
#set(ZSTD_LIBRARY zstd)
#set(ZSTD_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/contrib/zstd/lib)
