find_path(
  LIBRT_INCLUDE_DIRS
  NAMES time.h
  PATHS ${LIBRT_ROOT}/include/
)
find_library(LIBRT_LIBRARIES rt)
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Librt DEFAULT_MSG LIBRT_LIBRARIES LIBRT_INCLUDE_DIRS)
mark_as_advanced(LIBRT_INCLUDE_DIRS LIBRT_LIBRARIES)

if(LIBRT_FOUND)
  if(NOT TARGET LIBRT::LIBRT)
    add_library(LIBRT::LIBRT UNKNOWN IMPORTED)
    set_target_properties(
      LIBRT::LIBRT PROPERTIES
      IMPORTED_LOCATION "${LIBRT_LIBRARIES}"
      INTERFACE_INCLUDE_DIRECTORIES "${LIBRT_INCLUDE_DIRS}"
    )
  endif()
endif()

set(LIBRT_LIBRARIES ${LIBRT_LIBRARY})
