option(USE_INTERNAL_FMT_LIBRARY "Set to FALSE to use system fmt library instead of bundled)" ON)

if(NOT USE_INTERNAL_FMT_LIBRARY)
  find_library(FMT fmt)
endif()

if(FMT_LIBRARY AND FMT_INCLUDE_DIR)
  set(USE_FMT 1)
elseif()
  set(FMT_INCLUDE_DIR "${CMAKE_THIRDPARTY_HEADER_DIR}/fmt")
  add_library(fmt::fmt ALIAS fmt_static)
  add_library(fmt ALIAS fmt_static)
  set(FMT_LIBRARY ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/libfmt.a)
  set(USE_BROTLI 1)
endif()

message(STATUS "Using fmt ${FMT_INCLUDE_DIR} : ${FMT_LIBRARY}")
