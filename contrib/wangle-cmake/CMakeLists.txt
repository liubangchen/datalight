fetchcontent_declare(
  wangle
  GIT_REPOSITORY https://github.com/facebook/wangle.git
  GIT_TAG v2022.05.30.00
  GIT_PROGRESS TRUE
  SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/wangle
)

fetchcontent_getproperties(wangle)

if(NOT wangle_POPULATED)
  message(STATUS "Fetching wangle")
  fetchcontent_populate(wangle)
  message(STATUS "Fetching wangle - done")
  set(WANGLE_SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/wangle)
endif()

if(UNIX AND NOT APPLE)
  find_package(Librt)
endif()

include(CheckAtomic.cmake)
find_package(Threads REQUIRED)

set(
  WANGLE_HEADER_DIRS
  ${WANGLE_SOURCE_DIR}/acceptor
  ${WANGLE_SOURCE_DIR}/bootstrap
  ${WANGLE_SOURCE_DIR}/channel
  ${WANGLE_SOURCE_DIR}/client
  ${WANGLE_SOURCE_DIR}/codec
  ${WANGLE_SOURCE_DIR}/service
  ${WANGLE_SOURCE_DIR}/ssl
  ${WANGLE_SOURCE_DIR}/util
)

#foreach(dir ${WANGLE_HEADER_DIRS})
#  file(GLOB_RECURSE headers ${dir}/*.h)
#  set(
#    WANGLE_HEADERS
#    ${WANGLE_HEADERS}
#    ${headers}
#  )
#endforeach()

set(
  WANGLE_SOURCES
  ${WANGLE_SOURCE_DIR}/wangle/acceptor/Acceptor.cpp
  ${WANGLE_SOURCE_DIR}/wangle/acceptor/AcceptorHandshakeManager.cpp
  ${WANGLE_SOURCE_DIR}/wangle/acceptor/ConnectionManager.cpp
  ${WANGLE_SOURCE_DIR}/wangle/acceptor/EvbHandshakeHelper.cpp
  ${WANGLE_SOURCE_DIR}/wangle/acceptor/FizzAcceptorHandshakeHelper.cpp
  ${WANGLE_SOURCE_DIR}/wangle/acceptor/FizzConfigUtil.cpp
  ${WANGLE_SOURCE_DIR}/wangle/acceptor/LoadShedConfiguration.cpp
  ${WANGLE_SOURCE_DIR}/wangle/acceptor/ManagedConnection.cpp
  ${WANGLE_SOURCE_DIR}/wangle/acceptor/SecureTransportType.cpp
  ${WANGLE_SOURCE_DIR}/wangle/acceptor/SocketOptions.cpp
  ${WANGLE_SOURCE_DIR}/wangle/acceptor/SSLAcceptorHandshakeHelper.cpp
  ${WANGLE_SOURCE_DIR}/wangle/acceptor/TLSPlaintextPeekingCallback.cpp
  ${WANGLE_SOURCE_DIR}/wangle/acceptor/TransportInfo.cpp
  ${WANGLE_SOURCE_DIR}/wangle/bootstrap/ServerBootstrap.cpp
  ${WANGLE_SOURCE_DIR}/wangle/channel/FileRegion.cpp
  ${WANGLE_SOURCE_DIR}/wangle/channel/Pipeline.cpp
  ${WANGLE_SOURCE_DIR}/wangle/client/persistence/FilePersistenceLayer.cpp
  ${WANGLE_SOURCE_DIR}/wangle/client/persistence/PersistentCacheCommon.cpp
  ${WANGLE_SOURCE_DIR}/wangle/client/ssl/SSLSessionCacheData.cpp
  ${WANGLE_SOURCE_DIR}/wangle/client/ssl/SSLSessionCacheUtils.cpp
  ${WANGLE_SOURCE_DIR}/wangle/client/ssl/SSLSessionCallbacks.cpp
  ${WANGLE_SOURCE_DIR}/wangle/codec/LengthFieldBasedFrameDecoder.cpp
  ${WANGLE_SOURCE_DIR}/wangle/codec/LengthFieldPrepender.cpp
  ${WANGLE_SOURCE_DIR}/wangle/codec/LineBasedFrameDecoder.cpp
  ${WANGLE_SOURCE_DIR}/wangle/ssl/PasswordInFileFactory.cpp
  ${WANGLE_SOURCE_DIR}/wangle/ssl/ServerSSLContext.cpp
  ${WANGLE_SOURCE_DIR}/wangle/ssl/SSLContextManager.cpp
  ${WANGLE_SOURCE_DIR}/wangle/ssl/SSLSessionCacheManager.cpp
  ${WANGLE_SOURCE_DIR}/wangle/ssl/SSLUtil.cpp
  ${WANGLE_SOURCE_DIR}/wangle/ssl/TLSTicketKeyManager.cpp
  ${WANGLE_SOURCE_DIR}/wangle/ssl/TLSCredProcessor.cpp
  ${WANGLE_SOURCE_DIR}/wangle/util/FilePoller.cpp
)

add_library(
  wangle
  STATIC
  ${WANGLE_SOURCES}
)

add_library(wangle::wangle ALIAS wangle)

target_link_libraries(
  wangle
  ssl
  folly
  fizz
  Threads::Threads
)

target_include_directories(
  wangle
  PRIVATE
  ${WANGLE_SOURCE_DIR}
)

target_include_directories(
  wangle
  PUBLIC
  $<BUILD_INTERFACE:${WANGLE_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${INCLUDE_INSTALL_DIR}>
)
