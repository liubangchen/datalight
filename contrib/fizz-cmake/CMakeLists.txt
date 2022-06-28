fetchcontent_declare(
  fizz
  GIT_REPOSITORY https://github.com/facebookincubator/fizz.git
  GIT_TAG ${FB_LIB_VERSION}
  GIT_PROGRESS TRUE
  SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/fizz
)

fetchcontent_getproperties(fizz)

if(NOT fizz_POPULATED)
  message(STATUS "Fetching fizz")
  fetchcontent_populate(fizz)
  message(STATUS "Fetching fizz - done")
  set(FIZZ_SOURCE_DIR ${PROJECT_SOURCE_DIR}/contrib/fizz)
endif()

find_package(Threads REQUIRED)

set(
  FIZZ_SOURCES
  ${FIZZ_SOURCE_DIR}/fizz/crypto/Utils.cpp
  ${FIZZ_SOURCE_DIR}/fizz/crypto/exchange/X25519.cpp
  ${FIZZ_SOURCE_DIR}/fizz/crypto/aead/OpenSSLEVPCipher.cpp
  ${FIZZ_SOURCE_DIR}/fizz/crypto/aead/IOBufUtil.cpp
  ${FIZZ_SOURCE_DIR}/fizz/crypto/signature/Signature.cpp
  ${FIZZ_SOURCE_DIR}/fizz/crypto/Hkdf.cpp
  ${FIZZ_SOURCE_DIR}/fizz/crypto/KeyDerivation.cpp
  ${FIZZ_SOURCE_DIR}/fizz/crypto/Sha256.cpp
  ${FIZZ_SOURCE_DIR}/fizz/crypto/Sha384.cpp
  ${FIZZ_SOURCE_DIR}/fizz/crypto/hpke/Context.cpp
  ${FIZZ_SOURCE_DIR}/fizz/crypto/hpke/DHKEM.cpp
  ${FIZZ_SOURCE_DIR}/fizz/crypto/hpke/Hkdf.cpp
  ${FIZZ_SOURCE_DIR}/fizz/crypto/hpke/Hpke.cpp
  ${FIZZ_SOURCE_DIR}/fizz/crypto/hpke/Utils.cpp
  ${FIZZ_SOURCE_DIR}/fizz/crypto/openssl/OpenSSLKeyUtils.cpp
  ${FIZZ_SOURCE_DIR}/fizz/record/Types.cpp
  ${FIZZ_SOURCE_DIR}/fizz/record/RecordLayer.cpp
  ${FIZZ_SOURCE_DIR}/fizz/record/EncryptedRecordLayer.cpp
  ${FIZZ_SOURCE_DIR}/fizz/record/PlaintextRecordLayer.cpp
  ${FIZZ_SOURCE_DIR}/fizz/record/BufAndPaddingPolicy.cpp
  ${FIZZ_SOURCE_DIR}/fizz/server/AeadTokenCipher.cpp
  ${FIZZ_SOURCE_DIR}/fizz/server/AeadCookieCipher.cpp
  ${FIZZ_SOURCE_DIR}/fizz/server/ServerProtocol.cpp
  ${FIZZ_SOURCE_DIR}/fizz/server/CertManager.cpp
  ${FIZZ_SOURCE_DIR}/fizz/server/State.cpp
  ${FIZZ_SOURCE_DIR}/fizz/server/FizzServer.cpp
  ${FIZZ_SOURCE_DIR}/fizz/server/TicketCodec.cpp
  ${FIZZ_SOURCE_DIR}/fizz/server/CookieCipher.cpp
  ${FIZZ_SOURCE_DIR}/fizz/server/ReplayCache.cpp
  ${FIZZ_SOURCE_DIR}/fizz/server/SlidingBloomReplayCache.cpp
  ${FIZZ_SOURCE_DIR}/fizz/protocol/AsyncFizzBase.cpp
  ${FIZZ_SOURCE_DIR}/fizz/protocol/Types.cpp
  ${FIZZ_SOURCE_DIR}/fizz/protocol/Exporter.cpp
  ${FIZZ_SOURCE_DIR}/fizz/protocol/DefaultCertificateVerifier.cpp
  ${FIZZ_SOURCE_DIR}/fizz/protocol/Events.cpp
  ${FIZZ_SOURCE_DIR}/fizz/protocol/KeyScheduler.cpp
  ${FIZZ_SOURCE_DIR}/fizz/protocol/Certificate.cpp
  ${FIZZ_SOURCE_DIR}/fizz/protocol/CertDecompressionManager.cpp
  ${FIZZ_SOURCE_DIR}/fizz/protocol/Params.cpp
  ${FIZZ_SOURCE_DIR}/fizz/protocol/ZlibCertificateCompressor.cpp
  ${FIZZ_SOURCE_DIR}/fizz/protocol/ZlibCertificateDecompressor.cpp
  ${FIZZ_SOURCE_DIR}/fizz/protocol/ZstdCertificateCompressor.cpp
  ${FIZZ_SOURCE_DIR}/fizz/protocol/ZstdCertificateDecompressor.cpp
  ${FIZZ_SOURCE_DIR}/fizz/protocol/clock/SystemClock.cpp
  ${FIZZ_SOURCE_DIR}/fizz/protocol/ech/Decrypter.cpp
  ${FIZZ_SOURCE_DIR}/fizz/protocol/ech/Encryption.cpp
  ${FIZZ_SOURCE_DIR}/fizz/extensions/delegatedcred/DelegatedCredentialCertManager.cpp
  ${FIZZ_SOURCE_DIR}/fizz/extensions/delegatedcred/DelegatedCredentialClientExtension.cpp
  ${FIZZ_SOURCE_DIR}/fizz/extensions/delegatedcred/DelegatedCredentialFactory.cpp
  ${FIZZ_SOURCE_DIR}/fizz/extensions/delegatedcred/DelegatedCredentialUtils.cpp
  ${FIZZ_SOURCE_DIR}/fizz/extensions/delegatedcred/Types.cpp
  ${FIZZ_SOURCE_DIR}/fizz/extensions/exportedauth/ExportedAuthenticator.cpp
  ${FIZZ_SOURCE_DIR}/fizz/extensions/tokenbinding/Types.cpp
  ${FIZZ_SOURCE_DIR}/fizz/extensions/tokenbinding/TokenBindingConstructor.cpp
  ${FIZZ_SOURCE_DIR}/fizz/extensions/tokenbinding/TokenBindingClientExtension.cpp
  ${FIZZ_SOURCE_DIR}/fizz/extensions/tokenbinding/Validator.cpp
  ${FIZZ_SOURCE_DIR}/fizz/experimental/client/BatchSignaturePeerCert.cpp
  ${FIZZ_SOURCE_DIR}/fizz/experimental/protocol/BatchSignatureTypes.cpp
  ${FIZZ_SOURCE_DIR}/fizz/experimental/ktls/AsyncFizzBaseKTLS.cpp
  ${FIZZ_SOURCE_DIR}/fizz/experimental/ktls/FizzKTLSCallback.cpp
  ${FIZZ_SOURCE_DIR}/fizz/experimental/ktls/AsyncKTLSSocket.cpp
  ${FIZZ_SOURCE_DIR}/fizz/experimental/ktls/KTLS.cpp
  ${FIZZ_SOURCE_DIR}/fizz/client/State.cpp
  ${FIZZ_SOURCE_DIR}/fizz/client/ClientProtocol.cpp
  ${FIZZ_SOURCE_DIR}/fizz/client/PskSerializationUtils.cpp
  ${FIZZ_SOURCE_DIR}/fizz/client/SynchronizedLruPskCache.cpp
  ${FIZZ_SOURCE_DIR}/fizz/client/EarlyDataRejectionPolicy.cpp
  ${FIZZ_SOURCE_DIR}/fizz/tool/FizzCommandCommon.cpp
  ${FIZZ_SOURCE_DIR}/fizz/util/FizzUtil.cpp
)

add_library(
  fizz
  STATIC
  ${FIZZ_SOURCES}
)
if(CMAKE_SYSTEM_NAME MATCHES "Linux")
  target_compile_definitions(
    fizz
    PUBLIC
    -DFIZZ_PLATFORM_CAPABLE_KTLS
  )
endif()

add_library(fizz::fizz ALIAS fizz)

target_link_libraries(
  fizz
  ssl
  folly
  Threads::Threads
  zlib
  zstd
  sodium
)

target_include_directories(
  fizz
  PRIVATE
  ${FIZZ_SOURCE_DIR}
)

target_include_directories(
  fizz
  SYSTEM
  PUBLIC
  ${FIZZ_SOURCE_DIR}
)
