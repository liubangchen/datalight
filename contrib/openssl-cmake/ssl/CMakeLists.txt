set(OPENSSL_SSL_SRC_DIR "${OPENSSL_SRC_DIR}/ssl")

if(BUILD_SHARED_LIBS)
  #add_definitions(-DOPENSSL_BUILD_SHLIBSSL)
endif()

set(
  LIBSRC
  "${OPENSSL_SSL_SRC_DIR}/bio_ssl.c"
  "${OPENSSL_SSL_SRC_DIR}/d1_lib.c"
  "${OPENSSL_SSL_SRC_DIR}/d1_msg.c"
  "${OPENSSL_SSL_SRC_DIR}/d1_srtp.c"
  "${OPENSSL_SSL_SRC_DIR}/methods.c"
  "${OPENSSL_SSL_SRC_DIR}/packet.c"
  "${OPENSSL_SSL_SRC_DIR}/pqueue.c"
  "${OPENSSL_SSL_SRC_DIR}/s3_cbc.c"
  "${OPENSSL_SSL_SRC_DIR}/s3_enc.c"
  "${OPENSSL_SSL_SRC_DIR}/s3_lib.c"
  "${OPENSSL_SSL_SRC_DIR}/s3_msg.c"
  "${OPENSSL_SSL_SRC_DIR}/ssl_asn1.c"
  "${OPENSSL_SSL_SRC_DIR}/ssl_cert.c"
  "${OPENSSL_SSL_SRC_DIR}/ssl_ciph.c"
  "${OPENSSL_SSL_SRC_DIR}/ssl_conf.c"
  "${OPENSSL_SSL_SRC_DIR}/ssl_err.c"
  "${OPENSSL_SSL_SRC_DIR}/ssl_init.c"
  "${OPENSSL_SSL_SRC_DIR}/ssl_lib.c"
  "${OPENSSL_SSL_SRC_DIR}/ssl_mcnf.c"
  "${OPENSSL_SSL_SRC_DIR}/ssl_rsa.c"
  "${OPENSSL_SSL_SRC_DIR}/ssl_sess.c"
  "${OPENSSL_SSL_SRC_DIR}/ssl_stat.c"
  "${OPENSSL_SSL_SRC_DIR}/ssl_txt.c"
  "${OPENSSL_SSL_SRC_DIR}/ssl_utst.c"
  "${OPENSSL_SSL_SRC_DIR}/t1_enc.c"
  "${OPENSSL_SSL_SRC_DIR}/t1_lib.c"
  "${OPENSSL_SSL_SRC_DIR}/t1_trce.c"
  "${OPENSSL_SSL_SRC_DIR}/tls13_enc.c"
  "${OPENSSL_SSL_SRC_DIR}/tls_srp.c"
  "${OPENSSL_SSL_SRC_DIR}/record/dtls1_bitmap.c"
  "${OPENSSL_SSL_SRC_DIR}/record/rec_layer_d1.c"
  "${OPENSSL_SSL_SRC_DIR}/record/rec_layer_s3.c"
  "${OPENSSL_SSL_SRC_DIR}/record/ssl3_buffer.c"
  "${OPENSSL_SSL_SRC_DIR}/record/ssl3_record.c"
  "${OPENSSL_SSL_SRC_DIR}/record/ssl3_record_tls13.c"
  "${OPENSSL_SSL_SRC_DIR}/statem/extensions.c"
  "${OPENSSL_SSL_SRC_DIR}/statem/extensions_clnt.c"
  "${OPENSSL_SSL_SRC_DIR}/statem/extensions_cust.c"
  "${OPENSSL_SSL_SRC_DIR}/statem/extensions_srvr.c"
  "${OPENSSL_SSL_SRC_DIR}/statem/statem.c"
  "${OPENSSL_SSL_SRC_DIR}/statem/statem_clnt.c"
  "${OPENSSL_SSL_SRC_DIR}/statem/statem_dtls.c"
  "${OPENSSL_SSL_SRC_DIR}/statem/statem_lib.c"
  "${OPENSSL_SSL_SRC_DIR}/statem/statem_srvr.c"
)

add_library(
  ssl
  STATIC
  ${LIBSRC}
)

target_link_libraries(ssl crypto)

set_target_properties(
  ssl PROPERTIES
  VERSION "${LIB_VERSION}" SOVERSION "${LIB_SOVERSION}"
)

target_include_directories(
  ssl
  PRIVATE
  ${OPENSSL_SRC_DIR}
  ${OPENSSL_SRC_DIR}/include
  ${OPENSSL_SRC_DIR}/crypto
  ${CMAKE_THIRDPARTY_HEADER_DIR}/openssl
)

target_include_directories(
  ssl
  SYSTEM
  PUBLIC
  ${OPENSSL_SRC_DIR}
  ${OPENSSL_SRC_DIR}/include
  ${OPENSSL_SRC_DIR}/crypto
  ${CMAKE_THIRDPARTY_HEADER_DIR}/openssl
)
#list(APPEND OPENSSL_LIBRARIES ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/libssl.a)
set(
  OPENSSL_INCLUDE_DIR
  ${OPENSSL_SRC_DIR}
  ${OPENSSL_SRC_DIR}/include
  ${OPENSSL_SRC_DIR}/crypto
  ${CMAKE_THIRDPARTY_HEADER_DIR}/openssl
)
#set(FMT_LIBRARY ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/libfmt.a)
#set(FMT_FOUND TRUE)
