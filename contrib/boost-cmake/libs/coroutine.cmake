_add_boost_lib(
  NAME coroutine
  SOURCES
  ${coroutine_srcs}
  ${BOOST_SOURCE}/libs/coroutine/src/exceptions.cpp
  DEFINE_PRIVATE
  BOOST_COROUTINES_SOURCE
  LINK
  Boost::context
  Boost::thread
)

#_add_boost_test(
#  NAME coroutine_test
#  LINK
#    Boost::coroutine
#    Boost::unit_test_framework
#  TESTS
#    RUN ${BOOST_SOURCE}/libs/coroutine/test/test_asymmetric_coroutine.cpp
#    RUN ${BOOST_SOURCE}/libs/coroutine/test/test_symmetric_coroutine.cpp
#)
