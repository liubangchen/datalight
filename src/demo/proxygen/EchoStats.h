#pragma once

#include <stdint.h>

namespace EchoService {

/**
 * Just some dummy class containing request count. Since we keep
 * one instance of this in each class, there is no need of
 * synchronization
 */
class EchoStats {
 public:
  virtual ~EchoStats() {
  }

  // NOTE: We make the following methods `virtual` so that we can
  //       mock them using Gmock for our C++ unit-tests. EchoStats
  //       is an external dependency to handler and we should be
  //       able to mock it.

  virtual void recordRequest() {
    ++reqCount_;
  }

  virtual uint64_t getRequestCount() {
    return reqCount_;
  }

 private:
  uint64_t reqCount_{0};
};

} // namespace EchoService
