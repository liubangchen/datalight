#pragma once

#include <folly/SocketAddress.h>
#include <http/HttpServer.h>
#include <string>

namespace datalight {

class TrinoServer {
 public:
  TrinoServer();
  void run();
  void stop();
  virtual ~TrinoServer();

 protected:
  std::string nodeId_;
  std::unique_ptr<http::HttpServer> httpServer_;
};

}  // namespace datalight
