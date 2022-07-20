#pragma once

#include <folly/SocketAddress.h>
#include <folly/Synchronized.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/io/async/EventBaseThread.h>
#include <http/HttpServer.h>
#include <protocol/TrinoProtocol.h>
#include <velox/exec/Task.h>
#include <string>
#include "CPUMon.h"

namespace datalight::server {
class SignalHandler;
class TaskManager;
class TaskResource;
enum class NodeState { ACTIVE, INACTIVE, SHUTTING_DOWN };
class TrinoServer {
 public:
  explicit TrinoServer(const std::string& configDirectoryPath);
  virtual ~TrinoServer();
  void run();
  void stop();
  NodeState nodeState() const {
    return nodeState_;
  }
  void setNodeState(NodeState nodeState) {
    nodeState_ = nodeState;
  }

 protected:
  virtual std::function<folly::SocketAddress()> discoveryAddressLookup();

  virtual std::shared_ptr<facebook::velox::exec::TaskListener>
  getTaskListener();

  void reportMemoryInfo(proxygen::ResponseHandler* downstream);

  void reportServerInfo(proxygen::ResponseHandler* downstream);

  void reportNodeStatus(proxygen::ResponseHandler* downstream);

  void populateMemAndCPUInfo();

 protected:
  const std::string configDirectoryPath_;
  CPUMon cpuMon_;
  std::atomic<NodeState> nodeState_{NodeState::ACTIVE};
  std::unique_ptr<http::HttpServer> httpServer_;
  std::unique_ptr<TaskManager> taskManager_;
  std::unique_ptr<TaskResource> taskResource_;
  std::unique_ptr<SignalHandler> signalHandler_;
  folly::Synchronized<std::unique_ptr<protocol::MemoryInfo>> memoryInfo_;
  std::chrono::steady_clock::time_point start_;

  std::string environment_;
  std::string nodeVersion_;
  std::string nodeId_;
  std::string address_;
  std::string nodeLocation_;
};
} // namespace datalight::server
