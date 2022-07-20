#include "TrinoServer.h"
#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/host_name.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/lexical_cast.hpp>
#include <folly/Uri.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <protocol/TrinoProtocol.h>
#include "HeartbeatService.h"
#include "PeriodicTaskManager.h"
#include "QueryContextManager.h"
#include "SignalHandler.h"
#include "TaskManager.h"
#include "TaskResource.h"
#include "config/Configs.h"

DEFINE_int32(num_io_threads, 30, "Number of IO threads");

DEFINE_int32(
    shutdown_onset_sec,
    10,
    "Seconds between moving to 'shutting down' state"
    " and starting shutting down.");

DEFINE_int32(
    system_memory_gb,
    37,
    "System memory available for Presto Server in Gb.");

DEFINE_bool(
    enable_serialized_page_checksum,
    true,
    "Enable use of CRC in exchange");

using namespace facebook::velox;

namespace datalight::server {
namespace {
constexpr char const* kHttp = "http";
constexpr char const* kBaseUriFormat = "http://{}:{}";
constexpr char const* kConnectorName = "connector.name";
constexpr char const* kCacheEnabled = "cache.enabled";
constexpr char const* kCacheMaxCacheSize = "cache.max-cache-size";
} // namespace

protocol::NodeState convertNodeState(NodeState nodeState) {
  switch (nodeState) {
    case NodeState::ACTIVE:
      return protocol::NodeState::ACTIVE;
    case NodeState::INACTIVE:
      return protocol::NodeState::INACTIVE;
    case NodeState::SHUTTING_DOWN:
      return protocol::NodeState::SHUTTING_DOWN;
  }
  return protocol::NodeState::ACTIVE;
}

static protocol::Duration getUptime(
    std::chrono::steady_clock::time_point& start) {
  auto seconds = std::chrono::duration_cast<std::chrono::seconds>(
                     std::chrono::steady_clock::now() - start)
                     .count();
  if (seconds >= 86400) {
    return protocol::Duration(seconds / 86400.0, protocol::TimeUnit::DAYS);
  }
  if (seconds >= 3600) {
    return protocol::Duration(seconds / 3600.0, protocol::TimeUnit::HOURS);
  }
  if (seconds >= 60) {
    return protocol::Duration(seconds / 60.0, protocol::TimeUnit::MINUTES);
  }

  return protocol::Duration(seconds, protocol::TimeUnit::SECONDS);
}

void sendOkResponse(proxygen::ResponseHandler* downstream, const json& body) {
  proxygen::ResponseBuilder(downstream)
      .status(http::kHttpOk, "OK")
      .header(
          proxygen::HTTP_HEADER_CONTENT_TYPE, http::kMimeTypeApplicationJson)
      .body(body.dump())
      .sendWithEOM();
}

std::string getLocalIp() {
  using boost::asio::ip::tcp;
  boost::asio::io_service io_service;
  tcp::resolver resolver(io_service);
  tcp::resolver::query query(boost::asio::ip::host_name(), kHttp);
  tcp::resolver::iterator it = resolver.resolve(query);
  while (it != tcp::resolver::iterator()) {
    boost::asio::ip::address addr = (it++)->endpoint().address();
    // simple check to see if the address is not ::
    if (addr.to_string().length() > 4) {
      return fmt::format("[{}]", addr.to_string());
    }
  }
  VELOX_FAIL(
      "Could not infer Node IP. Please specify node.ip in the node.properties file.");
}

void enableChecksum() {
  // exec::PartitionedOutputBufferManager::getInstance().lock()->setListenerFactory(
  //    []() { return
  //    std::make_unique<serializer::presto::PrestoOutputStreamListener>(); });
}

TrinoServer::TrinoServer(const std::string& configDirectoryPath)
    : configDirectoryPath_(configDirectoryPath),
      signalHandler_(std::make_unique<SignalHandler>(this)),
      start_(std::chrono::steady_clock::now()),
      memoryInfo_(std::make_unique<protocol::MemoryInfo>()){

      };

TrinoServer::~TrinoServer() {}

std::function<folly::SocketAddress()> TrinoServer::discoveryAddressLookup() {
  auto uri = folly::Uri(config::SystemConfig::instance()->discoveryUri());

  return [uri]() {
    return folly::SocketAddress(uri.hostname(), uri.port(), true);
  };
}

std::shared_ptr<exec::TaskListener> TrinoServer::getTaskListener() {
  return nullptr;
}

void TrinoServer::reportMemoryInfo(proxygen::ResponseHandler* downstream) {
  sendOkResponse(downstream, json(**memoryInfo_.rlock()));
}

void TrinoServer::populateMemAndCPUInfo() {
  const int64_t nodeMemoryGb = 4096;
  protocol::MemoryInfo memoryInfo{
      16,
      {
          36077725286,
          0,
          0,
          {},
          {},
          {},
          {},
          {},
      }};
  protocol::MemoryAllocation ma{"e", 23423};
  cpuMon_.update();
  **memoryInfo_.wlock() = std::move(memoryInfo);
}

void TrinoServer::reportServerInfo(proxygen::ResponseHandler* downstream) {
  const protocol::ServerInfo serverInfo{
      {"381"},
      "production",
      false,
      false,
      std::make_shared<protocol::Duration>(getUptime(start_))};
  sendOkResponse(downstream, json(serverInfo));
}

void TrinoServer::reportNodeStatus(proxygen::ResponseHandler* downstream) {
  const int64_t nodeMemoryGb =
      20 * 1024 * 1024 * 1024; // FLAGS_system_memory_gb - cacheRamCapacityGb_;

  const double cpuLoadPct{cpuMon_.getCPULoadPct()};

  // TODO(spershin): As 'nonHeapUsed' we could export the cache memory.
  const int64_t nonHeapUsed{0};

  protocol::NodeStatus nodeStatus{
      nodeId_,
      {"381"},
      "production",
      false,
      getUptime(start_),
      "172.19.254.10:9100",
      "172.19.254.10:9100",
      **memoryInfo_.rlock(),
      (int)std::thread::hardware_concurrency(),
      cpuLoadPct,
      cpuLoadPct,
      0,
      nodeMemoryGb * 1024 * 1024 * 1024,
      nonHeapUsed};

  sendOkResponse(downstream, json(nodeStatus));
}

void TrinoServer::run() {
  auto executor = std::make_shared<folly::IOThreadPoolExecutor>(
      FLAGS_num_io_threads,
      std::make_shared<folly::NamedThreadFactory>("TrinoWorkerNetwork"));
  folly::setUnsafeMutableGlobalIOExecutor(executor);

  auto systemConfig = config::SystemConfig::instance();
  systemConfig->initialize(configDirectoryPath_ + "/config.properties");
  auto nodeConfig = config::NodeConfig::instance();
  nodeConfig->initialize(configDirectoryPath_ + "/node.properties");

  auto servicePort = systemConfig->httpServerHttpPort();
  nodeVersion_ = systemConfig->trinoVersion();
  int httpExecThreads = systemConfig->httpExecThreads();
  environment_ = nodeConfig->nodeEnvironment();
  nodeId_ = nodeConfig->nodeId();
  address_ = nodeConfig->nodeIp(getLocalIp);
  nodeLocation_ = nodeConfig->nodeLocation();
  if (address_.find(':') != std::string::npos && address_.front() != '[') {
    address_ = fmt::format("[{}]", address_);
  }

  folly::SocketAddress socketAddress;
  socketAddress.setFromLocalPort(servicePort);
  LOG(INFO) << fmt::format(
      "STARTUP: Starting server at {}:{} ({})",
      socketAddress.getIPAddress().str(),
      servicePort,
      address_);

  std::vector<std::string> catalogNames;
  catalogNames.push_back("hive");
  HeartbeatService hbService(
      address_,
      servicePort,
      discoveryAddressLookup(),
      nodeVersion_,
      environment_,
      nodeId_,
      nodeLocation_,
      catalogNames,
      30'000);
  hbService.start();

  httpServer_ =
      std::make_unique<http::HttpServer>(socketAddress, httpExecThreads);

  httpServer_->registerGet(
      "/v1/memory",
      [server = this](
          proxygen::HTTPMessage* /*message*/,
          const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
          proxygen::ResponseHandler* downstream) {
        server->reportMemoryInfo(downstream);
      });
  httpServer_->registerGet(
      "/v1/info",
      [server = this](
          proxygen::HTTPMessage* /*message*/,
          const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
          proxygen::ResponseHandler* downstream) {
        server->reportServerInfo(downstream);
      });
  httpServer_->registerGet(
      "/v1/info/state",
      [server = this](
          proxygen::HTTPMessage* /*message*/,
          const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
          proxygen::ResponseHandler* downstream) {
        json infoStateJson = convertNodeState(server->nodeState());
        sendOkResponse(downstream, infoStateJson);
      });
  httpServer_->registerGet(
      "/v1/status",
      [server = this](
          proxygen::HTTPMessage* /*message*/,
          const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
          proxygen::ResponseHandler* downstream) {
        server->reportNodeStatus(downstream);
      });
  httpServer_->registerHead(
      "/v1/status",
      [server = this](
          proxygen::HTTPMessage* /*message*/,
          const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
          proxygen::ResponseHandler* downstream) {
        proxygen::ResponseBuilder(downstream)
            .status(http::kHttpOk, "OK")
            .header(
                proxygen::HTTP_HEADER_CONTENT_TYPE,
                http::kMimeTypeApplicationJson)
            .sendWithEOM();
      });

  taskManager_ = std::make_unique<TaskManager>(
      systemConfig->values(), nodeConfig->values());
  taskManager_->setBaseUri(fmt::format(kBaseUriFormat, address_, servicePort));
  taskResource_ = std::make_unique<TaskResource>(*taskManager_);
  taskResource_->registerUris(*httpServer_);
  if (FLAGS_enable_serialized_page_checksum) {
    enableChecksum();
  }

  if (systemConfig->enableVeloxTaskLogging()) {
    if (auto listener = getTaskListener()) {
      exec::registerTaskListener(listener);
    }
  }

  LOG(INFO) << "STARTUP: Starting all periodic tasks...";
  PeriodicTaskManager periodicTaskManager(
      driverCPUExecutor(), httpServer_->getExecutor());
  periodicTaskManager.addTask(
      [server = this]() { server->populateMemAndCPUInfo(); },
      1'000'000, // 1 second
      "populate_mem_cpu_info");
  periodicTaskManager.start();

  httpServer_->start();

  LOG(INFO) << "SHUTDOWN: Stopping all periodic tasks...";
  periodicTaskManager.stop();
}
void TrinoServer::stop() {
  if (httpServer_) {
    LOG(INFO) << "SHUTDOWN: All tasks are completed. Stopping HTTP Server...";
    httpServer_->stop();
    LOG(INFO) << "SHUTDOWN: HTTP Server stopped.";
  }
}
} // namespace datalight::server
