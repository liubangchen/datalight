#include "TrinoServer.h"
#include "SignalHandler.h"
#include "HeartbeatService.h"
#include "PeriodicTaskManager.h"
#include <protocol/TrinoProtocol.h>
#include <folly/executors/CPUThreadPoolExecutor.h>

namespace datalight::server
{
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

    static std::shared_ptr<folly::CPUThreadPoolExecutor>& executor() {
        static std::shared_ptr<folly::CPUThreadPoolExecutor> executor =
            std::make_shared<folly::CPUThreadPoolExecutor>(12);
        return executor;
    }

    folly::CPUThreadPoolExecutor* driverCPUExecutor(){
        return executor().get();
    }

    void sendOkResponse(proxygen::ResponseHandler* downstream,const json& body) {
        proxygen::ResponseBuilder(downstream)
            .status(http::kHttpOk, "OK")
            .header(
                proxygen::HTTP_HEADER_CONTENT_TYPE, http::kMimeTypeApplicationJson)
            .body(body.dump())
            .sendWithEOM();
    }

    TrinoServer::TrinoServer()
        : signalHandler_(std::make_unique<SignalHandler>(this)),
          memoryInfo_(std::make_unique<protocol::MemoryInfo>()){

    };

    TrinoServer::~TrinoServer()
    {
    }

    void TrinoServer::reportMemoryInfo(proxygen::ResponseHandler* downstream) {
        sendOkResponse(downstream, json(**memoryInfo_.rlock()));
    }

    void TrinoServer::populateMemAndCPUInfo() {
        const int64_t nodeMemoryGb=4096;
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
            }
        };
        protocol::MemoryAllocation ma{"e",23423};
        //LOG(INFO) <<"populateMemAndCPUInfo..."<<json(memoryInfo).dump();
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
        const int64_t nodeMemoryGb = 20*1024 * 1024 * 1024;//FLAGS_system_memory_gb - cacheRamCapacityGb_;

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

    void TrinoServer::run()
    {
        HeartbeatService hbService;
        hbService.start();

        int httpExecThreads = 4;
        auto servicePort = 9100;
        folly::SocketAddress socketAddress;
        socketAddress.setFromLocalPort(servicePort);
        LOG(INFO) << fmt::format("STARTUP: Starting server at {}:{} ({})", socketAddress.getIPAddress().str(), servicePort, "0.0.0.0");

        httpServer_ = std::make_unique<http::HttpServer>(socketAddress, httpExecThreads);

        httpServer_->registerGet(
            "/v1/memory",
            [server = this](
                proxygen::HTTPMessage* /*message*/,
                const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
                proxygen::ResponseHandler* downstream) {
                LOG(INFO)<<"/v1/memory"<< " ok";
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
                LOG(INFO)<<"/v1/info/state"<< " ok";
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
    void TrinoServer::stop()
    {
        if (httpServer_) {
            LOG(INFO) << "SHUTDOWN: All tasks are completed. Stopping HTTP Server...";
            httpServer_->stop();
            LOG(INFO) << "SHUTDOWN: HTTP Server stopped.";
        }
    }
}
