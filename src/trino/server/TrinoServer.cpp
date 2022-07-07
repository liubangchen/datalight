#include "TrinoServer.h"
#include "SignalHandler.h"
#include "HeartbeatService.h"
#include "PeriodicTaskManager.h"
#include <protocol/TrinoProtocol.h>
#include <folly/executors/CPUThreadPoolExecutor.h>

namespace datalight::server
{
    static std::shared_ptr<folly::CPUThreadPoolExecutor>& executor() {
        static std::shared_ptr<folly::CPUThreadPoolExecutor> executor =
            std::make_shared<folly::CPUThreadPoolExecutor>(12);
        return executor;
    }

    folly::CPUThreadPoolExecutor* driverCPUExecutor(){
        return executor().get();
    }

    void sendOkResponse(proxygen::ResponseHandler* downstream) {
        std::string s("hello");
        auto buf = folly::IOBuf::copyBuffer(s.data(), s.size(), 1, 2);
        proxygen::ResponseBuilder(downstream)
            .status(http::kHttpOk, "OK")
            .header(
                proxygen::HTTP_HEADER_CONTENT_TYPE, http::kMimeTypeApplicationJson)
            .body(std::move(buf))
            .sendWithEOM();
    }
    TrinoServer::TrinoServer()
        : signalHandler_(std::make_unique<SignalHandler>(this)){

    };
    TrinoServer::~TrinoServer()
    {
    }
    void TrinoServer::populateMemAndCPUInfo() {
        const int64_t nodeMemoryGb=4096;
        protocol::MemoryInfo memoryInfo{
            16,
            {
                4096
            }
        };
        protocol::MemoryAllocation ma{"e",23423};
        LOG(INFO) <<"populateMemAndCPUInfo..."<<json(memoryInfo).dump();
        //**memoryInfo_.wlock() = std::move(memoryInfo);
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
            "/v1/test",
            [server = this](
                proxygen::HTTPMessage * /*message*/,
                const std::vector<std::unique_ptr<folly::IOBuf>> & /*body*/,
                proxygen::ResponseHandler * downstream) {
                sendOkResponse(downstream);
            });

        httpServer_->registerGet(
            "/v1/memory",
            [server = this](
                proxygen::HTTPMessage* /*message*/,
                const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
                proxygen::ResponseHandler* downstream) {
                //server->reportMemoryInfo(downstream);
                sendOkResponse(downstream);
            });

        httpServer_->registerGet(
            "/v1/info/state",
            [server = this](
                proxygen::HTTPMessage* /*message*/,
                const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
                proxygen::ResponseHandler* downstream) {
                //json infoStateJson = convertNodeState(server->nodeState());
                //sendOkResponse(downstream, infoStateJson);
                sendOkResponse(downstream);
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
