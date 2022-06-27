#include "TrinoServer.h"
#include "SignalHandler.h"

namespace datalight::server
{
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
    void TrinoServer::run()
    {
        int httpExecThreads = 4;
        auto servicePort = 9000;
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

        httpServer_->start();
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
