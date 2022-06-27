#pragma once

#include <string>
#include <folly/SocketAddress.h>
#include <http/HttpServer.h>

namespace datalight::server
{
    class SignalHandler;
    class TrinoServer
    {
    public:
    explicit TrinoServer();
        void run();
        void stop();
        virtual ~TrinoServer();

    protected:
        std::string nodeId_;
        std::unique_ptr<http::HttpServer> httpServer_;
        std::unique_ptr<SignalHandler> signalHandler_;
};
}
