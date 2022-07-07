#pragma once

#include <string>
#include <folly/SocketAddress.h>
#include <http/HttpServer.h>
#include <folly/io/async/EventBaseThread.h>
#include <folly/Synchronized.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <protocol/TrinoProtocol.h>

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
        void populateMemAndCPUInfo();
    protected:
        std::string nodeId_;
        std::unique_ptr<http::HttpServer> httpServer_;
        std::unique_ptr<SignalHandler> signalHandler_;
        folly::Synchronized<std::unique_ptr<protocol::MemoryInfo>> memoryInfo_;
    };
}
