#pragma once

#include <string>
#include <folly/SocketAddress.h>
#include <http/HttpServer.h>
#include <folly/io/async/EventBaseThread.h>
#include <folly/Synchronized.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <protocol/TrinoProtocol.h>
#include "CPUMon.h"

namespace datalight::server
{
    class SignalHandler;
    enum class NodeState { ACTIVE, INACTIVE, SHUTTING_DOWN };
    class TrinoServer
    {
    public:
    explicit TrinoServer();
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
        void reportMemoryInfo(proxygen::ResponseHandler* downstream);

        void reportServerInfo(proxygen::ResponseHandler* downstream);

        void reportNodeStatus(proxygen::ResponseHandler* downstream);

        void populateMemAndCPUInfo();
    protected:
        std::string nodeId_;
        CPUMon cpuMon_;
        std::atomic<NodeState> nodeState_{NodeState::ACTIVE};
        std::unique_ptr<http::HttpServer> httpServer_;
        std::unique_ptr<SignalHandler> signalHandler_;
        folly::Synchronized<std::unique_ptr<protocol::MemoryInfo>> memoryInfo_;
        std::chrono::steady_clock::time_point start_;
    };
}
