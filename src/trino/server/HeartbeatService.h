#pragma once

#include <folly/io/async/EventBaseThread.h>
#include <http/HttpClient.h>

namespace datalight::server
{
    class HeartbeatService
    {
    public:
        HeartbeatService(const std::string& address,
                         int port,
                         std::function<folly::SocketAddress()> discoveryAddressLookup,
                         const std::string& nodeVersion,
                         const std::string& environment,
                         const std::string& nodeId,
                         const std::string& nodeLocation,
                         const std::vector<std::string>& connectorIds,
                         int frequencyMs);
        ~HeartbeatService();
        void start();
        void stop();
    private:
        void sendHearbeat();
        void scheduleNext();
        std::function<folly::SocketAddress()> discoveryAddressLookup_;
        const int frequencyMs_;
        const std::string announcementBody_;
        const proxygen::HTTPMessage announcementRequest_;
        folly::SocketAddress address_;
        std::unique_ptr<http::HttpClient> client_;
        std::atomic_bool stopped_{true};
        folly::EventBaseThread eventBaseThread_;
    };
}
