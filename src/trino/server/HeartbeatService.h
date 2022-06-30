#pragma once

#include <folly/io/async/EventBaseThread.h>
#include <http/HttpClient.h>

namespace datalight::server
{
    class HeartbeatService
    {
    public:
        HeartbeatService();
        ~HeartbeatService();
        void start();
        void stop();
    private:
        void sendHearbeat();
        void scheduleNext();
        std::unique_ptr<http::HttpClient> client_;
        std::atomic_bool stopped_{true};
        folly::EventBaseThread eventBaseThread_;
    };
}
