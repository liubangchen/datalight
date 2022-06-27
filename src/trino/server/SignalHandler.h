#pragma once

#include <folly/io/async/AsyncSignalHandler.h>

namespace datalight::server
{
    class TrinoServer;
    class SignalHandler : private folly::AsyncSignalHandler
    {
    public:
        explicit SignalHandler(TrinoServer * trinoServer);

    private:
        void signalReceived(int signum) noexcept override;

    private:
        TrinoServer * trinoServer_{nullptr};
    };
}
