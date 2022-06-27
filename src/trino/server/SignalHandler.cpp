#include "SignalHandler.h"
#include <csignal>
#include <folly/io/async/EventBaseManager.h>
#include "TrinoServer.h"

namespace datalight::server
{
    SignalHandler::SignalHandler(TrinoServer * trinoServer)
        : folly::AsyncSignalHandler(folly::EventBaseManager::get()->getEventBase()), trinoServer_(trinoServer)
    {
        registerSignalHandler(SIGINT);
        registerSignalHandler(SIGTERM);
    }

    void SignalHandler::signalReceived(int signum) noexcept
    {
        LOG(INFO) << "SHUTDOWN: Received signal " << signum;
        trinoServer_->stop();
    }
}
