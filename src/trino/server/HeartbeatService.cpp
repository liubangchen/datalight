#include "HeartbeatService.h"
#include <protocol/Announcement.h>

namespace datalight::server
{
    HeartbeatService::HeartbeatService() : eventBaseThread_(false /*autostart*/)
    {
    }
    void HeartbeatService::start()
    {
        eventBaseThread_.start("HeartbeatService");
        stopped_ = false;
        auto * eventBase = eventBaseThread_.getEventBase();
        eventBase->runOnDestruction([this] { client_.reset(); });
        eventBase->schedule([this]() { return sendHearbeat(); });
    }
    HeartbeatService::~HeartbeatService()
    {
    }
    void HeartbeatService::stop()
    {
        stopped_ = true;
        eventBaseThread_.stop();
    }
    void HeartbeatService::sendHearbeat()
    {
        if (stopped_)
        {
            return;
        }
        LOG(INFO) << "send heartbeat......";
        try
        {
            client_ = std::make_unique<http::HttpClient>(
                eventBaseThread_.getEventBase(), folly::SocketAddress("172.19.254.10", 9000, false), std::chrono::milliseconds(10'000));
        }
        catch (const std::exception & ex)
        {
            LOG(WARNING) << "Error occurred during announcement run: " << ex.what();
            scheduleNext();
            return;
        }
        auto body = protocol::createAnnouncementBody();
        auto request=protocol::announcementRequest(body);
        client_->sendRequest(request, body)
            .via(eventBaseThread_.getEventBase())
            .thenValue(
                [](auto response)
                    {
                        auto message = response->headers.get();
                        if (message->getStatusCode() != http::kHttpAccepted)
                        {
                            LOG(WARNING) << "Announcement failed: HTTP " << message->getStatusCode() << " - " << response->dumpBodyChain();
                        }
                        else
                        {
                            LOG(INFO) << "Announcement succeeded: " << message->getStatusCode();
                        }
                    })
            .thenError(folly::tag_t<std::exception>{}, [](const std::exception & e) { LOG(WARNING) << "Announcement failed: " << e.what(); })
            .thenTry([this](auto /*unused*/) { scheduleNext(); });
    }
    void HeartbeatService::scheduleNext()
    {
        if (stopped_)
        {
            return;
        }
        eventBaseThread_.getEventBase()->scheduleAt(
            [this]() { return sendHearbeat(); }, std::chrono::steady_clock::now() + std::chrono::milliseconds(2000));
    }
}
