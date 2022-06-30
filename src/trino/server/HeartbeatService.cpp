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
        auto* eventBase = eventBaseThread_.getEventBase();
        eventBase->runOnDestruction([this] { client_.reset(); });
        eventBase->schedule([this]() { return sendHearbeat(); });
    }
    HeartbeatService::~HeartbeatService(){

    }
    void HeartbeatService::stop()
    {
        stopped_ = true;
        eventBaseThread_.stop();
    }
    void HeartbeatService::sendHearbeat(){
        if (stopped_) {
            return;
        }
        LOG(INFO) <<"send heartbeat......";
        scheduleNext();
    }
    void HeartbeatService::scheduleNext() {
        if (stopped_) {
            return;
        }
        eventBaseThread_.getEventBase()->scheduleAt(
            [this]() { return sendHearbeat(); },
            std::chrono::steady_clock::now() +
            std::chrono::milliseconds(2000));
    }
}
