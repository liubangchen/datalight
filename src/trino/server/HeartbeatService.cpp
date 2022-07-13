#include "HeartbeatService.h"
#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <string>
#include <fmt/core.h>
#include <hash-library/sha256.h>
#include <jwt/jwt.h>
#include <nlohmann/json.hpp>
#include <proxygen/httpserver/RequestHandler.h>

namespace datalight::server
{
    using min = std::chrono::minutes;

    namespace {

        std::string announcementBody(
            const std::string& address,
            int port,
            const std::string& nodeVersion,
            const std::string& environment,
            const std::string& nodeLocation,
            const std::vector<std::string>& connectorIds) {
            std::string id =
                    boost::lexical_cast<std::string>(boost::uuids::random_generator()());

            std::ostringstream connectors;
            for (int i = 0; i < connectorIds.size(); i++) {
                if (i > 0) {
                    connectors << ",";
                }
                connectors << connectorIds[i];
            }

            nlohmann::json body = {
                {"environment", environment},
                {"pool", "general"},
                {"location", nodeLocation},
                {"services",
                 {{{"id", id},
                   {"type", "trino"},
                   {"properties",
                    {{"node_version", nodeVersion},
                     {"coordinator", false},
                     {"connectorIds", connectors.str()},
                     {"http", fmt::format("http://{}:{}", address, port)}}}}}}};
            return body.dump();
        }

        proxygen::HTTPMessage announcementRequest(
            const std::string& address,
            int port,
            const std::string& environment,
            const std::string& nodeId,
            const std::string& body) {

            hashlibrary::SHA256 sha256;
            sha256.add(environment.c_str(), environment.size());
            unsigned char sbuf[hashlibrary::SHA256::HashBytes];
            sha256.getHash(sbuf);
            std::string hash_key(sbuf, sbuf + sizeof sbuf / sizeof sbuf[0]);
            const auto time = jwt::date::clock::now();
            auto token = jwt::create()
                .set_subject(nodeId)
                .set_expires_at(time + min{5})
                .sign(jwt::algorithm::hs256(hash_key));

            proxygen::HTTPMessage request;
            request.setMethod(proxygen::HTTPMethod::PUT);
            request.setURL(fmt::format("/v1/announcement/{}", nodeId));
            request.getHeaders().set(
                proxygen::HTTP_HEADER_HOST, fmt::format("{}:{}", address, port));
            request.getHeaders().set(
                proxygen::HTTP_HEADER_CONTENT_TYPE, "application/json");
            request.getHeaders().rawSet("X-Trino-Internal-Bearer", token);
            request.getHeaders().set(
                proxygen::HTTP_HEADER_CONTENT_LENGTH, std::to_string(body.size()));
            return request;
        }
    }

    HeartbeatService::HeartbeatService(const std::string& address,
      int port,
      std::function<folly::SocketAddress()> discoveryAddressLookup,
      const std::string& nodeVersion,
      const std::string& environment,
      const std::string& nodeId,
      const std::string& nodeLocation,
      const std::vector<std::string>& connectorIds,
      int frequencyMs) :
        discoveryAddressLookup_(std::move(discoveryAddressLookup)),
      frequencyMs_(frequencyMs),
      announcementBody_(announcementBody(
          address,
          port,
          nodeVersion,
          environment,
          nodeLocation,
          connectorIds)),
      announcementRequest_(
          announcementRequest(address, port,environment ,nodeId, announcementBody_)),
        eventBaseThread_(false /*autostart*/)
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
        try
        {
            auto newAddress = discoveryAddressLookup_();
            if (newAddress != address_) {
                LOG(INFO) << "Discovery service changed to " << newAddress.getAddressStr()
                          << ":" << newAddress.getPort();
                std::swap(address_, newAddress);
                client_ = std::make_unique<http::HttpClient>(
                    eventBaseThread_.getEventBase(),
                    address_,
                    std::chrono::milliseconds(10'000));
           }
        }
        catch (const std::exception & ex)
        {
            LOG(WARNING) << "Error occurred during announcement run: " << ex.what();
            scheduleNext();
            return;
        }
        client_->sendRequest(announcementRequest_, announcementBody_)
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
