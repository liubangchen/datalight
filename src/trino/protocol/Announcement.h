#pragma once
#include <string>
#include <fmt/core.h>
#include <hash-library/sha256.h>
#include <jwt/jwt.h>
#include <nlohmann/json.hpp>
#include <proxygen/httpserver/RequestHandler.h>

using namespace std;

namespace datalight::protocol
{
    static const std::string uuid = "e4901aae-a5c9-9ff7-97a9-5687835ad54c";
    static const char * env = "production";
    using min = std::chrono::minutes;
    string createAnnouncementBody()
    {
        nlohmann::json body
            = {{"environment", "production"},
               {"pool", "general"},
               {"nodeId", uuid},
               {"location", fmt::format("/{}", uuid)},
               {"services",
                {{{"id", "66b459fd-9262-4236-867d-5b0958570e2d"},
                  {"type", "trino"},
                  {"properties",
                   {{"node_version", "381"},
                    {"coordinator", "false"},
                    {"connectorIds", "hive"},
                    {"http-external", "http://172.19.254.10:9100"},
                    {"http", fmt::format("http://{}:{}", "172.19.254.10", "9100")}}}}}}};
        return body.dump();
    }
    proxygen::HTTPMessage announcementRequest(const std::string & body)
    {
        hashlibrary::SHA256 sha256;
        sha256.add(env, strlen(env));
        unsigned char sbuf[hashlibrary::SHA256::HashBytes];
        sha256.getHash(sbuf);
        string hash_key(sbuf, sbuf + sizeof sbuf / sizeof sbuf[0]);
        const auto time = jwt::date::clock::now();
        auto token = jwt::create()
            .set_subject(uuid)
            .set_expires_at(time + min{5})
            .sign(jwt::algorithm::hs256(hash_key));

        proxygen::HTTPMessage request;
        request.setMethod(proxygen::HTTPMethod::PUT);
        request.setHTTPVersion(1, 1);
        request.setURL(fmt::format("/v1/announcement/{}", uuid));
        request.getHeaders().rawSet("User-Agent", uuid);
        request.getHeaders().set(proxygen::HTTP_HEADER_HOST, fmt::format("{}:{}", "172.19.254.10", "9000"));
        request.getHeaders().set(proxygen::HTTP_HEADER_CONTENT_TYPE, "application/json");
        request.getHeaders().rawSet("X-Trino-User", "hadoop");
        request.getHeaders().rawSet("X-Trino-Internal-Bearer", token);
        request.getHeaders().set(proxygen::HTTP_HEADER_CONTENT_LENGTH, std::to_string(body.size()));
        return request;
    }
}
