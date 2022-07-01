#pragma once
#include <fmt/core.h>
#include <nlohmann/json.hpp>
#include <proxygen/httpserver/RequestHandler.h>
#include <jwt/jwt.h>

namespace datalight::protocol
{
    static const std::string uuid = "e4901aae-a5c9-9ff7-97a9-5687835ad54c";
    using min = std::chrono::minutes;
    std::string createAnnouncementBody()
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
        const auto time = jwt::date::clock::now();
        auto token = jwt::create()
            .set_type("JWS")
            .set_payload_claim("sub", jwt::claim(uuid))
            .set_expires_at(time + min{5})
            //.set_payload_claim("exp",picojson::value(time+min{5}))
            .sign(jwt::algorithm::hs256{"secret"});
        ////std::cout <<token << " = ================"  << std::endl;
        auto decoded1 = jwt::decode(token);
        for(auto& e : decoded1.get_payload_claims())
                std::cout << e.first << " * " << e.second << std::endl;

        proxygen::HTTPMessage request;
        request.setMethod(proxygen::HTTPMethod::PUT);
        request.setURL(fmt::format("/v1/announcement/{}", uuid));
        request.getHeaders().set("User-Agent", uuid);
        request.getHeaders().set(proxygen::HTTP_HEADER_HOST, fmt::format("{}:{}", "172.19.254.10", "9000"));
        request.getHeaders().set(proxygen::HTTP_HEADER_CONTENT_TYPE, "application/json");
        request.getHeaders().set("X-Trino-User", "hadoop");
        request.getHeaders().set("X-Trino-Internal-Bearer", token);
        request.getHeaders().set(proxygen::HTTP_HEADER_CONTENT_LENGTH, std::to_string(body.size()));
        return request;
    }
}
