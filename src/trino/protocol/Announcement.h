#pragma once
#include <fmt/core.h>
#include <nlohmann/json.hpp>
#include <proxygen/httpserver/RequestHandler.h>

namespace datalight::protocol
{
    std::string createAnnouncementBody()
    {
        nlohmann::json body
            = {{"environment", "production"},
               {"pool", "general"},
               {"nodeId", "e4901aae-a5c9-9ff7-97a9-5687835ad54c"},
               {"location", "/e4901aae-a5c9-9ff7-97a9-5687835ad54c"},
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
        proxygen::HTTPMessage request;
        request.setMethod(proxygen::HTTPMethod::PUT);
        request.setURL(fmt::format("/v1/announcement/{}", "e4901aae-a5c9-9ff7-97a9-5687835ad54c"));
        request.getHeaders().set(proxygen::HTTP_HEADER_HOST, fmt::format("{}:{}", "172.19.254.10", "9100"));
        request.getHeaders().set(proxygen::HTTP_HEADER_CONTENT_TYPE, "application/json");
        request.getHeaders().set(proxygen::HTTP_HEADER_CONTENT_LENGTH, std::to_string(body.size()));
        return request;
    }
}
