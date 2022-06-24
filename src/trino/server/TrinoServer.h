#pragma once
#include<string>
#include <http/HttpServer.h>

namespace datalight
{
    class TrinoServer
    {
    public:
    TrinoServer();
    void run();
    void stop();
    virtual ~TrinoServer();

    protected:
    std::string nodeId_;
    std::unique_ptr<http::HttpServer> httpServer_;
    };

}
