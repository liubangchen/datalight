#pragma once

#include <folly/futures/Future.h>
#include <proxygen/lib/http/HTTPConnector.h>
#include <proxygen/lib/http/connpool/SessionPool.h>
#include <proxygen/lib/http/session/HTTPUpstreamSession.h>
#include "HttpConstants.h"

namespace datalight::http
{

struct HttpResponse
{
    std::unique_ptr<proxygen::HTTPMessage> headers;
    std::vector<std::unique_ptr<folly::IOBuf>> bodyChain;

    explicit HttpResponse(std::unique_ptr<proxygen::HTTPMessage> _headers) : headers(std::move(_headers)) { }

    std::string dumpBodyChain() const
    {
        std::string responseBody;
        if (!bodyChain.empty())
        {
            std::ostringstream oss;
            for (auto & buf : bodyChain)
            {
                oss << std::string((const char *)buf->data(), buf->length());
            }
            responseBody = oss.str();
        }
        return responseBody;
    }
};

class HttpClient
{
public:
    HttpClient(folly::EventBase * eventBase, const folly::SocketAddress & address, std::chrono::milliseconds timeout);

    ~HttpClient();

    // TODO Avoid copy by using IOBuf for body
    folly::SemiFuture<std::unique_ptr<HttpResponse>> sendRequest(const proxygen::HTTPMessage & request, const std::string & body = "");

private:
    folly::EventBase * eventBase_;
    const folly::SocketAddress address_;
    const folly::HHWheelTimer::UniquePtr timer_;
    std::unique_ptr<proxygen::SessionPool> sessionPool_;
};

class RequestBuilder
{
public:
    RequestBuilder() { headers_.setHTTPVersion(1, 1); }

    RequestBuilder & method(proxygen::HTTPMethod method)
    {
        headers_.setMethod(method);
        return *this;
    }

    RequestBuilder & url(const std::string & url)
    {
        headers_.setURL(url);
        return *this;
    }

    RequestBuilder & header(proxygen::HTTPHeaderCode code, const std::string & value)
    {
        headers_.getHeaders().set(code, value);
        return *this;
    }

    RequestBuilder & header(const std::string & header, const std::string & value)
    {
        headers_.getHeaders().set(header, value);
        return *this;
    }

    folly::SemiFuture<std::unique_ptr<HttpResponse>> send(HttpClient * client, const std::string & body = "")
    {
        header(proxygen::HTTP_HEADER_CONTENT_LENGTH, std::to_string(body.size()));
        headers_.ensureHostHeader();
        return client->sendRequest(headers_, body);
    }

private:
    proxygen::HTTPMessage headers_;
};

}
