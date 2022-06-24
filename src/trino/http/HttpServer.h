#pragma once

#include <fmt/core.h>
#include <proxygen/httpserver/HTTPServer.h>
#include <proxygen/httpserver/RequestHandlerFactory.h>
#include <proxygen/httpserver/ResponseBuilder.h>
#include <re2/re2.h>

namespace datalight::http
{

    class AbstractRequestHandler : public proxygen::RequestHandler
    {
    public:
        void onRequest(std::unique_ptr<proxygen::HTTPMessage> headers) noexcept override
        {
            //REPORT_ADD_STAT_VALUE(kCounterNumHTTPRequest, 1);
            startTime_ = std::chrono::steady_clock::now();
            headers_ = std::move(headers);
            body_.clear();
        }

        void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override { body_.emplace_back(std::move(body)); }

        void onUpgrade(proxygen::UpgradeProtocol proto) noexcept override { }

        void requestComplete() noexcept override { delete this; }

        void onError(proxygen::ProxygenError err) noexcept override
        {
            //REPORT_ADD_STAT_VALUE(kCounterNumHTTPRequestError, 1);
            delete this;
        }

    protected:
        std::chrono::steady_clock::time_point startTime_;
        std::unique_ptr<proxygen::HTTPMessage> headers_;
        std::vector<std::unique_ptr<folly::IOBuf>> body_;
    };

    class CallbackRequestHandlerState
    {
public:
    ~CallbackRequestHandlerState() { finalize(); }

    void finalize()
    {
        requestExpired_ = true;
        if (onFinalizationCallback_)
        {
            onFinalizationCallback_();
            onFinalizationCallback_ = std::function<void(void)>();
        }
    }

    // The function 'fn' will run on the thread that invoked onEOM()
    void runOnFinalization(std::function<void(void)> callback) { onFinalizationCallback_ = callback; }

    bool requestExpired() const { return requestExpired_; }

    static std::shared_ptr<CallbackRequestHandlerState> create() { return std::make_shared<CallbackRequestHandlerState>(); }

private:
    std::function<void(void)> onFinalizationCallback_;
    bool requestExpired_{false};
};

using RequestHandlerCallback = std::function<void(
    proxygen::HTTPMessage *, std::vector<std::unique_ptr<folly::IOBuf>> & body, proxygen::ResponseHandler * downstream)>;

using AsyncRequestHandlerCallback = std::function<void(
    proxygen::HTTPMessage *,
    std::vector<std::unique_ptr<folly::IOBuf>> & body,
    proxygen::ResponseHandler * downstream,
    std::shared_ptr<CallbackRequestHandlerState> state)>;

class CallbackRequestHandler : public AbstractRequestHandler {
public:
    explicit CallbackRequestHandler(RequestHandlerCallback callback)
        : callback_(wrap(callback)) {}

    explicit CallbackRequestHandler(AsyncRequestHandlerCallback callback)
        : callback_(callback), state_{CallbackRequestHandlerState::create()} {}

    ~CallbackRequestHandler() override {
        if (state_) {
            state_->finalize();
        }
    }

    void onEOM() noexcept override {
        callback_(headers_.get(), body_, downstream_, state_);
    }

private:
    const AsyncRequestHandlerCallback callback_;
    std::shared_ptr<CallbackRequestHandlerState> state_;

    static AsyncRequestHandlerCallback wrap(RequestHandlerCallback callback) {
        return [callback](
            proxygen::HTTPMessage* headers,
            std::vector<std::unique_ptr<folly::IOBuf>>& body,
            proxygen::ResponseHandler* downstream,
            std::shared_ptr<CallbackRequestHandlerState> /* state */) {
            callback(headers, body, downstream);
        };
    }
};


class ErrorRequestHandler : public AbstractRequestHandler {
public:
ErrorRequestHandler(uint16_t errorCode, const std::string& errorMessage)
    : errorCode_(errorCode), errorMessage_(errorMessage) {}

    void onEOM() noexcept override {
        proxygen::ResponseBuilder(downstream_)
            .status(errorCode_, errorMessage_)
            .sendWithEOM();
    }

private:
    const uint16_t errorCode_;
    const std::string errorMessage_;
};

using EndpointRequestHandlerFactory = std::function<proxygen::RequestHandler*(
    proxygen::HTTPMessage* message,
    const std::vector<std::string>& args)>;

class DispatchingRequestHandlerFactory
    : public proxygen::RequestHandlerFactory {
public:
    void onServerStart(folly::EventBase* /*evb*/) noexcept override {}

    void onServerStop() noexcept override {}

    proxygen::RequestHandler* onRequest(
        proxygen::RequestHandler*,
        proxygen::HTTPMessage* message) noexcept override;

    void registerEndPoint(
        proxygen::HTTPMethod method,
        const std::string& pattern,
        const EndpointRequestHandlerFactory& endpoint);

private:
    class EndPoint {
    public:
    EndPoint(
        const std::string& pattern,
        const EndpointRequestHandlerFactory& factory)
        : re_(pattern), factory_(factory) {}

        proxygen::RequestHandler* checkAndApply(
            const std::string& path,
            proxygen::HTTPMessage* message,
            std::vector<std::string>& matches,
            std::vector<RE2::Arg>& args,
            std::vector<RE2::Arg*>& argPtrs) const;

    private:
        RE2 re_;
        EndpointRequestHandlerFactory factory_;
    };

    std::unordered_map<
        proxygen::HTTPMethod,
        std::vector<std::unique_ptr<EndPoint>>>
        endpoints_;
};


class HttpServer {
public:
    explicit HttpServer(
        const folly::SocketAddress& httpAddress,
        int httpExecThreads = 8);

    void start(
        std::function<void(proxygen::HTTPServer* /*server*/)> onSuccess = nullptr,
        std::function<void(std::exception_ptr)> onError = nullptr);

    folly::IOThreadPoolExecutor* getExecutor() {
        return httpExecutor_.get();
    }

    void stop() {
        server_->stop();
    }

    void registerGet(
        const std::string& pattern,
        const EndpointRequestHandlerFactory& endpoint) {
        handlerFactory_->registerEndPoint(
            proxygen::HTTPMethod::GET, pattern, endpoint);
    }

    void registerGet(
        const std::string& pattern,
        const RequestHandlerCallback& callback) {
        registerGet(pattern, endPointWrapper(callback));
    }

    void registerHead(
        const std::string& pattern,
        const EndpointRequestHandlerFactory& endpoint) {
        handlerFactory_->registerEndPoint(
            proxygen::HTTPMethod::HEAD, pattern, endpoint);
    }

    void registerHead(
        const std::string& pattern,
        const RequestHandlerCallback& callback) {
        registerHead(pattern, endPointWrapper(callback));
    }

    void registerPost(
        const std::string& pattern,
        const EndpointRequestHandlerFactory& endpoint) {
        handlerFactory_->registerEndPoint(
            proxygen::HTTPMethod::POST, pattern, endpoint);
    }

    void registerPost(
        const std::string& pattern,
        const RequestHandlerCallback& callback) {
        registerPost(pattern, endPointWrapper(callback));
    }

    void registerPut(
        const std::string& pattern,
        const EndpointRequestHandlerFactory& endpoint) {
        handlerFactory_->registerEndPoint(
            proxygen::HTTPMethod::PUT, pattern, endpoint);
    }

    void registerPut(
        const std::string& pattern,
        const RequestHandlerCallback& callback) {
        registerPut(pattern, endPointWrapper(callback));
    }

    void registerDelete(
        const std::string& pattern,
        const EndpointRequestHandlerFactory& endpoint) {
        handlerFactory_->registerEndPoint(
            proxygen::HTTPMethod::DELETE, pattern, endpoint);
    }

    void registerDelete(
        const std::string& pattern,
        const RequestHandlerCallback& callback) {
        registerDelete(pattern, endPointWrapper(callback));
    }

private:
    const folly::SocketAddress httpAddress_;
    int httpExecThreads_;
    std::unique_ptr<DispatchingRequestHandlerFactory> handlerFactory_ =
        std::make_unique<DispatchingRequestHandlerFactory>();
    std::unique_ptr<proxygen::HTTPServer> server_;
    std::shared_ptr<folly::IOThreadPoolExecutor> httpExecutor_;

    static EndpointRequestHandlerFactory endPointWrapper(
        const RequestHandlerCallback& callback) {
        return [callback](
            proxygen::HTTPMessage* /* headers */,
            const std::vector<std::string>& /* args */) {
            return new CallbackRequestHandler(callback);
        };
    }
};

}
