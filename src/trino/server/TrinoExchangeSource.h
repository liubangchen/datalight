#pragma once

#include <folly/Uri.h>

#include "http/HttpClient.h"
#include "velox/exec/Exchange.h"

using namespace facebook;

namespace datalight::server
{
    class TrinoExchangeSource : public velox::exec::ExchangeSource {
    public:
        TrinoExchangeSource(
            const folly::Uri& baseUri,
            int destination,
            std::shared_ptr<velox::exec::ExchangeQueue> queue);

        bool shouldRequestLocked() override;

        static std::unique_ptr<ExchangeSource> createExchangeSource(
            const std::string& url,
            int destination,
            std::shared_ptr<velox::exec::ExchangeQueue> queue);

    private:
        void request() override;

        void close() override {
            closed_.store(true);
        }

        void doRequest();

        void processDataResponse(std::unique_ptr<http::HttpResponse> response);

        void processDataError(const std::string& path, const std::string& error);

        void acknowledgeResults(int64_t ackSequence);

        void abortResults();

        // Returns a shared ptr owning the current object.
        std::shared_ptr<TrinoExchangeSource> getSelfPtr();

        const std::string basePath_;
        const std::string host_;
        const uint16_t port_;
        std::unique_ptr<http::HttpClient> httpClient_;
        int failedAttempts_;
        std::atomic_bool closed_{false};
    };
}
