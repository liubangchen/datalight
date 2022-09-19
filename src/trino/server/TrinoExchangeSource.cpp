/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "server/TrinoExchangeSource.h"

#include <fmt/core.h>
#include <folly/SocketAddress.h>
#include <re2/re2.h>
#include <velox/common/base/Exceptions.h>
#include <velox/exec/Operator.h>
#include <sstream>
#include "common/Configs.h"
#include <hash-library/sha256.h>
#include <jwt/jwt.h>

#include "protocol/TrinoProtocol.h"
#include "server/QueryContextManager.h"

using namespace facebook::velox;

using min = std::chrono::minutes;

namespace datalight::trino {

    namespace {

        std::string extractTaskId(const std::string& path) {
            static const RE2 kPattern("/v1/task/([^/]*)/.*");
            std::string taskId;
            if (RE2::PartialMatch(path, kPattern, &taskId)) {
                return taskId;
            }

            VLOG(1) << "Failed to extract task ID from remote split: " << path;

            throw std::invalid_argument(
                fmt::format("Cannot extract task ID from remote split URL: {}", path));
        }

        void onFinalFailure(
            const std::string& errorMessage,
            std::shared_ptr<exec::ExchangeQueue> queue) {
            VLOG(1) << errorMessage;

            std::lock_guard<std::mutex> l(queue->mutex());
            queue->setErrorLocked(errorMessage);
        }
    } // namespace

    TrinoExchangeSource::TrinoExchangeSource(
        const folly::Uri& baseUri,
        int destination,
        std::shared_ptr<exec::ExchangeQueue> queue,
        memory::MemoryPool* pool)
    : ExchangeSource(extractTaskId(baseUri.path()), destination, queue, pool),
      basePath_(baseUri.path()),
      host_(baseUri.host()),
      port_(baseUri.port()) {
  folly::SocketAddress address(folly::IPAddress(host_).str(), port_, true);
  auto* eventBase = folly::getUnsafeMutableGlobalEventBase();
  httpClient_ = std::make_unique<http::HttpClient>(
      eventBase, address, std::chrono::milliseconds(10'000));
}

bool TrinoExchangeSource::shouldRequestLocked() {
  if (atEnd_) {
    return false;
  }
  bool pending = requestPending_;
  requestPending_ = true;
  return !pending;
}

void TrinoExchangeSource::request() {
  failedAttempts_ = 0;
  doRequest();
}

void TrinoExchangeSource::doRequest() {
  if (closed_.load()) {
    std::lock_guard<std::mutex> l(queue_->mutex());
    queue_->setErrorLocked("TrinoExchangeSource closed");
    return;
  }
  auto path = fmt::format("{}/{}", basePath_, sequence_);
  VLOG(1) << "Fetching data from " << host_ << ":" << port_ << " " << path;
  auto self = getSelfPtr();
  http::RequestBuilder()
      .method(proxygen::HTTPMethod::GET)
      .url(path)
      .header(protocol::TRINO_MAX_SIZE_HTTP_HEADER, "32MB")
      .header("X-Trino-Internal-Bearer", getJwtToken())
      .send(httpClient_.get())
      .via(driverCPUExecutor())
      .thenValue([path, self](std::unique_ptr<http::HttpResponse> response) {
          auto* headers = response->headers();
          if (headers->getStatusCode() != http::kHttpOk &&
              headers->getStatusCode() != http::kHttpNoContent) {
              self->processDataError(
                  path,
                  fmt::format(
                      "Received HTTP {} {}",
                      headers->getStatusCode(),
                      headers->getStatusMessage()));
          } else {
              self->processDataResponse(std::move(response));
          }
      })
      .thenError(
          folly::tag_t<std::exception>{},
          [path, self](const std::exception& e) {
              self->processDataError(path, e.what());
          });
};

void TrinoExchangeSource::processDataResponse(
    std::unique_ptr<http::HttpResponse> response) {
    auto* headers = response->headers();
    VELOX_CHECK(
        !headers->getIsChunked(),
        "Chunked http transferring encoding is not supported.")
        uint64_t contentLength =
        atol(headers->getHeaders()
             .getSingleOrEmpty(proxygen::HTTP_HEADER_CONTENT_LENGTH)
             .c_str());
    VLOG(1) << "Fetched data for " << basePath_ << "/" << sequence_ << ": "
          << contentLength << " bytes";

  auto complete = headers->getHeaders()
                      .getSingleOrEmpty(protocol::TRINO_BUFFER_COMPLETE_HEADER)
                      .compare("true") == 0;
  if (complete) {
    VLOG(1) << "Received buffer-complete header for " << basePath_ << "/"
            << sequence_;
  }

  int64_t ackSequence =
      atol(headers->getHeaders()
               .getSingleOrEmpty(protocol::TRINO_PAGE_NEXT_TOKEN_HEADER)
               .c_str());

  std::unique_ptr<exec::SerializedPage> page;
  std::unique_ptr<folly::IOBuf> singleChain;
  bool empty = response->empty();
  if (!empty) {
    auto iobufs = response->consumeBody();
    for (auto& buf : iobufs) {
      if (!singleChain) {
        singleChain = std::move(buf);
      } else {
        singleChain->prev()->appendChain(std::move(buf));
      }
    }
    page = std::make_unique<exec::SerializedPage>(
        std::move(singleChain),
        pool_,
        [mappedMemory = response->mappedMemory()](folly::IOBuf& iobuf) {
          // Free the backed memory from MappedMemory on page dtor
          folly::IOBuf* start = &iobuf;
          auto curr = start;
          do {
            mappedMemory->freeBytes(curr->writableData(), curr->length());
            curr = curr->next();
          } while (curr != start);
        });
  }

  {
    std::lock_guard<std::mutex> l(queue_->mutex());
    if (page) {
      VLOG(1) << "Enqueuing page for " << basePath_ << "/" << sequence_ << ": "
              << page->size() << " bytes";
      queue_->enqueue(std::move(page));
    }
    if (complete) {
      VLOG(1) << "Enqueuing empty page for " << basePath_ << "/" << sequence_;
      atEnd_ = true;
      queue_->enqueue(nullptr);
    }

    sequence_ = ackSequence;

    // Reset requestPending_ if the response is complete or have pages.
    if (complete || !empty) {
      requestPending_ = false;
    }
  }

  if (complete) {
    abortResults();
  } else {
    if (!empty) {
      // Acknowledge results for non-empty content.
      acknowledgeResults(ackSequence);
    } else {
        // Rerequest results for incomplete results with no pages.
      request();
    }
  }
}

void TrinoExchangeSource::processDataError(
    const std::string& path,
    const std::string& error) {
  failedAttempts_++;
  if (failedAttempts_ < 3) {
    VLOG(1) << "Failed to fetch data from " << host_ << ":" << port_ << " "
            << path << " - Retrying: " << error;

    doRequest();
    return;
  }

  onFinalFailure(
      fmt::format(
          "Failed to fetched data from {}:{} {} - Exhausted retries: {}",
          host_,
          port_,
          path,
          error),
      queue_);
}

void TrinoExchangeSource::acknowledgeResults(int64_t ackSequence) {
  auto ackPath = fmt::format("{}/{}/acknowledge", basePath_, ackSequence);
  VLOG(1) << "Sending ack " << ackPath;
  auto self = getSelfPtr();

  http::RequestBuilder()
      .method(proxygen::HTTPMethod::GET)
      .url(ackPath)
      .header("X-Trino-Internal-Bearer", getJwtToken())
      .send(httpClient_.get())
      .via(driverCPUExecutor())
      .thenValue([self](std::unique_ptr<http::HttpResponse> response) {
        VLOG(1) << "Ack " << response->headers()->getStatusCode();
      })
      .thenError(
          folly::tag_t<std::exception>{}, [self](const std::exception& e) {
            // Acks are optional. No need to fail the query.
              VLOG(1) << "Ack failed: " << e.what();
          });
}

    std::string TrinoExchangeSource::getJwtToken(){
        auto nodeConfig = NodeConfig::instance();
        auto environment_ = nodeConfig->nodeEnvironment();
        auto nodeId_ = nodeConfig->nodeId();

        hashlibrary::SHA256 sha256;
        sha256.add(environment_.c_str(), environment_.size());
        unsigned char sbuf[hashlibrary::SHA256::HashBytes];
        sha256.getHash(sbuf);
        std::string hash_key(sbuf, sbuf + sizeof sbuf / sizeof sbuf[0]);
        const auto time = jwt::date::clock::now();
        auto token = jwt::create()
            .set_subject(nodeId_)
            .set_expires_at(time + min{5})
            .sign(jwt::algorithm::hs256(hash_key));
        return token;
    }

    void TrinoExchangeSource::abortResults() {
        VLOG(1) << "Sending abort results " << basePath_;
        auto queue = queue_;
        auto self = getSelfPtr();

        http::RequestBuilder()
            .method(proxygen::HTTPMethod::DELETE)
            .url(basePath_)
            .header("X-Trino-Internal-Bearer", getJwtToken())
            .send(httpClient_.get())
            .via(driverCPUExecutor())
            .thenValue([queue, self](std::unique_ptr<http::HttpResponse> response) {
                auto statusCode = response->headers()->getStatusCode();
                if (statusCode != http::kHttpOk && statusCode != http::kHttpNoContent) {
                    const std::string errMsg = fmt::format(
                        "Abort results failed: {}, path {}", statusCode, self->basePath_);
                    LOG(ERROR) << errMsg;
                    onFinalFailure(errMsg, queue);
                } else {
                    self->abortResultsSucceeded_.store(true);
                }
            })
            .thenError(
                folly::tag_t<std::exception>{},
                [queue, self](const std::exception& e) {
                    const std::string errMsg = fmt::format(
                        "Abort results failed: {}, path {}", e.what(), self->basePath_);
                    LOG(ERROR) << errMsg;
                    // Captures 'queue' by value to ensure lifetime. Error
                    // detection can be arbitrarily late, for example after cancellation
                    // due to other errors.
                    onFinalFailure(errMsg, queue);
                });
    }

    void TrinoExchangeSource::close() {
        closed_.store(true);
        if (!abortResultsSucceeded_.load()) {
            abortResults();
        }
    }

    std::shared_ptr<TrinoExchangeSource> TrinoExchangeSource::getSelfPtr() {
        return std::dynamic_pointer_cast<TrinoExchangeSource>(shared_from_this());
    }

// static
    std::unique_ptr<exec::ExchangeSource>
    TrinoExchangeSource::createExchangeSource(
        const std::string& url,
        int destination,
        std::shared_ptr<exec::ExchangeQueue> queue,
        memory::MemoryPool* pool) {
        if (strncmp(url.c_str(), "http://", 7) == 0) {
            return std::make_unique<TrinoExchangeSource>(
                folly::Uri(url), destination, queue, pool);
        }
        return nullptr;
    }


}
