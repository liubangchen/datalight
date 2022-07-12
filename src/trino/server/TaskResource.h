#pragma once

#include "TaskManager.h"
#include "http/HttpServer.h"
#include "velox/common/memory/Memory.h"

namespace datalight::server
{
    class TaskResource
    {
    public:
        explicit TaskResource(TaskManager & taskManager) : taskManager_(taskManager), pool_(velox::memory::getDefaultScopedMemoryPool()) { }
        void registerUris(http::HttpServer & server);
        velox::memory::ScopedMemoryPool * getPool() const { return pool_.get(); }

    private:

        proxygen::RequestHandler * abortResults(proxygen::HTTPMessage * message, const std::vector<std::string> & pathMatch);
        proxygen::RequestHandler * acknowledgeResults(proxygen::HTTPMessage * message, const std::vector<std::string> & pathMatch);
        proxygen::RequestHandler * createOrUpdateTask(proxygen::HTTPMessage * message, const std::vector<std::string> & pathMatch);
        proxygen::RequestHandler * deleteTask(proxygen::HTTPMessage * message, const std::vector<std::string> & pathMatch);
        proxygen::RequestHandler * getResults(proxygen::HTTPMessage * message, const std::vector<std::string> & pathMatch);
        proxygen::RequestHandler * getTaskStatus(proxygen::HTTPMessage * message, const std::vector<std::string> & pathMatch);
        proxygen::RequestHandler * getTaskInfo(proxygen::HTTPMessage * message, const std::vector<std::string> & pathMatch);
        proxygen::RequestHandler * removeRemoteSource(proxygen::HTTPMessage * message, const std::vector<std::string> & pathMatch);
        TaskManager & taskManager_;
        std::unique_ptr<velox::memory::ScopedMemoryPool> pool_;

    };
}
