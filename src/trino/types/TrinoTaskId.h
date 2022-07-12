#pragma once
#include <stdexcept>
#include <string>

namespace datalight::types
{
    class TrinoTaskId
    {
    public:
        explicit TrinoTaskId(const std::string & taskId)
        {
            int start = 0;
            auto pos = nextDot(taskId, start);
            queryId_ = taskId.substr(0, pos);

            start = pos + 1;
            pos = nextDot(taskId, start);
            stageId_ = parseInt(taskId, start, pos);

            start = pos + 1;
            pos = nextDot(taskId, start);
            stageExecutionId_ = parseInt(taskId, start, pos);

            start = pos + 1;
            id_ = parseInt(taskId, start, taskId.length());
        }

        const std::string & queryId() const { return queryId_; }

        int32_t stageId() const { return stageId_; }

        int32_t stageExecutionId() const { return stageExecutionId_; }

        int32_t id() const { return id_; }
    private:
        int nextDot(const std::string & taskId, int start)
    {
        auto pos = taskId.find(".", start);
        if (pos == std::string::npos)
        {
            throw std::invalid_argument("Malformed task ID: " + taskId);
        }
        return pos;
    }

    int parseInt(const std::string & taskId, int start, int end)
    {
        auto string = taskId.substr(start, end - start);
        return atoi(string.c_str());
    }

    std::string queryId_;
    int32_t stageId_;
    int32_t stageExecutionId_;
    int32_t id_;
    };

}
