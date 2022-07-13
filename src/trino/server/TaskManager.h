#pragma once


#pragma once

#include <memory>
#include <folly/Synchronized.h>
#include "QueryContextManager.h"
#include "TrinoTask.h"
#include "http/HttpServer.h"
#include "protocol/TrinoProtocol.h"
#include "velox/exec/PartitionedOutputBufferManager.h"

using namespace facebook;

namespace datalight::server
{
struct DriverCountStats
{
    size_t numBlockedDrivers{0};
    size_t numRunningDrivers{0};
};

class TaskManager
{
public:
    explicit TaskManager(
        std::unordered_map<std::string, std::string> properties = {}, std::unordered_map<std::string, std::string> nodeProperties = {});

    void setBaseUri(const std::string & baseUri) { baseUri_ = baseUri; }

    TaskMap tasks() const
    {
        return taskMap_.withRLock([](const auto & tasks) { return tasks; });
    }

    void abortResults(const protocol::TaskId & taskId, long bufferId);

    void acknowledgeResults(const protocol::TaskId & taskId, long bufferId, long token);

    // Creating an empty task that only contains the error information so that
    // next time coordinator checks for the status it retrieves the error.
    std::unique_ptr<protocol::TaskInfo> createOrUpdateErrorTask(const protocol::TaskId & taskId, const std::exception_ptr & exception);

    std::unique_ptr<protocol::TaskInfo> createOrUpdateTask(
        const protocol::TaskId & taskId,
        velox::core::PlanFragment planFragment,
        //const std::vector<protocol::TaskSource> & sources,
        const protocol::OutputBuffers & outputBuffers,
        std::unordered_map<std::string, std::string> && configStrings,
        std::unordered_map<std::string, std::unordered_map<std::string, std::string>> && connectorConfigStrings);

    std::unique_ptr<protocol::TaskInfo> deleteTask(const protocol::TaskId & taskId, bool abort);

    /// Remove old Finished, Cancelled, Failed and Aborted tasks.
    /// Old is being defined by the lifetime of the task.
    size_t cleanOldTasks();

    folly::Future<std::unique_ptr<protocol::TaskInfo>> getTaskInfo(
        const protocol::TaskId & taskId,
        bool summarize,
        std::optional<protocol::TaskState> currentState,
        std::optional<protocol::Duration> maxWait,
        std::shared_ptr<http::CallbackRequestHandlerState> state);

    folly::Future<std::unique_ptr<Result>> getResults(
        const protocol::TaskId & taskId,
        long bufferId,
        long token,
        protocol::DataSize maxSize,
        protocol::Duration maxWait,
        std::shared_ptr<http::CallbackRequestHandlerState> state);

    folly::Future<std::unique_ptr<protocol::TaskStatus>> getTaskStatus(
        const protocol::TaskId & taskId,
        std::optional<protocol::TaskState> currentState,
        std::optional<protocol::Duration> maxWait,
        std::shared_ptr<http::CallbackRequestHandlerState> state);

    void removeRemoteSource(const protocol::TaskId & taskId, const protocol::TaskId & remoteSourceTaskId);

    std::string toString() const;

    QueryContextManager * getQueryContextManager() { return &queryContextManager_; }

    const QueryContextManager * getQueryContextManager() const { return &queryContextManager_; }

    inline size_t getNumTasks() const { return taskMap_.rlock()->size(); }

    // Returns the number of running drivers in all tasks.
    DriverCountStats getDriverCountStats() const;

    // Returns array with number of tasks for each of five TaskState (enum defined
    // in exec/Task.h).
    std::array<size_t, 5> getTaskNumbers(size_t & numTasks) const;

public:
    static constexpr folly::StringPiece kMaxDriversPerTask{"max_drivers_per_task"};
    static constexpr folly::StringPiece kConcurrentLifespansPerTask{"concurrent_lifespans_per_task"};
    static constexpr folly::StringPiece kSessionTimezone{"session_timezone"};

private:
    std::shared_ptr<TrinoTask> findOrCreateTask(const protocol::TaskId & taskId);

    std::shared_ptr<TrinoTask> findOrCreateTaskLocked(TaskMap & taskMap, const protocol::TaskId & taskId);

    std::string baseUri_;
    std::shared_ptr<velox::exec::PartitionedOutputBufferManager> bufferManager_;
    folly::Synchronized<TaskMap> taskMap_;
    QueryContextManager queryContextManager_;
    int32_t maxDriversPerTask_;
    int32_t concurrentLifespansPerTask_;
};

}
