#include "QueryContextManager.h"

using namespace facebook::velox;

using datalight::protocol::QueryId;
using datalight::protocol::TaskId;
using namespace facebook;

DEFINE_int32(num_query_threads, std::thread::hardware_concurrency(), "Process-wide number of query execution threads");

namespace datalight::server
{

namespace
{
    static std::shared_ptr<folly::CPUThreadPoolExecutor> & executor()
    {
        static std::shared_ptr<folly::CPUThreadPoolExecutor> executor
            = std::make_shared<folly::CPUThreadPoolExecutor>(FLAGS_num_query_threads);
        return executor;
    }
}

folly::CPUThreadPoolExecutor * driverCPUExecutor()
{
    return executor().get();
}

std::shared_ptr<core::QueryCtx> QueryContextManager::findOrCreateQueryCtx(
    const TaskId & taskId,
    std::unordered_map<std::string, std::string> && configStrings,
    std::unordered_map<std::string, std::unordered_map<std::string, std::string>> && connectorConfigStrings)
{
    QueryId queryId = taskId.substr(0, taskId.find('.'));

    auto lockedCache = queryContextCache_.wlock();
    if (auto queryCtx = lockedCache->get(queryId))
    {
        return queryCtx;
    }

    // If `legacy_timestamp` is true, the coordinator expects timestamp
    // conversions without a timezone to be converted to the user's
    // session_timezone.
    auto it = configStrings.find("legacy_timestamp");

    // `legacy_timestamp` default value is true in the coordinator.
    if ((it == configStrings.end()) || (folly::to<bool>(it->second)))
    {
        configStrings.emplace(core::QueryConfig::kAdjustTimestampToTimezone, "true");
    }

    std::shared_ptr<Config> config = std::make_shared<core::MemConfig>(configStrings);
    std::unordered_map<std::string, std::shared_ptr<Config>> connectorConfigs;
    for (auto & entry : connectorConfigStrings)
    {
        connectorConfigs.insert({entry.first, std::make_shared<core::MemConfig>(entry.second)});
    }

    int64_t maxUserMemoryPerNode = getMaxMemoryPerNode(kQueryMaxMemoryPerNode, kDefaultMaxMemoryPerNode);
    int64_t maxSystemMemoryPerNode = kDefaultMaxMemoryPerNode;
    int64_t maxTotalMemoryPerNode = getMaxMemoryPerNode(kQueryMaxTotalMemoryPerNode, kDefaultMaxMemoryPerNode);

    auto pool = memory::getProcessDefaultMemoryManager().getRoot().addScopedChild("query_root");
    pool->setMemoryUsageTracker(
        velox::memory::MemoryUsageTracker::create(maxUserMemoryPerNode, maxSystemMemoryPerNode, maxTotalMemoryPerNode));

    auto queryCtx
        = std::make_shared<core::QueryCtx>(executor(), config, connectorConfigs, memory::MappedMemory::getInstance(), std::move(pool));

    return lockedCache->insert(queryId, queryCtx);
}

void QueryContextManager::visitAllContexts(std::function<void(const protocol::QueryId &, const velox::core::QueryCtx *)> visitor) const
{
    auto lockedCache = queryContextCache_.rlock();
    for (const auto & it : lockedCache->ctxs())
    {
        if (const auto queryCtxSP = it.second.first.lock())
        {
            visitor(it.first, queryCtxSP.get());
        }
    }
}

}
