#include "PeriodicTaskManager.h"
#include <folly/Range.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/stop_watch.h>
#include "velox/common/base/StatsReporter.h"
#include "velox/common/memory/MappedMemory.h"
#include "velox/exec/Driver.h"

namespace datalight::server
{
    constexpr folly::StringPiece kCounterDriverCPUExecutorQueueSize{"presto_cpp.driver_cpu_executor_queue_size"};
    constexpr folly::StringPiece kCounterDriverCPUExecutorLatencyMs{"presto_cpp.driver_cpu_executor_latency_ms"};
    constexpr folly::StringPiece kCounterHTTPExecutorLatencyMs{
        "presto_cpp.http_executor_latency_ms"};
// Every two seconds we export server counters.
    static constexpr size_t kTaskPeriodGlobalCounters{2'000'000}; // 2 seconds.
// Every two seconds we export memory counters.
    static constexpr size_t kMemoryPeriodGlobalCounters{2'000'000}; // 2 seconds.
// Every 1 minute we clean old tasks.
    static constexpr size_t kTaskPeriodCleanOldTasks{60'000'000}; // 60 seconds.

    PeriodicTaskManager::PeriodicTaskManager(folly::CPUThreadPoolExecutor * driverCPUExecutor, folly::IOThreadPoolExecutor * httpExecutor)
    : driverCPUExecutor_(driverCPUExecutor), httpExecutor_(httpExecutor)
    {
    }

    void PeriodicTaskManager::start()
    {
        if (driverCPUExecutor_ or httpExecutor_)
        {
            scheduler_.addFunction(
                [driverCPUExecutor = driverCPUExecutor_, httpExecutor = httpExecutor_]()
                    {
                        if (driverCPUExecutor)
                        {
                            // Report the current queue size of the thread pool.
                            //REPORT_ADD_STAT_VALUE(kCounterDriverCPUExecutorQueueSize, driverCPUExecutor->getTaskQueueSize());
                            // Report the latency between scheduling the task and its execution.
                            folly::stop_watch<std::chrono::milliseconds> timer;
                            //driverCPUExecutor->add([timer = timer]()
                            //    { REPORT_ADD_STAT_VALUE(kCounterDriverCPUExecutorLatencyMs, timer.elapsed().count()); });
                        }

                        if (httpExecutor)
                        {
                            // Report the latency between scheduling the task and its execution.
                            folly::stop_watch<std::chrono::milliseconds> timer;
                            //httpExecutor->add([timer = timer]() { REPORT_ADD_STAT_VALUE(kCounterHTTPExecutorLatencyMs, timer.elapsed().count()); });
                        }
                    },
                std::chrono::microseconds{kTaskPeriodGlobalCounters},
                "executor_counters");
        }

        scheduler_.start();
    }

    void PeriodicTaskManager::stop()
    {
        scheduler_.cancelAllFunctionsAndWait();
        scheduler_.shutdown();
    }
}
