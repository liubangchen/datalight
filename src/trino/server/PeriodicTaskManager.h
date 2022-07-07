#pragma once

#include <folly/experimental/FunctionScheduler.h>

namespace folly {
    class CPUThreadPoolExecutor;
    class IOThreadPoolExecutor;
}

namespace datalight::server {
    //class TaskManager;
    class PeriodicTaskManager {
    public:
        explicit PeriodicTaskManager(
            folly::CPUThreadPoolExecutor* driverCPUExecutor,
            folly::IOThreadPoolExecutor* httpExecutor);

        ~PeriodicTaskManager() {
            stop();
        }

        // All the tasks will start here.
        void start();

        // Add a task to run periodically.
        template <typename TFunc>
            void addTask(TFunc&& func, size_t periodMicros, const std::string& taskName) {
            scheduler_.addFunction(
                func, std::chrono::microseconds{periodMicros}, taskName);
        }

        // Stops all periodic tasks. Returns only when everything is stopped.
        void stop();

    private:
        folly::FunctionScheduler scheduler_;
        folly::CPUThreadPoolExecutor* driverCPUExecutor_;
        folly::IOThreadPoolExecutor* httpExecutor_;
        //TaskManager* taskManager_;
    };
}
