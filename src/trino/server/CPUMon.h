#pragma once

#include <atomic>
#include <vector>

namespace datalight::server {

/// Used to keep track of the system's CPU usage.
    class CPUMon {
    public:
        /// Call this periodically to update the CPU load. Not thread-safe.
        void update();

        /// Returns the current (latest) CPU load. Thread-safe.
        inline double getCPULoadPct() {
            return cpuLoadPct_.load();
        }

    private:
        std::vector<uint64_t> prev_{8};
        bool firstTime_{true};
        std::atomic<double> cpuLoadPct_{0.0};
    };

} // namespace facebook::presto
