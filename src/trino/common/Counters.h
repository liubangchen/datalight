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
#pragma once

#include <folly/Range.h>

// Here we have all the counters presto cpp worker would export.
namespace datalight::trino {

// Sets up all the counters in the presto cpp, but specifying their types.
// See velox/common/base/StatsReporter.h for the interface.
void registerPrestoCppCounters();

constexpr folly::StringPiece kCounterDriverCPUExecutorQueueSize{
    "trino_cpp.driver_cpu_executor_queue_size"};
constexpr folly::StringPiece kCounterDriverCPUExecutorLatencyMs{
    "trino_cpp.driver_cpu_executor_latency_ms"};

constexpr folly::StringPiece kCounterHTTPExecutorLatencyMs{
    "trino_cpp.http_executor_latency_ms"};
constexpr folly::StringPiece kCounterNumHTTPRequest{
    "trino_cpp.num_http_request"};
constexpr folly::StringPiece kCounterNumHTTPRequestError{
    "trino_cpp.num_http_request_error"};
constexpr folly::StringPiece kCounterHTTPRequestLatencyMs{
    "trino_cpp.http_request_latency_ms"};

constexpr folly::StringPiece kCounterNumQueryContexts{
    "trino_cpp.num_query_contexts"};

constexpr folly::StringPiece kCounterNumTasks{"trino_cpp.num_tasks"};
constexpr folly::StringPiece kCounterNumTasksRunning{
    "trino_cpp.num_tasks_running"};
constexpr folly::StringPiece kCounterNumTasksFinished{
    "trino_cpp.num_tasks_finished"};
constexpr folly::StringPiece kCounterNumTasksCancelled{
    "trino_cpp.num_tasks_cancelled"};
constexpr folly::StringPiece kCounterNumTasksAborted{
    "trino_cpp.num_tasks_aborted"};
constexpr folly::StringPiece kCounterNumTasksFailed{
    "trino_cpp.num_tasks_failed"};
constexpr folly::StringPiece kCounterNumZombieTasks{
    "trino_cpp.num_zombie_tasks"};
constexpr folly::StringPiece kCounterNumZombiePrestoTasks{
    "trino_cpp.num_zombie_presto_tasks"};
constexpr folly::StringPiece kCounterNumRunningDrivers{
    "trino_cpp.num_running_drivers"};
constexpr folly::StringPiece kCounterNumBlockedDrivers{
    "trino_cpp.num_blocked_drivers"};

// Number of bytes of memory MappedMemory currently maps (RSS). It also includes
// memory that was freed and currently not in use.
constexpr folly::StringPiece kCounterMappedMemoryBytes{
    "trino_cpp.mapped_memory_bytes"};
// Number of bytes of memory MappedMemory currently allocates. Memories in use
constexpr folly::StringPiece kCounterAllocatedMemoryBytes{
    "trino_cpp.allocated_memory_bytes"};

// Number of total PartitionedOutputBuffer managed by all
// PartitionedOutputBufferManager
constexpr folly::StringPiece kCounterTotalPartitionedOutputBuffer{
    "trino_cpp.num_partitioned_output_buffer"};
} // namespace datalight::trino
