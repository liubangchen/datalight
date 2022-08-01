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
#include "server/TrinoTask.h"

#include <velox/common/base/Exceptions.h>
#include <velox/common/time/Timer.h>
#include <velox/exec/Operator.h>

#include "common/Exception.h"
#include "common/Utils.h"

using namespace facebook::velox;

namespace datalight::trino {

namespace {

protocol::TaskState toPrestoTaskState(exec::TaskState state) {
  switch (state) {
    case exec::kRunning:
      return protocol::TaskState::RUNNING;
    case exec::kFinished:
      return protocol::TaskState::FINISHED;
    case exec::kCanceled:
      return protocol::TaskState::CANCELED;
    case exec::kFailed:
      return protocol::TaskState::FAILED;
    case exec::kAborted:
    default:
      return protocol::TaskState::ABORTED;
  }
}

protocol::ExecutionFailureInfo toPrestoError(std::exception_ptr ex) {
  try {
    rethrow_exception(ex);
  } catch (const VeloxException& e) {
    return VeloxToPrestoExceptionTranslator::translate(e);
  } catch (const std::exception& e) {
    return VeloxToPrestoExceptionTranslator::translate(e);
  }
}

void setTiming(
    const CpuWallTiming& timing,
    int64_t& count,
    protocol::Duration& wall,
    protocol::Duration& cpu) {
  count = timing.count;
  wall = protocol::Duration(timing.wallNanos, protocol::TimeUnit::NANOSECONDS);
  cpu = protocol::Duration(timing.cpuNanos, protocol::TimeUnit::NANOSECONDS);
}

} // namespace

PrestoTask::PrestoTask(const std::string& taskId) : id(taskId) {
  // info.taskId = taskId;
}

void PrestoTask::updateHeartbeatLocked() {
  lastHeartbeatMs = velox::getCurrentTimeMs();
  info.lastHeartbeat = util::toISOTimestamp(lastHeartbeatMs);
}

uint64_t PrestoTask::timeSinceLastHeartbeatMs() const {
  std::lock_guard<std::mutex> l(mutex);
  if (lastHeartbeatMs == 0UL) {
    return 0UL;
  }
  return getCurrentTimeMs() - lastHeartbeatMs;
}

protocol::TaskStatus PrestoTask::updateStatusLocked() {
  if (!taskStarted) {
    info.taskStatus.state = protocol::TaskState::RUNNING;
    return info.taskStatus;
  }

  // Error occurs when creating task or even before task is created. Set error
  // and return immediately
  if (error != nullptr) {
    if (info.taskStatus.failures.empty()) {
      info.taskStatus.failures.emplace_back(toPrestoError(error));
    }
    info.taskStatus.state = protocol::TaskState::FAILED;
    return info.taskStatus;
  }
  VELOX_CHECK_NOT_NULL(task, "task is null when updating status")
  const auto taskStats = task->taskStats();

  // Presto has a Driver per split. when splits represent partitions
  // of data, there is a queue of them per Task. We represent
  // processed/queued splits as Drivers for Presto.
  info.taskStatus.queuedPartitionedDrivers = taskStats.numQueuedSplits;
  info.taskStatus.runningPartitionedDrivers = taskStats.numRunningSplits;

  // TODO(spershin): Note, we dont' clean the stats.completedSplitGroups
  // and it seems not required now, but we might want to do it one day.
  for (auto splitGroupId : taskStats.completedSplitGroups) {
    // info.taskStatus.completedDriverGroups.push_back({true, splitGroupId});
  }
  info.taskStatus.state = toPrestoTaskState(task->state());

  auto tracker = task->pool()->getMemoryUsageTracker();
  // info.taskStatus.memoryReservationInBytes = tracker->getCurrentUserBytes();
  // info.taskStatus.systemMemoryReservationInBytes =
  //     tracker->getCurrentSystemBytes();
  // info.taskStatus.peakNodeTotalMemoryReservationInBytes =
  //     task->queryCtx()->pool()->getMemoryUsageTracker()->getPeakTotalBytes();

  if (task->error() && info.taskStatus.failures.empty()) {
    info.taskStatus.failures.emplace_back(toPrestoError(task->error()));
  }
  return info.taskStatus;
}

protocol::TaskInfo PrestoTask::updateInfoLocked() {
  return info;
}

std::string PrestoTask::taskNumbersToString(
    const std::array<size_t, 5>& taskNumbers) {
  static constexpr std::array<folly::StringPiece, 5> taskStateNames{
      "Running",
      "Finished",
      "Canceled",
      "Aborted",
      "Failed",
  };

  std::string str;
  for (size_t i = 0; i < taskNumbers.size(); ++i) {
    if (taskNumbers[i] != 0) {
      folly::toAppend(
          fmt::format("{}={} ", taskStateNames[i], taskNumbers[i]), &str);
    }
  }
  return str;
}

} // namespace datalight::trino
