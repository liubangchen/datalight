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
#include "types/TrinoToVeloxSplit.h"

#include <velox/connectors/hive/HiveConnectorSplit.h>
#include <velox/exec/Exchange.h>
#include <optional>

using namespace facebook::velox;

namespace datalight::trino {

namespace {

dwio::common::FileFormat toVeloxFileFormat(
    const protocol::String& format) {
    if (format == "com.facebook.hive.orc.OrcInputFormat") {
        return dwio::common::FileFormat::DWRF;
    } else if (
        format ==
        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat") {
        return dwio::common::FileFormat::PARQUET;
    } else {
        VELOX_FAIL("Unknown file format {}", format);
    }
}

} // anonymous namespace

facebook::velox::exec::Split toVeloxSplit(
    const protocol::ScheduledSplit& scheduledSplit) {
  const auto& connectorSplit = scheduledSplit.split.connectorSplit;
  const auto splitGroupId =  -1;
  if (auto hiveSplit = std::dynamic_pointer_cast<const protocol::HiveSplit>(
          connectorSplit)) {
    std::unordered_map<std::string, std::optional<std::string>> partitionKeys;
    for (const auto& entry : hiveSplit->partitionKeys) {
      partitionKeys.emplace(
          entry.name,
          entry.value.size() == 0 ? std::nullopt
          : std::optional<std::string>{entry.value});
    }

    return facebook::velox::exec::Split(
        std::make_shared<connector::hive::HiveConnectorSplit>(
            //scheduledSplit.split.connectorId,
            "hive",
            hiveSplit->path,
            toVeloxFileFormat("com.facebook.hive.orc.OrcInputFormat"),
            hiveSplit->start,
            hiveSplit->length,
            partitionKeys,
            hiveSplit->tableBucketNumber
                ? std::optional<int>(*hiveSplit->tableBucketNumber)
                : std::nullopt),
        splitGroupId);
  }
  if (auto remoteSplit = std::dynamic_pointer_cast<const protocol::RemoteSplit>(
          connectorSplit)) {

      if(auto exInput = std::dynamic_pointer_cast<const protocol::DirectExchangeInput>(remoteSplit->exchangeInput)){

          return  facebook::velox::exec::Split(
              std::make_shared<exec::RemoteConnectorSplit>(
                  exInput->location),
              splitGroupId);

      }else if(auto exInput = std::dynamic_pointer_cast<const protocol::SpoolingExchangeInput>(remoteSplit->exchangeInput)){
          VELOX_CHECK(false, "Unknown ExchangeInput type SpoolingExchangeInput");
      }else{
          VELOX_CHECK(false, "Unknown split type unknow");
      }
  }

  if (std::dynamic_pointer_cast<const protocol::EmptySplit>(connectorSplit)) {
      // We return NULL for empty splits to signal to do nothing.
      return facebook::velox::exec::Split(nullptr, splitGroupId);
  }

  VELOX_CHECK(false, "Unknown split type {}", connectorSplit->_type);
}

} // namespace datalight::trino
