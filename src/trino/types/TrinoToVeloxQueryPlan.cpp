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
#include "types/TrinoToVeloxQueryPlan.h"

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <folly/container/F14Set.h>
#include <velox/type/Filter.h>
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HivePartitionFunction.h"
#include "velox/exec/HashPartitionFunction.h"
#include "velox/exec/RoundRobinPartitionFunction.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

#include "types/TypeSignatureTypeConverter.h"

#if __has_include("filesystem")
#include <filesystem>
namespace fs = std::filesystem;
#else
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;
#endif

using namespace facebook::velox;

namespace datalight::trino {

    template <typename T>
    std::string toJsonString(const T& value) {
        return ((json)value).dump();
    }

    LocalPartitionNode::Type toLocalExchangeType(protocol::ExchangeNodeType type) {
        switch (type) {
        case protocol::ExchangeNodeType::GATHER:
            return velox::core::LocalPartitionNode::Type::kGather;
        case protocol::ExchangeNodeType::REPARTITION:
            return velox::core::LocalPartitionNode::Type::kRepartition;
        default:
            VELOX_UNSUPPORTED("Unsupported exchange type: {}", toJsonString(type));
        }
    }

    bool isFixedPartition(
        const std::shared_ptr<const protocol::ExchangeNode>& node,
        protocol::SystemPartitionFunction partitionFunction) {
        if (node->type != protocol::ExchangeNodeType::REPARTITION) {
            return false;
        }

        auto connectorHandle =
            node->partitioningScheme.partitioning.handle.connectorHandle;
        auto handle = std::dynamic_pointer_cast<protocol::SystemPartitioningHandle>(
            connectorHandle);
        if (!handle) {
            return false;
        }
        if (handle->partitioning != protocol::SystemPartitioning::FIXED) {
            return false;
        }
        if (handle->function != partitionFunction) {
            return false;
        }
        return true;
    }

    bool isHashPartition(
        const std::shared_ptr<const protocol::ExchangeNode>& node) {
        return isFixedPartition(node, protocol::SystemPartitionFunction::HASH);
    }

    bool isRoundRobinPartition(
        const std::shared_ptr<const protocol::ExchangeNode>& node) {
        return isFixedPartition(node, protocol::SystemPartitionFunction::ROUND_ROBIN);
    }

    std::shared_ptr<const velox::core::PlanNode>
    VeloxQueryPlanConverter::toVeloxQueryPlan(
        const std::shared_ptr<const protocol::PlanNode>& node,
        const protocol::TaskId& taskId) {
        if (auto exchange =
            std::dynamic_pointer_cast<const protocol::ExchangeNode>(node)) {
            return toVeloxQueryPlan(exchange, taskId);
        }
        if (auto filter =
            std::dynamic_pointer_cast<const protocol::FilterNode>(node)) {
            return toVeloxQueryPlan(filter, taskId);
        }
        if (auto project =
            std::dynamic_pointer_cast<const protocol::ProjectNode>(node)) {
            return toVeloxQueryPlan(project, taskId);
        }
        if (auto values =
            std::dynamic_pointer_cast<const protocol::ValuesNode>(node)) {
            return toVeloxQueryPlan(values, taskId);
        }
        if (auto tableScan =
            std::dynamic_pointer_cast<const protocol::TableScanNode>(node)) {
            return toVeloxQueryPlan(tableScan, taskId);
        }
        if (auto aggregation =
            std::dynamic_pointer_cast<const protocol::AggregationNode>(node)) {
            return toVeloxQueryPlan(aggregation, taskId);
        }
        if (auto groupId =
            std::dynamic_pointer_cast<const protocol::GroupIdNode>(node)) {
            return toVeloxQueryPlan(groupId, taskId);
        }
        if (auto distinctLimit =
            std::dynamic_pointer_cast<const protocol::DistinctLimitNode>(node)) {
            return toVeloxQueryPlan(distinctLimit, taskId);
        }
        if (auto join = std::dynamic_pointer_cast<const protocol::JoinNode>(node)) {
            return toVeloxQueryPlan(join, taskId);
        }
        /**
           if (auto join =
           std::dynamic_pointer_cast<const protocol::MergeJoinNode>(node)) {
           return toVeloxQueryPlan(join,  taskId);
           }
        **/
        if (auto remoteSource =
            std::dynamic_pointer_cast<const protocol::RemoteSourceNode>(node)) {
            return toVeloxQueryPlan(remoteSource, taskId);
        }
        if (auto topN = std::dynamic_pointer_cast<const protocol::TopNNode>(node)) {
            return toVeloxQueryPlan(topN, taskId);
        }
        if (auto limit = std::dynamic_pointer_cast<const protocol::LimitNode>(node)) {
            return toVeloxQueryPlan(limit, taskId);
        }
        if (auto sort = std::dynamic_pointer_cast<const protocol::SortNode>(node)) {
            return toVeloxQueryPlan(sort, taskId);
        }
        /**
           if (auto unnest =
           std::dynamic_pointer_cast<const protocol::UnnestNode>(node)) {
           return toVeloxQueryPlan(unnest, tableWriteInfo, taskId);
           }
           if (auto enforceSingleRow =
           std::dynamic_pointer_cast<const protocol::EnforceSingleRowNode>(
           node)) {
           return toVeloxQueryPlan(enforceSingleRow,  taskId);
           }
        **/
        if (auto tableWriter =
            std::dynamic_pointer_cast<const protocol::TableWriterNode>(node)) {
            return toVeloxQueryPlan(tableWriter, taskId);
        }
        /**
           if (auto assignUniqueId =
           std::dynamic_pointer_cast<const protocol::AssignUniqueId>(node)) {
           return toVeloxQueryPlan(assignUniqueId,  taskId);
           }
        **/
        VELOX_UNSUPPORTED("Unknown plan node type {}", node->_type);
    }

    std::shared_ptr<const velox::core::PlanNode>
    VeloxQueryPlanConverter::toVeloxQueryPlan(
        const std::shared_ptr<const protocol::ExchangeNode>& node,
        const protocol::TaskId& taskId) {
        VELOX_USER_CHECK(
            node->scope == protocol::ExchangeNodeScope::LOCAL,
            "Unsupported ExchangeNode scope");
        std::vector<std::shared_ptr<const PlanNode>> sourceNodes;
        sourceNodes.reserve(node->sources.size());
        /**
           for (const auto& source : node->sources) {
           sourceNodes.emplace_back(toVeloxQueryPlan(source, taskId));
           }

           if (node->orderingScheme) {
           std::vector<std::shared_ptr<const FieldAccessTypedExpr>> sortingKeys;
           std::vector<SortOrder> sortingOrders;
           sortingKeys.reserve(node->orderingScheme->orderBy.size());
           sortingOrders.reserve(node->orderingScheme->orderBy.size());
           for (const auto& orderBy : node->orderingScheme->orderBy) {
           // sortingKeys.emplace_back(exprConverter_.toVeloxExpr(orderBy));
           // sortingOrders.emplace_back(toVeloxSortOrder(orderBy));
           }
           return std::make_shared<velox::core::LocalMergeNode>(
           node->id, sortingKeys, sortingOrders, std::move(sourceNodes));
           }

           const auto type = toLocalExchangeType(node->type);

           const auto outputType = toRowType(node->partitioningScheme.outputLayout);

           RowTypePtr inputTypeFromSource = toRowType(node->inputs[0]);

           if (isHashPartition(node)) {
           auto partitionKeys = toFieldExprs(
           node->partitioningScheme.partitioning.arguments, exprConverter_);

           auto inputType = sourceNodes[0]->outputType();
           auto keyChannels = toChannels(inputType, partitionKeys);
           auto partitionFunctionFactory = [inputType,
           keyChannels](auto numPartitions) {
           return std::make_unique<velox::exec::HashPartitionFunction>(
           numPartitions, inputType, keyChannels);
           };

           return std::make_shared<velox::core::LocalPartitionNode>(
           node->id,
           type,
           partitionFunctionFactory,
           outputType,
           std::move(sourceNodes),
           std::move(inputTypeFromSource));
           }

           if (isRoundRobinPartition(node)) {
           auto partitionFunctionFactory = [](auto numPartitions) {
           return std::make_unique<velox::exec::RoundRobinPartitionFunction>(
           numPartitions);
           };

           return std::make_shared<velox::core::LocalPartitionNode>(
           node->id,
           type,
           partitionFunctionFactory,
           outputType,
           std::move(sourceNodes),
           std::move(inputTypeFromSource));
           }

           if (type == velox::core::LocalPartitionNode::Type::kGather) {
           return velox::core::LocalPartitionNode::gather(
           node->id,
           outputType,
           std::move(sourceNodes),
           std::move(inputTypeFromSource));
           }
        **/
        VELOX_UNSUPPORTED(
            "Unsupported flavor of local exchange: {}", toJsonString(node));
    }

    std::shared_ptr<const velox::core::ExchangeNode>
    VeloxQueryPlanConverter::toVeloxQueryPlan(
        const std::shared_ptr<const protocol::RemoteSourceNode>& node,
        const protocol::TaskId& taskId) {
        return nullptr;
    }

    std::shared_ptr<const velox::core::PlanNode>
    VeloxQueryPlanConverter::toVeloxQueryPlan(
        const std::shared_ptr<const protocol::FilterNode>& node,
        const protocol::TaskId& taskId) {
        return nullptr;
    }

    std::shared_ptr<const velox::core::PlanNode>
    VeloxQueryPlanConverter::toVeloxQueryPlan(
        const std::shared_ptr<const protocol::OutputNode>& node,
        const protocol::TaskId& taskId) {
        return nullptr;
    }

    std::shared_ptr<const velox::core::ProjectNode>
    VeloxQueryPlanConverter::toVeloxQueryPlan(
        const std::shared_ptr<const protocol::ProjectNode>& node,
        const protocol::TaskId& taskId) {
        return nullptr;
    }

    std::shared_ptr<const velox::core::ValuesNode>
    VeloxQueryPlanConverter::toVeloxQueryPlan(
        const std::shared_ptr<const protocol::ValuesNode>& node,
        const protocol::TaskId& taskId) {
        return nullptr;
    }

    std::shared_ptr<const velox::core::TableScanNode>
    VeloxQueryPlanConverter::toVeloxQueryPlan(
        const std::shared_ptr<const protocol::TableScanNode>& node,
        const protocol::TaskId& taskId) {
        return nullptr;
    }

    std::shared_ptr<const velox::core::AggregationNode>
    VeloxQueryPlanConverter::toVeloxQueryPlan(
        const std::shared_ptr<const protocol::AggregationNode>& node,
        const protocol::TaskId& taskId) {
        return nullptr;
    }

    std::shared_ptr<const velox::core::GroupIdNode>
    VeloxQueryPlanConverter::toVeloxQueryPlan(
        const std::shared_ptr<const protocol::GroupIdNode>& node,
        const protocol::TaskId& taskId) {
        return nullptr;
    }

    std::shared_ptr<const velox::core::PlanNode>
    VeloxQueryPlanConverter::toVeloxQueryPlan(
        const std::shared_ptr<const protocol::DistinctLimitNode>& node,
        const protocol::TaskId& taskId) {
        return nullptr;
    }

    std::shared_ptr<const velox::core::PlanNode>
    VeloxQueryPlanConverter::toVeloxQueryPlan(
        const std::shared_ptr<const protocol::JoinNode>& node,
        const protocol::TaskId& taskId) {
        return nullptr;
    }

    std::shared_ptr<const velox::core::TopNNode>
    VeloxQueryPlanConverter::toVeloxQueryPlan(
        const std::shared_ptr<const protocol::TopNNode>& node,
        const protocol::TaskId& taskId) {
        return nullptr;
    }

    std::shared_ptr<const velox::core::LimitNode>
    VeloxQueryPlanConverter::toVeloxQueryPlan(
        const std::shared_ptr<const protocol::LimitNode>& node,
        const protocol::TaskId& taskId) {
        return nullptr;
    }

    std::shared_ptr<const velox::core::OrderByNode>
    VeloxQueryPlanConverter::toVeloxQueryPlan(
        const std::shared_ptr<const protocol::SortNode>& node,
        const protocol::TaskId& taskId) {
        return nullptr;
    }

    std::shared_ptr<const velox::core::TableWriteNode>
    VeloxQueryPlanConverter::toVeloxQueryPlan(
        const std::shared_ptr<const protocol::TableWriterNode>& node,
        const protocol::TaskId& taskId) {
        return nullptr;
    }

    std::shared_ptr<const velox::core::ProjectNode>
    VeloxQueryPlanConverter::tryConvertOffsetLimit(
        const std::shared_ptr<const protocol::ProjectNode>& node,
        const protocol::TaskId& taskId) {
        return nullptr;
    }


    velox::core::PlanFragment VeloxQueryPlanConverter::toVeloxQueryPlan(
        const protocol::PlanFragment& fragment,
        const protocol::TaskId& taskId) {
        velox::core::PlanFragment planFragment;

        // Convert the fragment info first.
        planFragment.executionStrategy = velox::core::ExecutionStrategy::kUngrouped;
        planFragment.numSplitGroups = 1;

        if (auto output = std::dynamic_pointer_cast<const protocol::OutputNode>(
                fragment.root)) {
            planFragment.planNode = toVeloxQueryPlan(output, taskId);
            return planFragment;
        }

        auto partitioningScheme = fragment.partitioningScheme;
        auto partitioningHandle =
            partitioningScheme.partitioning.handle.connectorHandle;

        auto partitioningKeys = "";
        //    toTypedExprs(partitioningScheme.partitioning.arguments, exprConverter_);

        if (auto systemPartitioningHandle =
            std::dynamic_pointer_cast<protocol::SystemPartitioningHandle>(
                partitioningHandle)) {
            switch (systemPartitioningHandle->partitioning) {
            case protocol::SystemPartitioning::SINGLE:
                VELOX_CHECK(
                    systemPartitioningHandle->function ==
                    protocol::SystemPartitionFunction::SINGLE,
                    "Unsupported partitioning function: {}",
                    toJsonString(systemPartitioningHandle->function));
                // planFragment.planNode =
                //     PartitionedOutputNode::single("root", outputType, sourceNode);
                return planFragment;
            case protocol::SystemPartitioning::FIXED: {
                switch (systemPartitioningHandle->function) {
                case protocol::SystemPartitionFunction::ROUND_ROBIN: {
                    auto numPartitions = partitioningScheme.bucketToPartition->size();

                    if (numPartitions == 1) {
                        // planFragment.planNode =
                        //     PartitionedOutputNode::single("root", outputType,
                        //     sourceNode);
                        return planFragment;
                    }

                    auto partitionFunctionFactory = [](auto numPartitions) {
                        return std::make_unique<velox::exec::RoundRobinPartitionFunction>(
                            numPartitions);
                    };
                    /**
                       planFragment.planNode = std::make_shared<PartitionedOutputNode>(
                       "root",
                       partitioningKeys,
                       numPartitions,
                       false, // broadcast
                       partitioningScheme.replicateNullsAndAny,
                       partitionFunctionFactory,
                       outputType,
                       sourceNode);
                    **/
                    return planFragment;
                }
                case protocol::SystemPartitionFunction::HASH: {
                    auto numPartitions = partitioningScheme.bucketToPartition->size();

                    if (numPartitions == 1) {
                        // planFragment.planNode =
                        //     PartitionedOutputNode::single("root", outputType,
                        //     sourceNode);
                        return planFragment;
                    }
                    /**
                       auto partitionFunctionFactory = [inputType,
                       keyChannels](auto numPartitions) {
                       return std::make_unique<velox::exec::HashPartitionFunction>(
                       numPartitions,
                       inputType,
                       keyChannels.channels,
                       keyChannels.constValues);
                       };

                       planFragment.planNode = std::make_shared<PartitionedOutputNode>(
                       "root",
                       partitioningKeys,
                       numPartitions,
                       false, // broadcast
                       partitioningScheme.replicateNullsAndAny,
                       partitionFunctionFactory,
                       outputType,
                       sourceNode);
                    **/
                    return planFragment;
                }
                case protocol::SystemPartitionFunction::BROADCAST: {
                    // planFragment.planNode = PartitionedOutputNode::broadcast(
                    //     "root", 1, outputType, sourceNode);
                    return planFragment;
                }
                default:
                    VELOX_UNSUPPORTED(
                        "Unsupported partitioning function: {}",
                        toJsonString(systemPartitioningHandle->function));
                }
            }
            default:
                VELOX_FAIL("Unsupported kind of SystemPartitioning");
            }
        }
        LOG(INFO) << "test......";
        VELOX_UNSUPPORTED("Unsupported partitioning handle: {}", "test");
        return planFragment;
    }

} // namespace datalight::trino
