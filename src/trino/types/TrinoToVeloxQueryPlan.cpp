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

    std::shared_ptr<const velox::Type> stringToType(const std::string& typeString) {
        return TypeSignatureTypeConverter::parse(typeString);
    }


    std::vector<column_index_t> toChannels(
        const RowTypePtr& type,
        const std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>&
        fields) {
        std::vector<column_index_t> channels;
        channels.reserve(fields.size());
        for (const auto& field : fields) {
            auto channel = type->getChildIdx(field->name());
            channels.emplace_back(channel);
        }
        return channels;
    }

    column_index_t exprToChannel(
        const core::ITypedExpr* expr,
        const TypePtr& type) {
        if (auto field = dynamic_cast<const core::FieldAccessTypedExpr*>(expr)) {
            return type->as<TypeKind::ROW>().getChildIdx(field->name());
        }
        if (dynamic_cast<const core::ConstantTypedExpr*>(expr)) {
            return kConstantChannel;
        }
        VELOX_CHECK(false, "Expression must be field access or constant");
        return 0; // not reached.
    }

// Stores partitioned output channels.
// For each 'kConstantChannel', there is an entry in 'constValues'.
    struct PartitionedOutputChannels {
        std::vector<column_index_t> channels;
        // Each vector holding a single value for a constant channel.
        std::vector<VectorPtr> constValues;
    };

    PartitionedOutputChannels toChannels(
        const RowTypePtr& rowType,
        const std::vector<std::shared_ptr<const core::ITypedExpr>>& exprs,
        memory::MemoryPool* pool) {
        PartitionedOutputChannels output;
        output.channels.reserve(exprs.size());
        for (const auto& expr : exprs) {
            auto channel = exprToChannel(expr.get(), rowType);
            output.channels.push_back(channel);

            // For constant channels create a base vector, add single value to it from
            // our variant and add it to the list of constant expressions.
            if (channel == kConstantChannel) {
                output.constValues.emplace_back(
                    velox::BaseVector::create(expr->type(), 1, pool));
                auto constExpr =
                    std::dynamic_pointer_cast<const core::ConstantTypedExpr>(expr);
//                setCellFromVariant(output.constValues.back(), 0, constExpr->value());
            }
        }
        return output;
    }

    template <TypeKind KIND>
    void setCellFromVariantByKind(
        const VectorPtr& column,
        vector_size_t row,
        const velox::variant& value) {
        using T = typename TypeTraits<KIND>::NativeType;

        auto flatVector = column->as<FlatVector<T>>();
        flatVector->set(row, value.value<T>());
    }

    template <>
    void setCellFromVariantByKind<TypeKind::VARBINARY>(
        const VectorPtr& /*column*/,
        vector_size_t /*row*/,
        const velox::variant& value) {
        throw std::invalid_argument("Return of VARBINARY data is not supported");
    }

    template <>
    void setCellFromVariantByKind<TypeKind::VARCHAR>(
        const VectorPtr& column,
        vector_size_t row,
        const velox::variant& value) {
        auto values = column->as<FlatVector<StringView>>();
        values->set(row, StringView(value.value<Varchar>()));
    }

    void setCellFromVariant(
        const RowVectorPtr& data,
        vector_size_t row,
        vector_size_t column,
        const velox::variant& value) {
        auto columnVector = data->childAt(column);
        if (value.isNull()) {
            columnVector->setNull(row, true);
            return;
        }
        VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
            setCellFromVariantByKind,
            columnVector->typeKind(),
            columnVector,
            row,
            value);
    }

    void setCellFromVariant(
        const VectorPtr& data,
        vector_size_t row,
        const velox::variant& value) {
        if (value.isNull()) {
            data->setNull(row, true);
            return;
        }
        VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
            setCellFromVariantByKind, data->typeKind(), data, row, value);
    }

    void setCellFromConstantVector(
        const RowVectorPtr& data,
        vector_size_t row,
        vector_size_t column,
        VectorPtr valueVector) {
        auto columnVector = data->childAt(column);
        if (valueVector->isNullAt(0)) {
            // This is a constant vector and will have only one element.
            columnVector->setNull(row, true);
            return;
        }
        switch (valueVector->typeKind()) {
        case TypeKind::SHORT_DECIMAL: {
            auto flatVector = columnVector->as<FlatVector<velox::UnscaledShortDecimal>>();
            flatVector->set(
                row,
                valueVector->as<ConstantVector<velox::UnscaledShortDecimal>>()->valueAt(0));
        } break;
        case TypeKind::LONG_DECIMAL: {
            auto flatVector = columnVector->as<FlatVector<velox::UnscaledLongDecimal>>();
            flatVector->set(
                row,
                valueVector->as<ConstantVector<velox::UnscaledLongDecimal>>()->valueAt(0));
        } break;
        default:
            VELOX_UNSUPPORTED();
        }
    }

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

    std::shared_ptr<const RowType> toRowType(
        const protocol::Map<protocol::Symbol, protocol::Type>& symbols,
        const protocol::List<protocol::String>& columns,
        const protocol::List<protocol::Symbol> outputs) {
        std::vector<std::string> names;
        std::vector<velox::TypePtr> types;
        names.reserve(columns.size());
        types.reserve(outputs.size());

        for (const auto& variable : outputs) {
            names.emplace_back(variable);
            auto trino_type = symbols.at(variable);
            //if(!trino_type){
            //    VELOX_USER_FAIL("unknow  symbol type [{}]", variable);
            //}
            types.emplace_back(stringToType(trino_type));
        }

        return ROW(std::move(names), std::move(types));
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
        /**
           auto rowType = toRowType(node->outputVariables);
           if (node->orderingScheme) {
           std::vector<std::shared_ptr<const FieldAccessTypedExpr>> sortingKeys;
           std::vector<SortOrder> sortingOrders;
           sortingKeys.reserve(node->orderingScheme->orderBy.size());
           sortingOrders.reserve(node->orderingScheme->orderBy.size());

           for (const auto& orderBy : node->orderingScheme->orderBy) {
           sortingKeys.emplace_back(exprConverter_.toVeloxExpr(orderBy.variable));
           sortingOrders.emplace_back(toVeloxSortOrder(orderBy.sortOrder));
           }
           return std::make_shared<velox::core::MergeExchangeNode>(
           node->id, rowType, sortingKeys, sortingOrders);
           }
        **/
        //return std::make_shared<velox::core::ExchangeNode>(node->id, rowType);
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
        const protocol::PlanFragment& fragment,
        const std::shared_ptr<const protocol::OutputNode>& node,
        const protocol::TaskId& taskId) {
        return PartitionedOutputNode::single(
            node->id,
            toRowType(fragment.symbols, node->columns, node->outputs),
            toVeloxQueryPlan(node->source, taskId));
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
            planFragment.planNode = toVeloxQueryPlan(fragment, output, taskId);
            LOG(INFO) << "output node....";
            return planFragment;
        }

        auto partitioningScheme = fragment.partitioningScheme;
        auto partitioningHandle =
            partitioningScheme.partitioning.handle.connectorHandle;

        std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> fields;
        auto partitioningKeys = fields;
        //    toTypedExprs(partitioningScheme.partitioning.arguments, exprConverter_);
        auto sourceNode = toVeloxQueryPlan(fragment.root, taskId);
        auto inputType = sourceNode->outputType();

        //PartitionedOutputChannels keyChannels =
        //    toChannels(inputType, partitioningKeys, pool_);
        auto outputType = nullptr;//toRowType(partitioningScheme.outputLayout);

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
                planFragment.planNode =
                          PartitionedOutputNode::single("root", outputType, sourceNode);
                LOG(INFO) << "SINGLE....";
                return planFragment;
            case protocol::SystemPartitioning::FIXED: {
                switch (systemPartitioningHandle->function) {
                case protocol::SystemPartitionFunction::ROUND_ROBIN: {
                    auto numPartitions = partitioningScheme.bucketToPartition->size();
                    LOG(INFO) << "FIXED....";
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
                    LOG(INFO) << "HASH....";
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
                    LOG(INFO) << "BROADCAST....";
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
