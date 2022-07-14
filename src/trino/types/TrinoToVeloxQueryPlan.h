#pragma once

#include <stdexcept>
#include <vector>
#include "protocol/TrinoProtocol.h"
#include "velox/core/Expressions.h"
#include "velox/core/PlanFragment.h"
#include "velox/core/PlanNode.h"
#include "velox/type/Variant.h"

using namespace facebook;
namespace datalight::types
{
    class VeloxQueryPlanConverter {
    public:
        explicit VeloxQueryPlanConverter(velox::memory::MemoryPool* pool)
            : pool_(pool), exprConverter_(pool) {}

        velox::core::PlanFragment toVeloxQueryPlan(
            const protocol::PlanFragment& fragment,
            const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
            const protocol::TaskId& taskId);

        // visible for testing
        std::shared_ptr<const velox::core::PlanNode> toVeloxQueryPlan(
            const std::shared_ptr<const protocol::PlanNode>& node,
            const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
            const protocol::TaskId& taskId);

    private:
        std::shared_ptr<const velox::core::PlanNode> toVeloxQueryPlan(
            const std::shared_ptr<const protocol::ExchangeNode>& node,
            const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
            const protocol::TaskId& taskId);

        std::shared_ptr<const velox::core::ExchangeNode> toVeloxQueryPlan(
            const std::shared_ptr<const protocol::RemoteSourceNode>& node,
            const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
            const protocol::TaskId& taskId);

        std::shared_ptr<const velox::core::PlanNode> toVeloxQueryPlan(
            const std::shared_ptr<const protocol::FilterNode>& node,
            const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
            const protocol::TaskId& taskId);

        std::shared_ptr<const velox::core::PlanNode> toVeloxQueryPlan(
            const std::shared_ptr<const protocol::OutputNode>& node,
            const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
            const protocol::TaskId& taskId);

        std::shared_ptr<const velox::core::ProjectNode> toVeloxQueryPlan(
            const std::shared_ptr<const protocol::ProjectNode>& node,
            const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
            const protocol::TaskId& taskId);

        std::shared_ptr<const velox::core::ValuesNode> toVeloxQueryPlan(
            const std::shared_ptr<const protocol::ValuesNode>& node,
            const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
            const protocol::TaskId& taskId);

        std::shared_ptr<const velox::core::TableScanNode> toVeloxQueryPlan(
            const std::shared_ptr<const protocol::TableScanNode>& node,
            const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
            const protocol::TaskId& taskId);

        std::shared_ptr<const velox::core::AggregationNode> toVeloxQueryPlan(
            const std::shared_ptr<const protocol::AggregationNode>& node,
            const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
            const protocol::TaskId& taskId);

        std::shared_ptr<const velox::core::GroupIdNode> toVeloxQueryPlan(
            const std::shared_ptr<const protocol::GroupIdNode>& node,
            const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
            const protocol::TaskId& taskId);

        std::shared_ptr<const velox::core::PlanNode> toVeloxQueryPlan(
            const std::shared_ptr<const protocol::DistinctLimitNode>& node,
            const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
            const protocol::TaskId& taskId);

        std::shared_ptr<const velox::core::PlanNode> toVeloxQueryPlan(
            const std::shared_ptr<const protocol::JoinNode>& node,
            const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
            const protocol::TaskId& taskId);

        std::shared_ptr<const velox::core::PlanNode> toVeloxQueryPlan(
            const std::shared_ptr<const protocol::MergeJoinNode>& node,
            const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
            const protocol::TaskId& taskId);

        std::shared_ptr<const velox::core::TopNNode> toVeloxQueryPlan(
            const std::shared_ptr<const protocol::TopNNode>& node,
            const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
            const protocol::TaskId& taskId);

        std::shared_ptr<const velox::core::LimitNode> toVeloxQueryPlan(
            const std::shared_ptr<const protocol::LimitNode>& node,
            const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
            const protocol::TaskId& taskId);

        std::shared_ptr<const velox::core::OrderByNode> toVeloxQueryPlan(
            const std::shared_ptr<const protocol::SortNode>& node,
            const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
            const protocol::TaskId& taskId);

        std::shared_ptr<const velox::core::TableWriteNode> toVeloxQueryPlan(
            const std::shared_ptr<const protocol::TableWriterNode>& node,
            const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
            const protocol::TaskId& taskId);

        std::shared_ptr<const velox::core::UnnestNode> toVeloxQueryPlan(
            const std::shared_ptr<const protocol::UnnestNode>& node,
            const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
            const protocol::TaskId& taskId);

        std::shared_ptr<const velox::core::EnforceSingleRowNode> toVeloxQueryPlan(
            const std::shared_ptr<const protocol::EnforceSingleRowNode>& node,
            const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
            const protocol::TaskId& taskId);

        std::shared_ptr<const velox::core::AssignUniqueIdNode> toVeloxQueryPlan(
            const std::shared_ptr<const protocol::AssignUniqueId>& node,
            const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
            const protocol::TaskId& taskId);

        std::vector<std::shared_ptr<const FieldAccessTypedExpr>> toVeloxExprs(
            const std::vector<protocol::VariableReferenceExpression>& variables);

        std::shared_ptr<const velox::core::ProjectNode> tryConvertOffsetLimit(
            const std::shared_ptr<const protocol::ProjectNode>& node,
            const std::shared_ptr<protocol::TableWriteInfo>& tableWriteInfo,
            const protocol::TaskId& taskId);

        velox::memory::MemoryPool* pool_;
        VeloxExprConverter exprConverter_;
    };

}
