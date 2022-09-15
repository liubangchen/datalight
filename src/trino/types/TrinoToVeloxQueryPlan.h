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

#include <velox/core/Expressions.h>
#include <velox/core/PlanFragment.h>
#include <velox/core/PlanNode.h>
#include <velox/type/Variant.h>
#include <stdexcept>
#include <vector>

#include "protocol/TrinoProtocol.h"
#include "types/TrinoTaskId.h"
#include "types/TrinoToVeloxExpr.h"

namespace datalight::trino {
using namespace facebook;

class VeloxQueryPlanConverter {
 public:
  explicit VeloxQueryPlanConverter(velox::memory::MemoryPool* pool)
      : pool_(pool), exprConverter_(pool) {}

  velox::core::PlanFragment toVeloxQueryPlan(
      const protocol::PlanFragment& fragment,
      const protocol::TaskId& taskId);

  std::shared_ptr<const velox::core::PlanNode> toVeloxQueryPlan(
      const protocol::PlanFragment& fragment,
      const std::shared_ptr<const protocol::PlanNode>& node,
      const protocol::TaskId& taskId);

 private:
  std::shared_ptr<const velox::core::PlanNode> toVeloxQueryPlan(
      const protocol::PlanFragment& fragment,
      const std::shared_ptr<const protocol::ExchangeNode>& node,
      const protocol::TaskId& taskId);

  std::shared_ptr<const velox::core::ExchangeNode> toVeloxQueryPlan(
      const protocol::PlanFragment& fragment,
      const std::shared_ptr<const protocol::RemoteSourceNode>& node,
      const protocol::TaskId& taskId);

  std::shared_ptr<const velox::core::PlanNode> toVeloxQueryPlan(
      const protocol::PlanFragment& fragment,
      const std::shared_ptr<const protocol::FilterNode>& node,
      const protocol::TaskId& taskId);

  std::shared_ptr<const velox::core::PlanNode> toVeloxQueryPlan(
      const protocol::PlanFragment& fragment,
      const std::shared_ptr<const protocol::OutputNode>& node,
      const protocol::TaskId& taskId);

  std::shared_ptr<const velox::core::ProjectNode> toVeloxQueryPlan(
      const protocol::PlanFragment& fragment,
      const std::shared_ptr<const protocol::ProjectNode>& node,
      const protocol::TaskId& taskId);

  std::shared_ptr<const velox::core::ValuesNode> toVeloxQueryPlan(
      const protocol::PlanFragment& fragment,
      const std::shared_ptr<const protocol::ValuesNode>& node,
      const protocol::TaskId& taskId);

  std::shared_ptr<const velox::core::TableScanNode> toVeloxQueryPlan(
      const protocol::PlanFragment& fragment,
      const std::shared_ptr<const protocol::TableScanNode>& node,
      const protocol::TaskId& taskId);

  std::shared_ptr<const velox::core::AggregationNode> toVeloxQueryPlan(
      const protocol::PlanFragment& fragment,
      const std::shared_ptr<const protocol::AggregationNode>& node,
      const protocol::TaskId& taskId);

  std::shared_ptr<const velox::core::GroupIdNode> toVeloxQueryPlan(
      const protocol::PlanFragment& fragment,
      const std::shared_ptr<const protocol::GroupIdNode>& node,
      const protocol::TaskId& taskId);

  std::shared_ptr<const velox::core::PlanNode> toVeloxQueryPlan(
      const protocol::PlanFragment& fragment,
      const std::shared_ptr<const protocol::DistinctLimitNode>& node,
      const protocol::TaskId& taskId);

  std::shared_ptr<const velox::core::PlanNode> toVeloxQueryPlan(
      const protocol::PlanFragment& fragment,
      const std::shared_ptr<const protocol::JoinNode>& node,
      const protocol::TaskId& taskId);
  /**
      std::shared_ptr<const velox::core::PlanNode> toVeloxQueryPlan(
          const std::shared_ptr<const protocol::MergeJoinNode>& node,
          const protocol::TaskId& taskId);
  **/
  std::shared_ptr<const velox::core::TopNNode> toVeloxQueryPlan(
      const protocol::PlanFragment& fragment,
      const std::shared_ptr<const protocol::TopNNode>& node,
      const protocol::TaskId& taskId);

  std::shared_ptr<const velox::core::LimitNode> toVeloxQueryPlan(
      const protocol::PlanFragment& fragment,
      const std::shared_ptr<const protocol::LimitNode>& node,
      const protocol::TaskId& taskId);

  std::shared_ptr<const velox::core::OrderByNode> toVeloxQueryPlan(
      const protocol::PlanFragment& fragment,
      const std::shared_ptr<const protocol::SortNode>& node,
      const protocol::TaskId& taskId);

  std::shared_ptr<const velox::core::TableWriteNode> toVeloxQueryPlan(
      const protocol::PlanFragment& fragment,
      const std::shared_ptr<const protocol::TableWriterNode>& node,
      const protocol::TaskId& taskId);
  /**
      std::shared_ptr<const velox::core::UnnestNode> toVeloxQueryPlan(
          const std::shared_ptr<const protocol::UnnestNode>& node,
          const protocol::TaskId& taskId);

      std::shared_ptr<const velox::core::EnforceSingleRowNode> toVeloxQueryPlan(
          const std::shared_ptr<const protocol::EnforceSingleRowNode>& node,
          const protocol::TaskId& taskId);

      std::shared_ptr<const velox::core::AssignUniqueIdNode> toVeloxQueryPlan(
          const std::shared_ptr<const protocol::AssignUniqueId>& node,
          const protocol::TaskId& taskId);

      std::vector<std::shared_ptr<const FieldAccessTypedExpr>> toVeloxExprs(
          const std::vector<protocol::VariableReferenceExpression>& variables);
  **/
  std::shared_ptr<const velox::core::ProjectNode> tryConvertOffsetLimit(
      const protocol::PlanFragment& fragment,
      const std::shared_ptr<const protocol::ProjectNode>& node,
      const protocol::TaskId& taskId);

  velox::memory::MemoryPool* pool_;
  VeloxExprConverter exprConverter_;
};

} // namespace datalight::trino
