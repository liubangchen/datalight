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
/**
    std::vector<TypedExprPtr> toTypedExprs(
        const std::vector<std::shared_ptr<protocol::RowExpression>>& expressions,
        const VeloxExprConverter& exprConverter) {

        std::vector<TypedExprPtr> typedExprs;
        typedExprs.reserve(expressions.size());
        for (auto& expr : expressions) {
            auto typedExpr = exprConverter.toVeloxExpr(expr);
            auto field =
                std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(typedExpr);
            if (field == nullptr) {
                auto constant =
                    std::dynamic_pointer_cast<const core::ConstantTypedExpr>(typedExpr);
                VELOX_CHECK_NOT_NULL(
                    constant,
                    "Unexpected expression type: {}. Expected variable or constant.",
                    expr->_type);
            }
            typedExprs.emplace_back(std::move(typedExpr));
        }

        return typedExprs;
    }
**/
    velox::core::PlanFragment VeloxQueryPlanConverter::toVeloxQueryPlan(
        const protocol::PlanFragment& fragment,
        const protocol::TaskId& taskId) {
        velox::core::PlanFragment planFragment;

        // Convert the fragment info first.
        planFragment.numSplitGroups = 1;

        if (auto output = std::dynamic_pointer_cast<const protocol::OutputNode>(
                fragment.root)) {
            // planFragment.planNode = toVeloxQueryPlan(output, tableWriteInfo, taskId);
            return planFragment;
        }

        auto partitioningScheme = fragment.partitioningScheme;
        auto partitioningHandle =
            partitioningScheme.partitioning.handle.connectorHandle;

        // auto partitioningKeys =
        //    toTypedExprs(partitioningScheme.partitioning.arguments, exprConverter_);

        LOG(INFO) << "test......";
        VELOX_UNSUPPORTED("Unsupported partitioning handle: {}", "test");
        return planFragment;
    }

} // namespace datalight::trino
