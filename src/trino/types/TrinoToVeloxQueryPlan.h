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

private:
    velox::memory::MemoryPool* pool_;
    VeloxExprConverter exprConverter_;
};

} // namespace datalight::trino
