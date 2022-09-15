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
#include <stdexcept>

#include "protocol/TrinoProtocol.h"

using namespace facebook;

namespace datalight::trino {

class VeloxExprConverter {
 public:
  explicit VeloxExprConverter(facebook::velox::memory::MemoryPool* pool)
      : pool_(pool) {}

  std::shared_ptr<const velox::core::FieldAccessTypedExpr> toVeloxExpr(
      std::shared_ptr<protocol::Type> pexpr) const;

  velox::variant getConstantValue(
      const velox::TypePtr& type,
      const protocol::Block& block) const;

 private:
  facebook::velox::memory::MemoryPool* pool_;
};

} // namespace datalight::trino
