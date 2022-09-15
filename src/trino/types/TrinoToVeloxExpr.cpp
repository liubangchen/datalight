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
#include "types/TrinoToVeloxExpr.h"

#include <boost/algorithm/string/case_conv.hpp>
#include "velox/common/base/Exceptions.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/ConstantVector.h"
#include "velox/vector/FlatVector.h"

#include "types/ParseTypeSignature.h"
#include "protocol//Base64Util.h"

using namespace facebook;
using namespace facebook::velox::core;
using facebook::velox::TypeKind;

namespace datalight::trino {

    namespace {

        template <typename T>
        std::string toJsonString(const T& value) {
            return ((json)value).dump();
        }

        std::optional<std::string> mapDefaultFunctionName(
            const std::string& lowerCaseName) {
            static const char* kPrestoDefaultPrefix = "presto.default.";
            static const uint32_t kPrestoDefaultPrefixLength =
                strlen(kPrestoDefaultPrefix);

            if (lowerCaseName.compare(
                    0, kPrestoDefaultPrefixLength, kPrestoDefaultPrefix) == 0) {
                return lowerCaseName.substr(kPrestoDefaultPrefixLength);
            }

            return std::nullopt;
        }

        std::string mapScalarFunction(const std::string& name) {
            static const std::unordered_map<std::string, std::string> kFunctionNames = {
                // see com.facebook.presto.common.function.OperatorType
                {"presto.default.$operator$add", "plus"},
                {"presto.default.$operator$between", "between"},
                {"presto.default.$operator$divide", "divide"},
                {"presto.default.$operator$equal", "eq"},
                {"presto.default.$operator$greater_than", "gt"},
                {"presto.default.$operator$greater_than_or_equal", "gte"},
                {"presto.default.$operator$is_distinct_from", "distinct_from"},
                {"presto.default.$operator$less_than", "lt"},
                {"presto.default.$operator$less_than_or_equal", "lte"},
                {"presto.default.$operator$modulus", "mod"},
                {"presto.default.$operator$multiply", "multiply"},
                {"presto.default.$operator$negation", "negate"},
                {"presto.default.$operator$not_equal", "neq"},
                {"presto.default.$operator$subtract", "minus"},
                {"presto.default.$operator$subscript", "subscript"},
                {"presto.default.random", "rand"}};

            std::string lowerCaseName = boost::to_lower_copy(name);

            auto it = kFunctionNames.find(lowerCaseName);
            if (it != kFunctionNames.end()) {
                return it->second;
            }

            auto mappedName = mapDefaultFunctionName(lowerCaseName);
            if (mappedName.has_value()) {
                return mappedName.value();
            }

            return lowerCaseName;
        }

        std::string mapAggregateFunctionName(const std::string& name) {
            std::string lowerCaseName = boost::to_lower_copy(name);

            auto mappedName = mapDefaultFunctionName(lowerCaseName);
            if (mappedName.has_value()) {
                return mappedName.value();
            }

            return lowerCaseName;
        }

    } // namespace

    velox::variant VeloxExprConverter::getConstantValue(
        const velox::TypePtr& type,
        const protocol::Block& block) const {
        auto valueVector = protocol::readBlock(type, block.data, pool_);

        auto typeKind = type->kind();
        if (valueVector->isNullAt(0)) {
            return velox::variant(typeKind);
        }

        switch (typeKind) {
        case TypeKind::BIGINT:
            return valueVector->as<velox::SimpleVector<int64_t>>()->valueAt(0);
        case TypeKind::INTEGER:
            return valueVector->as<velox::SimpleVector<int32_t>>()->valueAt(0);
        case TypeKind::SMALLINT:
            return valueVector->as<velox::SimpleVector<int16_t>>()->valueAt(0);
        case TypeKind::TINYINT:
            return valueVector->as<velox::SimpleVector<int8_t>>()->valueAt(0);
        case TypeKind::TIMESTAMP:
            return valueVector->as<velox::SimpleVector<velox::Timestamp>>()->valueAt(
                0);
        case TypeKind::DATE:
            return valueVector->as<velox::SimpleVector<velox::Date>>()->valueAt(0);
        case TypeKind::INTERVAL_DAY_TIME:
            return valueVector->as<velox::SimpleVector<velox::IntervalDayTime>>()
                ->valueAt(0);
        case TypeKind::BOOLEAN:
            return valueVector->as<velox::SimpleVector<bool>>()->valueAt(0);
        case TypeKind::DOUBLE:
            return valueVector->as<velox::SimpleVector<double>>()->valueAt(0);
        case TypeKind::REAL:
            return valueVector->as<velox::SimpleVector<float>>()->valueAt(0);
        case TypeKind::VARCHAR:
            return velox::variant(
                valueVector->as<velox::SimpleVector<velox::StringView>>()->valueAt(
                    0));
        default:
            throw std::invalid_argument(
                "Unexpected Block type: " + mapTypeKindToName(typeKind));
        }
    }

    std::shared_ptr<const velox::core::FieldAccessTypedExpr> VeloxExprConverter::toVeloxExpr(
        std::shared_ptr<protocol::Type> pexpr) const{

        return nullptr;
    }
}
