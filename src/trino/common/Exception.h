#pragma once

#include <unordered_map>
#include "protocol/TrinoProtocol.h"
#include "velox/common/base/VeloxException.h"

using namespace facebook;

namespace std {
    class exception;
}

namespace datalight::protocol {
    struct ExecutionFailureInfo;
    struct ErrorCode;
}

namespace datalight::common{
    class VeloxToTrinoExceptionTranslator {
    public:
        // Translates to Presto error from Velox exceptions
        static protocol::ExecutionFailureInfo translate(
            const velox::VeloxException& e);

        // Translates to Presto error from std::exceptions
        static protocol::ExecutionFailureInfo translate(const std::exception& e);

    private:
        static const std::unordered_map<
            std::string,
            std::unordered_map<std::string, protocol::ErrorCode>>&
            translateMap() {
            static const std::unordered_map<
                std::string,
                std::unordered_map<std::string, protocol::ErrorCode>>
                kTranslateMap = {
                {velox::error_source::kErrorSourceRuntime,
                 {{velox::error_code::kMemCapExceeded,
                   {0x00020000,
                    "GENERIC_INSUFFICIENT_RESOURCES",
                    protocol::ErrorType::INSUFFICIENT_RESOURCES}},
                  {velox::error_code::kInvalidState,
                   {0x00010000,
                    "GENERIC_INTERNAL_ERROR",
                    protocol::ErrorType::INTERNAL_ERROR}},
                  {velox::error_code::kUnreachableCode,
                   {0x00010000,
                    "GENERIC_INTERNAL_ERROR",
                    protocol::ErrorType::INTERNAL_ERROR}},
                  {velox::error_code::kNotImplemented,
                   {0x00010000,
                    "GENERIC_INTERNAL_ERROR",
                    protocol::ErrorType::INTERNAL_ERROR}},
                  {velox::error_code::kUnknown,
                   {0x00010000,
                    "GENERIC_INTERNAL_ERROR",
                    protocol::ErrorType::INTERNAL_ERROR}}}},
                {velox::error_source::kErrorSourceUser,
                 {{velox::error_code::kInvalidArgument,
                   {0x00000000,
                    "GENERIC_USER_ERROR",
                    protocol::ErrorType::USER_ERROR}},
                  {velox::error_code::kUnsupported,
                   {0x0000000D, "NOT_SUPPORTED", protocol::ErrorType::USER_ERROR}},
                  {velox::error_code::kArithmeticError,
                   {0x00000000,
                    "GENERIC_USER_ERROR",
                    protocol::ErrorType::USER_ERROR}}}},
                {velox::error_source::kErrorSourceSystem, {}}};
            return kTranslateMap;
        }
    };
}
