#pragma once
#include <functional>
#include <string>
#include <unordered_map>
#include "velox/core/Context.h"

using namespace facebook;

namespace datalight::config
{
    std::unordered_map<std::string, std::string> readConfig(const std::string & filePath);

    std::string requiredProperty(const std::unordered_map<std::string, std::string> & properties, const std::string & name);

    std::string requiredProperty(const velox::Config & properties, const std::string & name);

    std::string getOptionalProperty(
        const std::unordered_map<std::string, std::string> & properties, const std::string & name, const std::function<std::string()> & func);

    std::string getOptionalProperty(
        const std::unordered_map<std::string, std::string> & properties, const std::string & name, const std::string & defaultValue);

    std::string getOptionalProperty(const velox::Config & properties, const std::string & name, const std::string & defaultValue);

}
