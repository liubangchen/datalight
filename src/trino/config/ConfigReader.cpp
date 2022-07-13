#include "ConfigReader.h"
#include <fstream>

namespace datalight::config {

    std::unordered_map<std::string, std::string> readConfig(
        const std::string& filePath) {
        std::ifstream configFile(filePath);
        if (!configFile.is_open()) {
            throw std::runtime_error(
                fmt::format("Couldn't open config file {} for reading.", filePath));
        }

        std::unordered_map<std::string, std::string> properties;
        std::string line;
        while (getline(configFile, line)) {
            line.erase(std::remove_if(line.begin(), line.end(), isspace), line.end());
            if (line[0] == '#' || line.empty()) {
                continue;
            }
            auto delimiterPos = line.find('=');
            auto name = line.substr(0, delimiterPos);
            auto value = line.substr(delimiterPos + 1);
            properties.emplace(name, value);
        }

        return properties;
    }

    std::string requiredProperty(
        const std::unordered_map<std::string, std::string>& properties,
        const std::string& name) {
        auto it = properties.find(name);
        if (it == properties.end()) {
            throw std::runtime_error(
                std::string("Missing configuration property ") + name);
        }
        return it->second;
    }

    std::string requiredProperty(
        const velox::Config& properties,
        const std::string& name) {
        auto value = properties.get(name);
        if (!value.hasValue()) {
            throw std::runtime_error(
                std::string("Missing configuration property ") + name);
        }
        return value.value();
    }

    std::string getOptionalProperty(
        const std::unordered_map<std::string, std::string>& properties,
        const std::string& name,
        const std::function<std::string()>& func) {
        auto it = properties.find(name);
        if (it == properties.end()) {
            return func();
        }
        return it->second;
    }

    std::string getOptionalProperty(
        const std::unordered_map<std::string, std::string>& properties,
        const std::string& name,
        const std::string& defaultValue) {
        auto it = properties.find(name);
        if (it == properties.end()) {
            return defaultValue;
        }
        return it->second;
    }

    std::string getOptionalProperty(
        const velox::Config& properties,
        const std::string& name,
        const std::string& defaultValue) {
        auto value = properties.get(name);
        if (!value.hasValue()) {
            return defaultValue;
        }
        return value.value();
    }

}
