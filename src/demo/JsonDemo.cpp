
#include <cstdio>
#include <iostream>
#include <string>
#include <vector>
#include <fmt/core.h>

#include <nlohmann/json.hpp>
using json = nlohmann::json;

template <typename T>
void to_json_key(json & j, const char * key, const T & value)
{
    j[key] = value;
}

template <typename T>
void to_json_key(json & j, const char * key, const std::shared_ptr<T> & value)
{
    if (value != nullptr)
    {
        j[key] = value;
    }
}


int main(int argc, char * argv[])
{
    fmt::print("The answer is {}.", 42);
    json j1 = {"one", "two", 3, 4.5, false};

    // create a copy
    json j2(j1);

    // serialize the JSON array
    std::cout << j1 << " = " << j2 << '\n';
    std::cout << std::boolalpha << (j1 == j2) << '\n';
    return 0;
}
