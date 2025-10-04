#include "scheduler/scheduler.h"

#include <iostream>
#include <sstream>

#include <nlohmann/json.hpp>

int main() {
    std::ostringstream buffer;
    buffer << std::cin.rdbuf();
    const auto input_text = buffer.str();

    std::cerr << "Scheduler binary called" << std::endl;
    std::cerr << input_text << std::endl;

    nlohmann::json input_json;
    if (!input_text.empty()) {
        std::cerr << "Parsing input JSON" << std::endl;
        input_json = nlohmann::json::parse(input_text);
    } else {
        input_json = nlohmann::json::object();
    }

    auto result = scheduler::RunScheduler(input_json);
    std::cout << result.dump();
    return 0;
}

