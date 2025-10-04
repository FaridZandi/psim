#pragma once

#include <nlohmann/json.hpp>

namespace scheduler {

nlohmann::json RunScheduler(const nlohmann::json &input);

}

