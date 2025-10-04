#include "scheduler/scheduler.h"

#include <algorithm>
#include <cmath>
#include <fstream>
#include <iostream>
#include <functional>
#include <iomanip>
#include <limits>
#include <memory>
#include <numeric>
#include <optional>
#include <queue>
#include <random>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include <boost/uuid/detail/md5.hpp>

namespace scheduler {
namespace {

using json = nlohmann::json;

enum class Direction {
    kUp,
    kDown,
};

struct LinkId {
    int rack = 0;
    Direction direction = Direction::kUp;

    bool operator==(const LinkId &other) const noexcept {
        return rack == other.rack && direction == other.direction;
    }
};

struct LinkIdHash {
    std::size_t operator()(const LinkId &id) const noexcept {
        return static_cast<std::size_t>(id.rack * 2 + (id.direction == Direction::kUp ? 1 : 0));
    }
};

struct FlowProfile {
    int flow_id = -1;
    int job_id = -1;
    int iteration = 0;
    int subflow = 0;
    int start_time = 0;
    int end_time = 0;
    int src_rack = 0;
    int dst_rack = 0;
    std::string dir;
    double fct = 0.0;
    int core = 0;
    std::string label;
    std::vector<double> progress_history;
    double flow_size = 0.0;
    std::vector<double> progress_history_summarized;
};

struct JobProfile {
    int period = 0;
    std::vector<FlowProfile> flows;
};

struct LinkProfile {
    std::vector<double> load;
    int period = 0;
    double max_load = 0.0;
};

class Job;
class Solution;
class LinkJobLoad;

using Throttle = double;

class Job {
  public:
    Job() = default;
    Job(int job_id,
        std::unordered_map<Throttle, JobProfile> profiles,
        bool has_profiles,
        int iter_count,
        std::unordered_map<Throttle, int> periods,
        int base_period)
        : job_id_(job_id),
          profiles_(std::move(profiles)),
          has_profiles_(has_profiles),
          iter_count_(iter_count),
          periods_(std::move(periods)),
          base_period_(base_period) {}

    int job_id() const { return job_id_; }
    int iter_count() const { return iter_count_; }
    int base_period() const { return base_period_; }

    const std::unordered_map<Throttle, int> &periods() const { return periods_; }
    const std::unordered_map<Throttle, JobProfile> &profiles() const { return profiles_; }
    bool has_profiles() const { return has_profiles_; }

    void AddLinkLoad(const LinkId &id, std::shared_ptr<LinkJobLoad> load);

    const std::unordered_map<LinkId, std::shared_ptr<LinkJobLoad>, LinkIdHash> &link_loads() const {
        return link_loads_;
    }

    const std::vector<double> &GetBaseSignal(Throttle throttle_rate) const;
    std::pair<int, int> GetActiveRange(Throttle throttle_rate, double inflate = 1.0) const;

  private:
    int job_id_ = 0;
    std::unordered_map<Throttle, JobProfile> profiles_;
    bool has_profiles_ = false;
    int iter_count_ = 0;
    std::unordered_map<Throttle, int> periods_;
    int base_period_ = 0;
    std::unordered_map<LinkId, std::shared_ptr<LinkJobLoad>, LinkIdHash> link_loads_;
    mutable std::unordered_map<Throttle, std::vector<double>> base_signal_cache_;
};

class Solution {
  public:
    explicit Solution(const std::unordered_map<int, Job> &job_map);

    const std::unordered_map<int, std::vector<int>> &deltas() const { return deltas_; }
    const std::unordered_map<int, std::vector<double>> &throttle_rates() const { return throttle_rates_; }

    std::unordered_map<int, std::vector<int>> &deltas() { return deltas_; }
    std::unordered_map<int, std::vector<double>> &throttle_rates() { return throttle_rates_; }

    double GetJobCost(int job_id) const;
    double GetAverageJobCost() const;
    std::vector<json> GetJobTimings() const;

    int GetJobIterStartTime(int job_id, int iter) const;
    std::pair<int, int> GetJobIterActiveTime(int job_id,
                                             int iter,
                                             double iter_throttle_rate = 1.0,
                                             double inflate = 1.0) const;

  private:
    const std::unordered_map<int, Job> &job_map_;
    std::unordered_map<int, std::vector<int>> deltas_;
    std::unordered_map<int, std::vector<double>> throttle_rates_;
};

Solution::Solution(const std::unordered_map<int, Job> &job_map) : job_map_(job_map) {
    for (const auto &[job_id, job] : job_map_) {
        deltas_[job_id] = std::vector<int>(job.iter_count(), 0);
        throttle_rates_[job_id] = std::vector<double>(job.iter_count(), 1.0);
    }
}

double Solution::GetJobCost(int job_id) const {
    const auto job_it = job_map_.find(job_id);
    if (job_it == job_map_.end()) {
        return 0.0;
    }

    const auto &job = job_it->second;
    const auto &job_deltas = deltas_.at(job_id);
    const auto &job_throttles = throttle_rates_.at(job_id);

    double cost = 0.0;
    for (std::size_t i = 0; i < job_deltas.size(); ++i) {
        const double throttle = job_throttles[i];
        auto period_it = job.periods().find(throttle);
        int period = (period_it != job.periods().end()) ? period_it->second : job.base_period();
        double throttle_cost = static_cast<double>(period - job.base_period());
        cost += static_cast<double>(job_deltas[i]) + throttle_cost;
    }
    return cost;
}

double Solution::GetAverageJobCost() const {
    if (job_map_.empty()) {
        return 0.0;
    }
    double total = 0.0;
    for (const auto &[job_id, _] : job_map_) {
        total += GetJobCost(job_id);
    }
    return total / static_cast<double>(job_map_.size());
}

std::vector<json> Solution::GetJobTimings() const {
    std::vector<json> result;
    result.reserve(job_map_.size());
    for (const auto &[job_id, _] : job_map_) {
        json entry;
        entry["job_id"] = job_id;
        entry["deltas"] = deltas_.at(job_id);
        entry["throttle_rates"] = throttle_rates_.at(job_id);
        result.push_back(entry);
    }
    return result;
}

int Solution::GetJobIterStartTime(int job_id, int iter) const {
    const auto job_it = job_map_.find(job_id);
    if (job_it == job_map_.end()) {
        return 0;
    }
    const auto &job = job_it->second;
    const auto &job_deltas = deltas_.at(job_id);
    const auto &job_throttles = throttle_rates_.at(job_id);

    int start_time = 0;
    for (int i = 0; i < iter; ++i) {
        const int delta = job_deltas[i];
        const double throttle_rate = job_throttles[i];
        auto period_it = job.periods().find(throttle_rate);
        int period = (period_it != job.periods().end()) ? period_it->second : job.base_period();
        start_time += delta + period;
    }
    start_time += job_deltas[iter];
    return start_time;
}

std::pair<int, int> Solution::GetJobIterActiveTime(int job_id,
                                                   int iter,
                                                   double iter_throttle_rate,
                                                   double inflate) const {
    const auto job_it = job_map_.find(job_id);
    if (job_it == job_map_.end()) {
        return {0, 0};
    }
    const auto &job = job_it->second;
    const int iter_start_time = GetJobIterStartTime(job_id, iter);
    auto [active_start, active_end] = job.GetActiveRange(iter_throttle_rate, inflate);
    return {iter_start_time + active_start, iter_start_time + active_end};
}

class LinkJobLoad {
  public:
    LinkJobLoad(const Job *job, std::unordered_map<Throttle, LinkProfile> profiles)
        : job_(job), profiles_(std::move(profiles)) {}

    const Job *job() const { return job_; }
    const std::unordered_map<Throttle, LinkProfile> &profiles() const { return profiles_; }

    std::vector<double> GetSignal(const Solution &solution,
                                  int start_time = 0,
                                  std::optional<int> start_iter = std::nullopt,
                                  std::optional<int> end_iter = std::nullopt) const;

    const std::vector<double> &GetBaseSignal(Throttle throttle_rate) const;

  private:
    const Job *job_ = nullptr;
    std::unordered_map<Throttle, LinkProfile> profiles_;
};

std::vector<double> LinkJobLoad::GetSignal(const Solution &solution,
                                           int start_time,
                                           std::optional<int> start_iter,
                                           std::optional<int> end_iter) const {
    if (!job_) {
        return {};
    }

    const int job_id = job_->job_id();
    const auto &deltas = solution.deltas().at(job_id);
    const auto &throttle_rates = solution.throttle_rates().at(job_id);

    const int begin_iter = start_iter.value_or(0);
    const int final_iter = end_iter.value_or(static_cast<int>(deltas.size()));

    if (begin_iter >= final_iter) {
        return std::vector<double>(start_time, 0.0);
    }

    std::vector<double> signal(start_time, 0.0);

    for (int iter = begin_iter; iter < final_iter; ++iter) {
        const int iter_delta = deltas[iter];
        signal.insert(signal.end(), iter_delta, 0.0);

        const double iter_throttle_rate = throttle_rates[iter];
        auto profile_it = profiles_.find(iter_throttle_rate);
        if (profile_it == profiles_.end()) {
            continue;
        }
        const auto &iter_signal = profile_it->second.load;
        signal.insert(signal.end(), iter_signal.begin(), iter_signal.end());
    }

    return signal;
}

const std::vector<double> &LinkJobLoad::GetBaseSignal(Throttle throttle_rate) const {
    static const std::vector<double> kEmpty;
    auto it = profiles_.find(throttle_rate);
    if (it == profiles_.end()) {
        return kEmpty;
    }
    return it->second.load;
}

class LinkLevelProblem {
  public:
    LinkLevelProblem(LinkId link_id, int max_length, std::string score_mode)
        : link_id_(link_id), max_length_(max_length), score_mode_(std::move(score_mode)) {}

    void AddJobLoad(std::shared_ptr<LinkJobLoad> job_load) { job_loads_.push_back(std::move(job_load)); }

    const LinkId &id() const { return link_id_; }
    const std::vector<std::shared_ptr<LinkJobLoad>> &job_loads() const { return job_loads_; }

    std::vector<double> GetTotalLoad(const Solution &solution) const;
    double GetCompatScore(const Solution &solution, double capacity) const;

  private:
    LinkId link_id_;
    int max_length_ = 0;
    std::string score_mode_;
    std::vector<std::shared_ptr<LinkJobLoad>> job_loads_;
};

std::vector<double> LinkLevelProblem::GetTotalLoad(const Solution &solution) const {
    if (job_loads_.empty()) {
        return std::vector<double>(max_length_, 0.0);
    }

    std::vector<std::vector<double>> signals;
    signals.reserve(job_loads_.size());
    std::size_t max_signal_length = 0;
    for (const auto &load : job_loads_) {
        auto signal = load->GetSignal(solution);
        max_signal_length = std::max<std::size_t>(max_signal_length, signal.size());
        signals.push_back(std::move(signal));
    }

    std::vector<double> sum(max_signal_length, 0.0);
    for (const auto &signal : signals) {
        for (std::size_t i = 0; i < signal.size(); ++i) {
            sum[i] += signal[i];
        }
    }
    return sum;
}

double LinkLevelProblem::GetCompatScore(const Solution &solution, double capacity) const {
    const auto sum_signal = GetTotalLoad(solution);
    const double max_util = sum_signal.empty() ? 0.0 :
        *std::max_element(sum_signal.begin(), sum_signal.end());

    if (score_mode_ == "under-cap") {
        if (sum_signal.empty()) {
            return 1.0;
        }
        int under_count = 0;
        for (double value : sum_signal) {
            if (value <= capacity) {
                ++under_count;
            }
        }
        return static_cast<double>(under_count) / static_cast<double>(sum_signal.size());
    }

    if (score_mode_ == "time-no-coll") {
        double compat_score = 0.0;
        if (max_util <= capacity) {
            compat_score = 1.0;
        } else {
            auto it = std::find_if(sum_signal.begin(), sum_signal.end(), [capacity](double v) {
                return v > capacity;
            });
            if (it == sum_signal.end()) {
                compat_score = 1.0;
            } else {
                compat_score = static_cast<double>(std::distance(sum_signal.begin(), it)) /
                               static_cast<double>(max_length_);
            }
        }

        std::vector<double> job_costs;
        job_costs.reserve(job_loads_.size());
        for (const auto &job_load : job_loads_) {
            if (job_load->job()) {
                job_costs.push_back(solution.GetJobCost(job_load->job()->job_id()));
            }
        }
        double solution_cost = 0.0;
        if (!job_costs.empty()) {
            solution_cost = std::accumulate(job_costs.begin(), job_costs.end(), 0.0) /
                            static_cast<double>(job_costs.size());
        }
        return compat_score - solution_cost;
    }

    if (score_mode_ == "max-util-left") {
        if (capacity == 0.0) {
            return 0.0;
        }
        return (capacity - max_util) / capacity;
    }

    return 0.0;
}

struct LinkLoadEntry {
    LinkId link_id;
    int job_id = 0;
    int iter_count = 0;
    std::unordered_map<Throttle, LinkProfile> profiles;
};

struct LinkLoadsResult {
    std::unordered_map<LinkId, std::vector<LinkLoadEntry>, LinkIdHash> link_loads;
    std::vector<int> cross_rack_jobs;
};

struct FlowInstance {
    int job_id = 0;
    int flow_id = 0;
    int iteration = 0;
    int src_rack = 0;
    int dst_rack = 0;
    int start_time = 0;
    int end_time = 0;
    int eff_start_time = 0;
    int eff_end_time = 0;
    int progress_shift = 0;
    double throttle_rate = 1.0;
    double max_load = 0.0;
    std::vector<double> progress_history;
    std::string dir;
    std::string traffic_id;
    std::string traffic_member_id;
    std::string traffic_pattern_hash;
    int needed_subflows = 1;
};

struct DecisionKey {
    int job_id = 0;
    int flow_id = 0;
    int iteration = 0;

    bool operator==(const DecisionKey &other) const noexcept {
        return job_id == other.job_id && flow_id == other.flow_id && iteration == other.iteration;
    }
};

struct DecisionKeyHash {
    std::size_t operator()(const DecisionKey &key) const noexcept {
        std::size_t h1 = std::hash<int>{}(key.job_id);
        std::size_t h2 = std::hash<int>{}(key.flow_id);
        std::size_t h3 = std::hash<int>{}(key.iteration);
        return ((h1 * 1315423911u + h2) << 1) ^ h3;
    }
};

struct PairHash {
    std::size_t operator()(const std::pair<std::string, std::string> &value) const noexcept {
        return std::hash<std::string>{}(value.first) ^ (std::hash<std::string>{}(value.second) << 1);
    }
};

struct DirectionalLoad {
    std::vector<double> up;
    std::vector<double> down;
};

using SpineLoads = std::vector<DirectionalLoad>;   // size = num_spines
using RemMatrix = std::vector<SpineLoads>;         // size = num_leaves

using JobUsageMatrix = std::vector<std::vector<DirectionalLoad>>;  // [leaf][spine]
using UsageMap = std::unordered_map<int, JobUsageMatrix>;

struct ColoringOutcome {
    int min_affected_time = std::numeric_limits<int>::max();
    int max_affected_time = 0;
    std::vector<std::pair<int, int>> bad_ranges;
};

struct ColoringSolution {
    std::pair<int, int> time_range{0, 0};
    std::unordered_set<std::string> patterns;
    std::unordered_map<std::string, std::vector<int>> coloring;
};

using MergedRangeMap = std::map<std::vector<std::string>, std::vector<std::pair<int, int>>>;

void AddSignalToSum(const std::vector<double> &signal, std::vector<double> &sum_signal);
std::string Md5Hex(const std::string &input);
std::string Md5Hex16(const std::string &input);
std::pair<std::unordered_map<int, int>, int> ColorBipartiteMultigraph(
    const std::vector<std::tuple<std::string, std::string, int>> &input_edges);
ColoringSolution *FindColoringForPattern(std::vector<ColoringSolution> &solutions,
                                         int value,
                                         const std::string &pattern_hash);
MergedRangeMap MergeOverlappingRangesV7(
    const std::unordered_map<std::string, std::vector<std::pair<int, int>>> &ranges_dict,
    const std::unordered_map<std::string, std::unordered_set<int>> &traffic_pattern_to_src_racks,
    const std::unordered_map<std::string, std::unordered_set<int>> &traffic_pattern_to_dst_racks);

LinkLoadsResult GetLinkLoads(const std::unordered_map<int, Job> &job_map,
                             int rack_count,
                             double link_bandwidth,
                             const std::vector<double> &throttle_factors) {
    LinkLoadsResult result;
    std::unordered_set<int> cross_rack_jobs_set;

    for (int rack = 0; rack < rack_count; ++rack) {
        for (Direction dir : {Direction::kUp, Direction::kDown}) {
            LinkId link_id{rack, dir};
            auto &link_entries = result.link_loads[link_id];

            for (const auto &[job_id, job] : job_map) {
                if (!job.has_profiles()) {
                    continue;
                }

                std::unordered_map<Throttle, LinkProfile> throttled_profiles;
                bool link_has_flow = false;

                for (double throttle : throttle_factors) {
                    auto profile_it = job.profiles().find(throttle);
                    if (profile_it == job.profiles().end()) {
                        continue;
                    }
                    const auto &job_profile = profile_it->second;
                    if (job_profile.flows.empty()) {
                        continue;
                    }

                    std::vector<double> link_load_combined;
                    bool throttle_has_flow = false;

                    for (const auto &flow : job_profile.flows) {
                        const bool matches =
                            (dir == Direction::kUp && flow.src_rack == rack) ||
                            (dir == Direction::kDown && flow.dst_rack == rack);
                        if (!matches) {
                            continue;
                        }
                        throttle_has_flow = true;
                        std::vector<double> normalized(flow.progress_history.size(), 0.0);
                        for (std::size_t t = 0; t < flow.progress_history.size(); ++t) {
                            normalized[t] = flow.progress_history[t] / link_bandwidth;
                        }
                        AddSignalToSum(normalized, link_load_combined);
                    }

                    if (throttle_has_flow) {
                        LinkProfile profile;
                        profile.load = std::move(link_load_combined);
                        profile.period = job_profile.period;
                        profile.max_load = profile.load.empty()
                                                ? 0.0
                                                : *std::max_element(profile.load.begin(), profile.load.end());
                        throttled_profiles.emplace(throttle, std::move(profile));
                        link_has_flow = true;
                    } else {
                        break;
                    }
                }

                if (link_has_flow) {
                    LinkLoadEntry entry;
                    entry.link_id = link_id;
                    entry.job_id = job_id;
                    entry.iter_count = job.iter_count();
                    entry.profiles = std::move(throttled_profiles);
                    link_entries.push_back(std::move(entry));
                    cross_rack_jobs_set.insert(job_id);
                }
            }
        }
    }

    result.cross_rack_jobs.assign(cross_rack_jobs_set.begin(), cross_rack_jobs_set.end());
    std::sort(result.cross_rack_jobs.begin(), result.cross_rack_jobs.end());
    return result;
}

RemMatrix InitializeRem(int num_leaves, int num_spines, double link_bandwidth, int routing_time) {
    RemMatrix rem(num_leaves, SpineLoads(num_spines));
    for (int leaf = 0; leaf < num_leaves; ++leaf) {
        for (int spine = 0; spine < num_spines; ++spine) {
            rem[leaf][spine].up = std::vector<double>(routing_time, link_bandwidth);
            rem[leaf][spine].down = std::vector<double>(routing_time, link_bandwidth);
        }
    }
    return rem;
}

UsageMap InitializeUsage(const std::vector<int> &job_ids,
                         int num_leaves,
                         int num_spines,
                         int routing_time) {
    UsageMap usage;
    for (int job_id : job_ids) {
        JobUsageMatrix matrix(num_leaves, std::vector<DirectionalLoad>(num_spines));
        for (int leaf = 0; leaf < num_leaves; ++leaf) {
            for (int spine = 0; spine < num_spines; ++spine) {
                matrix[leaf][spine].up = std::vector<double>(routing_time, 0.0);
                matrix[leaf][spine].down = std::vector<double>(routing_time, 0.0);
            }
        }
        usage.emplace(job_id, std::move(matrix));
    }
    return usage;
}

void UpdateTimeRange(int start_time,
                     int end_time,
                     const FlowInstance &flow,
                     const std::vector<std::pair<int, double>> &selected_spines,
                     RemMatrix &rem,
                     UsageMap &usage) {
    const int src_leaf = flow.src_rack;
    const int dst_leaf = flow.dst_rack;
    for (int t = start_time; t <= end_time; ++t) {
        int history_index = t - flow.progress_shift;
        if (history_index < 0 || history_index >= static_cast<int>(flow.progress_history.size())) {
            continue;
        }
        const double progress_value = flow.progress_history[history_index];
        for (const auto &[spine, mult] : selected_spines) {
            double time_req = progress_value * mult;
            if (src_leaf < static_cast<int>(rem.size()) && spine < static_cast<int>(rem[src_leaf].size()) &&
                t < static_cast<int>(rem[src_leaf][spine].up.size())) {
                rem[src_leaf][spine].up[t] -= time_req;
            }
            if (dst_leaf < static_cast<int>(rem.size()) && spine < static_cast<int>(rem[dst_leaf].size()) &&
                t < static_cast<int>(rem[dst_leaf][spine].down.size())) {
                rem[dst_leaf][spine].down[t] -= time_req;
            }

            auto usage_it = usage.find(flow.job_id);
            if (usage_it != usage.end()) {
                if (src_leaf < static_cast<int>(usage_it->second.size()) &&
                    spine < static_cast<int>(usage_it->second[src_leaf].size()) &&
                    t < static_cast<int>(usage_it->second[src_leaf][spine].up.size())) {
                    usage_it->second[src_leaf][spine].up[t] += time_req;
                }
                if (dst_leaf < static_cast<int>(usage_it->second.size()) &&
                    spine < static_cast<int>(usage_it->second[dst_leaf].size()) &&
                    t < static_cast<int>(usage_it->second[dst_leaf][spine].down.size())) {
                    usage_it->second[dst_leaf][spine].down[t] += time_req;
                }
            }
        }
    }
}

std::vector<FlowInstance> BuildAllFlows(
    const std::unordered_map<int, Job> &job_map,
    const std::unordered_map<int, std::vector<int>> &job_deltas,
    const std::unordered_map<int, std::vector<double>> &job_throttle_rates,
    const std::unordered_map<int, std::vector<int>> &job_periods,
    const std::unordered_map<int, int> &job_iterations) {
    std::vector<FlowInstance> all_flows;

    for (const auto &[job_id, job] : job_map) {
        auto delta_it = job_deltas.find(job_id);
        auto throttle_it = job_throttle_rates.find(job_id);
        auto periods_it = job_periods.find(job_id);
        auto iter_it = job_iterations.find(job_id);
        if (delta_it == job_deltas.end() || throttle_it == job_throttle_rates.end() ||
            periods_it == job_periods.end() || iter_it == job_iterations.end()) {
            continue;
        }

        const auto &deltas = delta_it->second;
        const auto &throttles = throttle_it->second;
        const auto &periods_vec = periods_it->second;
        int iterations = iter_it->second;

        int shift = 0;
        for (int iter = 0; iter < iterations; ++iter) {
            shift += (iter < static_cast<int>(deltas.size())) ? deltas[iter] : 0;
            double throttle = (iter < static_cast<int>(throttles.size())) ? throttles[iter] : 1.0;

            auto profile_it = job.profiles().find(throttle);
            if (profile_it == job.profiles().end()) {
                shift += (iter < static_cast<int>(periods_vec.size())) ? periods_vec[iter] : 0;
                continue;
            }
            const JobProfile &profile = profile_it->second;

            for (const auto &flow : profile.flows) {
                FlowInstance instance;
                instance.job_id = job_id;
                instance.flow_id = flow.flow_id;
                instance.iteration = iter;
                instance.src_rack = flow.src_rack;
                instance.dst_rack = flow.dst_rack;
                instance.start_time = flow.start_time;
                instance.end_time = flow.end_time;
                instance.eff_start_time = flow.start_time + shift;
                instance.eff_end_time = flow.end_time + shift;
                instance.progress_shift = shift;
                instance.throttle_rate = throttle;
                instance.progress_history = flow.progress_history;
                instance.dir = flow.dir;
                instance.max_load = flow.progress_history.empty()
                                         ? 0.0
                                         : *std::max_element(flow.progress_history.begin(),
                                                             flow.progress_history.end());
                all_flows.push_back(std::move(instance));
            }

            shift += (iter < static_cast<int>(periods_vec.size())) ? periods_vec[iter] : 0;
        }
    }

    return all_flows;
}

ColoringOutcome RouteFlowsGraphColoringV3(
    std::vector<FlowInstance> &all_flows,
    RemMatrix &rem,
    UsageMap &usage,
    int num_spines,
    std::unordered_map<DecisionKey, std::vector<std::pair<int, double>>, DecisionKeyHash> &lb_decisions) {
    ColoringOutcome outcome;
    if (all_flows.empty()) {
        outcome.min_affected_time = 0;
        return outcome;
    }

    std::sort(all_flows.begin(), all_flows.end(), [](const FlowInstance &a, const FlowInstance &b) {
        return a.eff_start_time < b.eff_start_time;
    });

    for (auto &flow : all_flows) {
        flow.traffic_id = std::to_string(flow.eff_start_time) + "_" + std::to_string(flow.job_id);
        flow.traffic_member_id = std::to_string(flow.job_id) + "_" + std::to_string(flow.src_rack) + "_" +
                                 std::to_string(flow.dst_rack);
    }

    std::unordered_map<std::string, std::vector<FlowInstance *>> traffic_id_to_flows;
    for (auto &flow : all_flows) {
        traffic_id_to_flows[flow.traffic_id].push_back(&flow);
    }

    std::unordered_map<std::string, std::string> hash_to_traffic_id;
    for (auto &kv : traffic_id_to_flows) {
        const std::string &traffic_id = kv.first;
        auto &flows = kv.second;
        std::sort(flows.begin(), flows.end(), [](FlowInstance *a, FlowInstance *b) {
            return a->traffic_member_id < b->traffic_member_id;
        });
        std::string pattern;
        for (std::size_t idx = 0; idx < flows.size(); ++idx) {
            if (idx > 0) {
                pattern.push_back('#');
            }
            pattern += flows[idx]->traffic_member_id;
        }
        std::string pattern_hash = Md5Hex(pattern);
        for (auto *flow_ptr : flows) {
            flow_ptr->traffic_pattern_hash = pattern_hash;
        }
        hash_to_traffic_id[pattern_hash] = traffic_id;
    }

    std::unordered_set<std::string> unique_hashes;
    for (const auto &kv : hash_to_traffic_id) {
        unique_hashes.insert(kv.first);
    }

    std::vector<FlowInstance *> current_flows;
    for (const auto &hash : unique_hashes) {
        const std::string &traffic_pattern_rep = hash_to_traffic_id[hash];
        auto &flows = traffic_id_to_flows[traffic_pattern_rep];
        current_flows.insert(current_flows.end(), flows.begin(), flows.end());
    }

    std::vector<std::tuple<std::string, std::string, int>> edges;
    edges.reserve(current_flows.size());
    int flow_counter = 0;
    for (auto *flow_ptr : current_flows) {
        ++flow_counter;
        std::string left = std::to_string(flow_ptr->src_rack) + "_l";
        std::string right = std::to_string(flow_ptr->dst_rack) + "_r";
        edges.emplace_back(left, right, flow_counter);
    }

    auto color_result = ColorBipartiteMultigraph(edges);
    auto &edge_color_map = color_result.first;

    std::unordered_map<std::string, std::vector<int>> color_id_to_color;
    flow_counter = 0;
    for (auto *flow_ptr : current_flows) {
        ++flow_counter;
        std::string color_id = flow_ptr->traffic_pattern_hash + "_" + flow_ptr->traffic_member_id;
        int color = edge_color_map[flow_counter];
        color_id_to_color[color_id].push_back(color);
    }

    for (auto &flow : all_flows) {
        std::string color_id = flow.traffic_pattern_hash + "_" + flow.traffic_member_id;
        auto &color_list = color_id_to_color[color_id];
        if (color_list.empty()) {
            continue;
        }
        int color = color_list.front();
        std::rotate(color_list.begin(), color_list.begin() + 1, color_list.end());
        int chosen_spine = (color - 1) % std::max(1, num_spines);
        std::vector<std::pair<int, double>> selected_spines{{chosen_spine, 1.0}};
        DecisionKey key{flow.job_id, flow.flow_id, flow.iteration};
        lb_decisions[key] = selected_spines;

        outcome.min_affected_time = std::min(outcome.min_affected_time, flow.eff_start_time);
        outcome.max_affected_time = std::max(outcome.max_affected_time, flow.eff_end_time);

        UpdateTimeRange(flow.eff_start_time, flow.eff_end_time, flow, selected_spines, rem, usage);
    }

    if (outcome.min_affected_time == std::numeric_limits<int>::max()) {
        outcome.min_affected_time = 0;
    }
    return outcome;
}

ColoringOutcome RouteFlowsGraphColoringV7(
    std::vector<FlowInstance> &all_flows,
    RemMatrix &rem,
    UsageMap &usage,
    int num_spines,
    std::unordered_map<DecisionKey, std::vector<std::pair<int, double>>, DecisionKeyHash> &lb_decisions,
    const json &run_context,
    int max_subflow_count,
    double link_bandwidth,
    bool early_return) {
    ColoringOutcome outcome;
    outcome.min_affected_time = std::numeric_limits<int>::max();
    outcome.max_affected_time = 0;

    if (all_flows.empty()) {
        outcome.min_affected_time = 0;
        return outcome;
    }

    double available_colors_max = static_cast<double>(num_spines * std::max(1, max_subflow_count));
    double subflow_capacity = (max_subflow_count > 0) ? link_bandwidth / static_cast<double>(max_subflow_count) : 0.0;

    std::sort(all_flows.begin(), all_flows.end(), [](const FlowInstance &a, const FlowInstance &b) {
        return a.eff_start_time < b.eff_start_time;
    });

    int flows_max_time = 0;
    for (const auto &flow : all_flows) {
        flows_max_time = std::max(flows_max_time, flow.eff_end_time);
    }

    std::vector<std::vector<int>> edge_count_in;
    std::vector<std::vector<int>> edge_count_out;
    int rack_capacity = std::max<int>(1, num_spines);
    edge_count_in.resize(rack_capacity);
    edge_count_out.resize(rack_capacity);
    for (auto &vec : edge_count_in) {
        vec.assign(flows_max_time + 1, 0);
    }
    for (auto &vec : edge_count_out) {
        vec.assign(flows_max_time + 1, 0);
    }

    for (auto &flow : all_flows) {
        int needed_subflows = 1;
        if (subflow_capacity > 0.0) {
            needed_subflows = static_cast<int>(std::ceil(flow.max_load / subflow_capacity));
        }
        needed_subflows = std::max(1, needed_subflows);
        flow.needed_subflows = needed_subflows;

        if (flow.src_rack >= static_cast<int>(edge_count_out.size())) {
            edge_count_out.resize(flow.src_rack + 1, std::vector<int>(flows_max_time + 1, 0));
        }
        if (flow.dst_rack >= static_cast<int>(edge_count_in.size())) {
            edge_count_in.resize(flow.dst_rack + 1, std::vector<int>(flows_max_time + 1, 0));
        }

        for (int t = flow.eff_start_time; t <= flow.eff_end_time && t <= flows_max_time; ++t) {
            edge_count_out[flow.src_rack][t] += needed_subflows;
            edge_count_in[flow.dst_rack][t] += needed_subflows;
        }
    }

    std::vector<double> max_edge_count(flows_max_time + 1, 0.0);
    int divisor = std::max(1, max_subflow_count);
    for (const auto &vec : edge_count_in) {
        for (int t = 0; t <= flows_max_time; ++t) {
            max_edge_count[t] = std::max(max_edge_count[t], vec[t] / static_cast<double>(divisor));
        }
    }
    for (const auto &vec : edge_count_out) {
        for (int t = 0; t <= flows_max_time; ++t) {
            max_edge_count[t] = std::max(max_edge_count[t], vec[t] / static_cast<double>(divisor));
        }
    }

    if (early_return) {
        bool in_bad_range = false;
        int range_start = 0;
        for (int t = 0; t <= flows_max_time; ++t) {
            if (max_edge_count[t] > available_colors_max) {
                if (!in_bad_range) {
                    in_bad_range = true;
                    range_start = t;
                }
            } else if (in_bad_range) {
                in_bad_range = false;
                outcome.bad_ranges.emplace_back(range_start, t - 1);
            }
        }
        if (in_bad_range) {
            outcome.bad_ranges.emplace_back(range_start, flows_max_time);
        }
        if (!outcome.bad_ranges.empty()) {
            return outcome;
        }
    }

    std::unordered_map<std::string, std::vector<FlowInstance *>> traffic_id_to_flows;
    std::unordered_map<std::string, std::vector<std::pair<int, int>>> hash_to_time_ranges;
    std::unordered_map<std::string, std::unordered_set<int>> pattern_to_src;
    std::unordered_map<std::string, std::unordered_set<int>> pattern_to_dst;
    std::unordered_map<std::string, std::string> hash_to_traffic_id;

    for (auto &flow : all_flows) {
        flow.traffic_id = std::to_string(flow.eff_start_time) + "_" + std::to_string(flow.job_id) + "_" +
                          std::to_string(flow.throttle_rate);
        flow.traffic_member_id = std::to_string(flow.job_id) + "_" + std::to_string(flow.src_rack) + "_" +
                                 std::to_string(flow.dst_rack) + "_" + std::to_string(flow.needed_subflows);
        traffic_id_to_flows[flow.traffic_id].push_back(&flow);
    }

    for (auto &kv : traffic_id_to_flows) {
        const std::string &traffic_id = kv.first;
        auto &flows = kv.second;
        std::sort(flows.begin(), flows.end(), [](FlowInstance *a, FlowInstance *b) {
            return a->traffic_member_id < b->traffic_member_id;
        });

        std::string pattern;
        for (std::size_t idx = 0; idx < flows.size(); ++idx) {
            if (idx > 0) {
                pattern.push_back('#');
            }
            pattern += flows[idx]->traffic_member_id;
        }
        std::string pattern_hash = Md5Hex16(pattern);
        int min_start = std::numeric_limits<int>::max();
        int max_end = 0;
        for (auto *flow_ptr : flows) {
            flow_ptr->traffic_pattern_hash = pattern_hash;
            min_start = std::min(min_start, flow_ptr->eff_start_time);
            max_end = std::max(max_end, flow_ptr->eff_end_time);
            pattern_to_src[pattern_hash].insert(flow_ptr->src_rack);
            pattern_to_dst[pattern_hash].insert(flow_ptr->dst_rack);
        }
        hash_to_traffic_id[pattern_hash] = traffic_id;
        hash_to_time_ranges[pattern_hash].push_back({min_start, max_end});
    }

    MergedRangeMap merged_ranges = MergeOverlappingRangesV7(hash_to_time_ranges, pattern_to_src, pattern_to_dst);

    std::vector<ColoringSolution> solutions;
    std::vector<std::pair<int, int>> bad_ranges;

    for (const auto &kv : merged_ranges) {
        const auto &overlapping_keys = kv.first;
        const auto &overlapping_ranges = kv.second;

        std::vector<FlowInstance *> current_flows;
        for (const auto &hash : overlapping_keys) {
            const std::string &traffic_pattern_rep = hash_to_traffic_id[hash];
            auto &flows = traffic_id_to_flows[traffic_pattern_rep];
            current_flows.insert(current_flows.end(), flows.begin(), flows.end());
        }

        std::vector<std::tuple<std::string, std::string, int>> edges;
        int subflow_counter = 0;
        for (auto *flow_ptr : current_flows) {
            for (int subflow = 0; subflow < flow_ptr->needed_subflows; ++subflow) {
                ++subflow_counter;
                std::string left = std::to_string(flow_ptr->src_rack) + "_l";
                std::string right = std::to_string(flow_ptr->dst_rack) + "_r";
                edges.emplace_back(left, right, subflow_counter);
            }
        }

        auto color_result = ColorBipartiteMultigraph(edges);
        const auto &edge_color_map = color_result.first;

        std::unordered_map<std::string, std::vector<int>> color_id_to_color;
        subflow_counter = 0;
        for (auto *flow_ptr : current_flows) {
            for (int subflow = 0; subflow < flow_ptr->needed_subflows; ++subflow) {
                ++subflow_counter;
                std::string color_id = flow_ptr->traffic_pattern_hash + "_" + flow_ptr->traffic_member_id;
                int color = edge_color_map.at(subflow_counter);
                color_id_to_color[color_id].push_back(color);
            }
        }

        std::unordered_set<int> colors_used;
        for (const auto &kv_color : color_id_to_color) {
            colors_used.insert(kv_color.second.begin(), kv_color.second.end());
        }
        double used_spines = colors_used.size() / static_cast<double>(std::max(1, max_subflow_count));

        ColoringSolution base_solution;
        for (const auto &hash : overlapping_keys) {
            base_solution.patterns.insert(hash);
        }
        base_solution.coloring = color_id_to_color;

        for (const auto &time_range : overlapping_ranges) {
            ColoringSolution snapshot = base_solution;
            snapshot.time_range = time_range;
            solutions.push_back(std::move(snapshot));
            if (used_spines > num_spines) {
                bad_ranges.push_back(time_range);
            }
        }
    }

    if (early_return && !bad_ranges.empty()) {
        outcome.bad_ranges = std::move(bad_ranges);
        return outcome;
    }

    for (auto &flow : all_flows) {
        ColoringSolution *solution = FindColoringForPattern(solutions, flow.eff_start_time, flow.traffic_pattern_hash);
        if (!solution) {
            continue;
        }
        std::string color_id = flow.traffic_pattern_hash + "_" + flow.traffic_member_id;
        auto color_it = solution->coloring.find(color_id);
        if (color_it == solution->coloring.end() || color_it->second.empty()) {
            continue;
        }
        auto &color_list = color_it->second;

        std::unordered_map<int, int> spine_counts;
        for (int subflow = 0; subflow < flow.needed_subflows; ++subflow) {
            int color = color_list.front();
            std::rotate(color_list.begin(), color_list.begin() + 1, color_list.end());
            int chosen_spine = color - 1;
            if (max_subflow_count > 0) {
                chosen_spine /= max_subflow_count;
            }
            if (num_spines > 0) {
                chosen_spine %= num_spines;
            }
            spine_counts[chosen_spine] += 1;
        }

        std::vector<std::pair<int, double>> selected_spines;
        for (const auto &kv_spine : spine_counts) {
            double ratio = static_cast<double>(kv_spine.second) /
                           static_cast<double>(std::max(1, flow.needed_subflows));
            selected_spines.emplace_back(kv_spine.first, ratio);
        }

        DecisionKey key{flow.job_id, flow.flow_id, flow.iteration};
        lb_decisions[key] = selected_spines;

        outcome.min_affected_time = std::min(outcome.min_affected_time, flow.eff_start_time);
        outcome.max_affected_time = std::max(outcome.max_affected_time, flow.eff_end_time);

        UpdateTimeRange(flow.eff_start_time, flow.eff_end_time, flow, selected_spines, rem, usage);
    }

    std::sort(bad_ranges.begin(), bad_ranges.end());
    outcome.bad_ranges = std::move(bad_ranges);
    if (outcome.min_affected_time == std::numeric_limits<int>::max()) {
        outcome.min_affected_time = 0;
    }
    return outcome;
}

struct DisjointSet {
    std::vector<int> parent;
    std::vector<int> rank;

    explicit DisjointSet(int n) : parent(n), rank(n, 0) {
        std::iota(parent.begin(), parent.end(), 0);
    }

    int Find(int x) {
        if (parent[x] != x) {
            parent[x] = Find(parent[x]);
        }
        return parent[x];
    }

    void Union(int x, int y) {
        int root_x = Find(x);
        int root_y = Find(y);
        if (root_x == root_y) {
            return;
        }
        if (rank[root_x] < rank[root_y]) {
            parent[root_x] = root_y;
        } else if (rank[root_x] > rank[root_y]) {
            parent[root_y] = root_x;
        } else {
            parent[root_y] = root_x;
            rank[root_x] += 1;
        }
    }
};

bool RacksOverlap(const std::unordered_set<int> &src_a,
                  const std::unordered_set<int> &dst_a,
                  const std::unordered_set<int> &src_b,
                  const std::unordered_set<int> &dst_b) {
    for (int rack : src_a) {
        if (src_b.count(rack)) {
            return true;
        }
    }
    for (int rack : dst_a) {
        if (dst_b.count(rack)) {
            return true;
        }
    }
    return false;
}

ColoringSolution *FindColoringForPattern(std::vector<ColoringSolution> &solutions,
                                         int value,
                                         const std::string &pattern_hash) {
    for (auto &solution : solutions) {
        if (value >= solution.time_range.first && value <= solution.time_range.second &&
            solution.patterns.count(pattern_hash)) {
            return &solution;
        }
    }
    return nullptr;
}

using MergedRangeMap = std::map<std::vector<std::string>, std::vector<std::pair<int, int>>>;

MergedRangeMap MergeOverlappingRangesV7(
    const std::unordered_map<std::string, std::vector<std::pair<int, int>>> &ranges_dict,
    const std::unordered_map<std::string, std::unordered_set<int>> &traffic_pattern_to_src_racks,
    const std::unordered_map<std::string, std::unordered_set<int>> &traffic_pattern_to_dst_racks) {
    struct Interval {
        int start;
        int end;
        std::string key;
        std::unordered_set<int> src;
        std::unordered_set<int> dst;
    };

    std::vector<Interval> intervals;
    for (const auto &kv : ranges_dict) {
        const std::string &key = kv.first;
        auto src_it = traffic_pattern_to_src_racks.find(key);
        auto dst_it = traffic_pattern_to_dst_racks.find(key);
        std::unordered_set<int> src = (src_it != traffic_pattern_to_src_racks.end()) ? src_it->second
                                                                                      : std::unordered_set<int>{};
        std::unordered_set<int> dst = (dst_it != traffic_pattern_to_dst_racks.end()) ? dst_it->second
                                                                                      : std::unordered_set<int>{};
        for (const auto &range : kv.second) {
            intervals.push_back(Interval{range.first, range.second, key, src, dst});
        }
    }

    std::sort(intervals.begin(), intervals.end(), [](const Interval &a, const Interval &b) {
        if (a.start == b.start) {
            return a.end < b.end;
        }
        return a.start < b.start;
    });

    const int n = static_cast<int>(intervals.size());
    DisjointSet dsu(n);
    std::vector<int> active;
    for (int idx = 0; idx < n; ++idx) {
        const auto &interval = intervals[idx];
        std::vector<int> new_active;
        for (int active_idx : active) {
            if (intervals[active_idx].end >= interval.start) {
                if (RacksOverlap(intervals[active_idx].src, intervals[active_idx].dst,
                                 interval.src, interval.dst)) {
                    dsu.Union(idx, active_idx);
                }
                new_active.push_back(active_idx);
            }
        }
        new_active.push_back(idx);
        active.swap(new_active);
    }

    std::unordered_map<int, std::vector<std::pair<int, int>>> component_ranges;
    std::unordered_map<int, std::unordered_set<std::string>> component_keys;
    for (int idx = 0; idx < n; ++idx) {
        int root = dsu.Find(idx);
        component_ranges[root].push_back({intervals[idx].start, intervals[idx].end});
        component_keys[root].insert(intervals[idx].key);
    }

    MergedRangeMap merged;
    for (const auto &kv : component_ranges) {
        int root = kv.first;
        auto ranges = kv.second;
        std::sort(ranges.begin(), ranges.end(), [](const auto &a, const auto &b) {
            if (a.first == b.first) {
                return a.second < b.second;
            }
            return a.first < b.first;
        });

        std::vector<std::pair<int, int>> summarized;
        if (!ranges.empty()) {
            auto current = ranges.front();
            for (std::size_t i = 1; i < ranges.size(); ++i) {
                if (ranges[i].first <= current.second + 1) {
                    current.second = std::max(current.second, ranges[i].second);
                } else {
                    summarized.push_back(current);
                    current = ranges[i];
                }
            }
            summarized.push_back(current);
        }

        std::vector<std::string> keys(component_keys[root].begin(), component_keys[root].end());
        std::sort(keys.begin(), keys.end());

        auto &entry = merged[keys];
        entry.insert(entry.end(), summarized.begin(), summarized.end());
    }

    for (auto &kv : merged) {
        auto &ranges = kv.second;
        std::sort(ranges.begin(), ranges.end(), [](const auto &a, const auto &b) {
            if (a.first == b.first) {
                return a.second < b.second;
            }
            return a.first < b.first;
        });
        std::vector<std::pair<int, int>> merged_ranges;
        for (const auto &range : ranges) {
            if (!merged_ranges.empty() && range.first <= merged_ranges.back().second + 1) {
                merged_ranges.back().second = std::max(merged_ranges.back().second, range.second);
            } else {
                merged_ranges.push_back(range);
            }
        }
        ranges.swap(merged_ranges);
    }

    return merged;
}

int ComputeMaxDegree(const std::vector<std::pair<std::string, std::string>> &edges) {
    std::unordered_map<std::string, int> degree;
    for (const auto &edge : edges) {
        degree[edge.first] += 1;
        degree[edge.second] += 1;
    }
    int max_degree = 0;
    for (const auto &[_, value] : degree) {
        max_degree = std::max(max_degree, value);
    }
    return max_degree;
}

std::unordered_map<std::string, std::string> HopcroftKarp(
    const std::unordered_map<std::string, std::vector<std::string>> &graph) {
    const std::string nil = "__NIL__";
    std::unordered_map<std::string, std::string> pair_u;
    std::unordered_map<std::string, std::string> pair_v;
    std::unordered_map<std::string, int> dist;

    auto bfs = [&]() {
        std::queue<std::string> q;
        const int inf = std::numeric_limits<int>::max();
        for (const auto &kv : graph) {
            const std::string &u = kv.first;
            if (!pair_u.count(u)) {
                dist[u] = 0;
                q.push(u);
            } else {
                dist[u] = inf;
            }
        }
        dist[nil] = inf;
        while (!q.empty()) {
            std::string u = q.front();
            q.pop();
            if (dist[u] < dist[nil]) {
                const auto &neighbors = graph.at(u);
                for (const auto &v : neighbors) {
                    std::string matched = pair_v.count(v) ? pair_v[v] : nil;
                    if (dist[matched] == inf) {
                        dist[matched] = dist[u] + 1;
                        q.push(matched);
                    }
                }
            }
        }
        return dist[nil] != inf;
    };

    std::function<bool(const std::string &)> dfs = [&](const std::string &u) -> bool {
        const int inf = std::numeric_limits<int>::max();
        if (u != nil) {
            const auto &neighbors = graph.at(u);
            for (const auto &v : neighbors) {
                std::string matched = pair_v.count(v) ? pair_v[v] : nil;
                if (dist[matched] == dist[u] + 1 && dfs(matched)) {
                    pair_u[u] = v;
                    pair_v[v] = u;
                    return true;
                }
            }
            dist[u] = inf;
            return false;
        }
        return true;
    };

    while (bfs()) {
        for (const auto &kv : graph) {
            const std::string &u = kv.first;
            if (!pair_u.count(u)) {
                dfs(u);
            }
        }
    }

    std::unordered_map<std::string, std::string> matching;
    for (const auto &kv : pair_u) {
        if (!kv.second.empty()) {
            matching.emplace(kv.first, kv.second);
        }
    }
    return matching;
}

std::pair<std::unordered_map<int, int>, int> ColorBipartiteMultigraphHelper(
    const std::vector<std::tuple<std::string, std::string, int>> &input_edges) {
    if (input_edges.empty()) {
        return {{}, 0};
    }

    std::vector<std::pair<std::string, std::string>> edges;
    edges.reserve(input_edges.size());
    for (const auto &[u, v, _] : input_edges) {
        edges.emplace_back(u, v);
    }

    int max_degree = ComputeMaxDegree(edges);
    std::vector<std::pair<std::string, std::string>> edge_list = edges;
    std::vector<int> colors(edge_list.size(), 0);
    std::vector<int> remaining(edge_list.size());
    std::iota(remaining.begin(), remaining.end(), 0);

    std::mt19937 rng(123456);

    for (int color = 1; color <= max_degree && !remaining.empty(); ++color) {
        std::unordered_set<std::pair<std::string, std::string>, PairHash> uv_pairs;
        for (int idx : remaining) {
            uv_pairs.insert(edge_list[idx]);
        }

        std::unordered_map<std::string, std::vector<std::string>> graph;
        for (const auto &uv : uv_pairs) {
            graph[uv.first].push_back(uv.second);
        }
        for (auto &kv : graph) {
            auto &vec = kv.second;
            std::shuffle(vec.begin(), vec.end(), rng);
        }

        auto matching = HopcroftKarp(graph);

        std::unordered_set<int> matched_indices;
        for (const auto &kv : matching) {
            const std::string &u = kv.first;
            const std::string &v = kv.second;
            for (int idx : remaining) {
                if (edge_list[idx].first == u && edge_list[idx].second == v &&
                    matched_indices.insert(idx).second) {
                    colors[idx] = color;
                    break;
                }
            }
        }

        std::vector<int> new_remaining;
        new_remaining.reserve(remaining.size());
        for (int idx : remaining) {
            if (!matched_indices.count(idx)) {
                new_remaining.push_back(idx);
            }
        }
        remaining.swap(new_remaining);
    }

    std::unordered_map<int, int> edge_color_map;
    edge_color_map.reserve(input_edges.size());
    for (std::size_t i = 0; i < input_edges.size(); ++i) {
        int edge_id = std::get<2>(input_edges[i]);
        edge_color_map[edge_id] = colors[i];
    }

    return {edge_color_map, max_degree};
}

std::pair<std::unordered_map<int, int>, int> ColorBipartiteMultigraph(
    const std::vector<std::tuple<std::string, std::string, int>> &input_edges) {
    auto [edge_color_map, max_degree] = ColorBipartiteMultigraphHelper(input_edges);
    auto count_colors = [&](const std::unordered_map<int, int> &map) {
        std::unordered_set<int> colors;
        for (const auto &kv : map) {
            colors.insert(kv.second);
        }
        return colors.size();
    };

    int colors_used = count_colors(edge_color_map);
    int round = 1;
    while (colors_used > max_degree && round < 10) {
        std::tie(edge_color_map, max_degree) = ColorBipartiteMultigraphHelper(input_edges);
        colors_used = count_colors(edge_color_map);
        ++round;
    }

    return {edge_color_map, max_degree};
}

int FirstNonZeroIndex(const std::vector<double> &values) {
    for (std::size_t i = 0; i < values.size(); ++i) {
        if (values[i] > 0.0) {
            return static_cast<int>(i);
        }
    }
    return static_cast<int>(values.size());
}

int LastNonZeroIndex(const std::vector<double> &values) {
    for (std::size_t i = values.size(); i-- > 0;) {
        if (values[i] > 0.0) {
            return static_cast<int>(i);
        }
    }
    return -1;
}

std::string DirectionToString(Direction d) {
    return d == Direction::kUp ? "up" : "down";
}

Direction StringToDirection(const std::string &dir) {
    return (dir == "up") ? Direction::kUp : Direction::kDown;
}

std::vector<double> AddSignals(const std::vector<double> &a, const std::vector<double> &b) {
    const std::size_t max_size = std::max(a.size(), b.size());
    std::vector<double> result(max_size, 0.0);
    for (std::size_t i = 0; i < max_size; ++i) {
        if (i < a.size()) {
            result[i] += a[i];
        }
        if (i < b.size()) {
            result[i] += b[i];
        }
    }
    return result;
}

void AddSignalToSum(const std::vector<double> &signal, std::vector<double> &sum_signal) {
    if (sum_signal.size() < signal.size()) {
        sum_signal.resize(signal.size(), 0.0);
    }
    for (std::size_t i = 0; i < signal.size(); ++i) {
        sum_signal[i] += signal[i];
    }
}

std::vector<double> ConcatenateWithZeros(std::vector<double> base, int zero_count) {
    base.insert(base.end(), zero_count, 0.0);
    return base;
}

std::vector<double> MakeZeros(int count) {
    return std::vector<double>(count, 0.0);
}

std::vector<double> PadToLength(const std::vector<double> &signal, std::size_t length) {
    if (signal.size() >= length) {
        return signal;
    }
    std::vector<double> result(signal);
    result.resize(length, 0.0);
    return result;
}

std::vector<double> SumSignals(const std::vector<std::vector<double>> &signals) {
    std::size_t max_len = 0;
    for (const auto &s : signals) {
        max_len = std::max(max_len, s.size());
    }
    std::vector<double> sum(max_len, 0.0);
    for (const auto &s : signals) {
        for (std::size_t i = 0; i < s.size(); ++i) {
            sum[i] += s[i];
        }
    }
    return sum;
}

std::vector<double> DivideSignal(const std::vector<double> &signal, double divisor) {
    std::vector<double> result(signal.size(), 0.0);
    if (divisor == 0.0) {
        return result;
    }
    for (std::size_t i = 0; i < signal.size(); ++i) {
        result[i] = signal[i] / divisor;
    }
    return result;
}

std::vector<double> MultiplySignal(const std::vector<double> &signal, double multiplier) {
    std::vector<double> result(signal.size(), 0.0);
    for (std::size_t i = 0; i < signal.size(); ++i) {
        result[i] = signal[i] * multiplier;
    }
    return result;
}

void Job::AddLinkLoad(const LinkId &id, std::shared_ptr<LinkJobLoad> load) {
    link_loads_[id] = std::move(load);
}

const std::vector<double> &Job::GetBaseSignal(Throttle throttle_rate) const {
    auto cache_it = base_signal_cache_.find(throttle_rate);
    if (cache_it != base_signal_cache_.end()) {
        return cache_it->second;
    }

    std::vector<double> signal;
    for (const auto &entry : link_loads_) {
        const auto &link_signal = entry.second->GetBaseSignal(throttle_rate);
        signal = AddSignals(signal, link_signal);
    }

    auto [it, _] = base_signal_cache_.emplace(throttle_rate, std::move(signal));
    return it->second;
}

std::pair<int, int> Job::GetActiveRange(Throttle throttle_rate, double inflate) const {
    const auto &base_signal = GetBaseSignal(throttle_rate);
    int start_time = base_signal.empty() ? 0 : FirstNonZeroIndex(base_signal);
    int end_time = base_signal.empty() ? 0 : LastNonZeroIndex(base_signal);

    if (start_time >= static_cast<int>(base_signal.size())) {
        start_time = 0;
        end_time = 0;
    }

    if (inflate > 1.0) {
        int active_range = std::max(0, end_time - start_time);
        int inflate_amount = static_cast<int>(active_range * (inflate - 1.0));
        start_time = std::max(0, start_time - inflate_amount / 2);
        end_time = end_time + inflate_amount / 2;
    }

    return {start_time, end_time};
}

int FindEarliestAvailableTime(int start,
                              int end,
                              const std::vector<double> &rem,
                              double max_value) {
    int delay = 0;
    auto window_sum = std::count_if(rem.begin() + start, rem.begin() + end, [max_value](double v) {
        return v < max_value;
    });
    while (window_sum > 0) {
        if (rem[start + delay] < max_value) {
            --window_sum;
        }
        if (rem[end + delay] < max_value) {
            ++window_sum;
        }
        ++delay;
    }
    return delay;
}

int FindEarliestAvailableTimeAllLinks(int start,
                                      int end,
                                      const std::unordered_map<LinkId, std::vector<double>, LinkIdHash> &rem,
                                      const std::unordered_map<LinkId, double, LinkIdHash> &max_loads,
                                      double multiplier) {
    int delay = 0;
    long window_sum = 0;

    for (const auto &entry : rem) {
        const auto &link_id = entry.first;
        const auto &signal = entry.second;
        auto it = max_loads.find(link_id);
        if (it == max_loads.end()) {
            continue;
        }
        const double limit = it->second * multiplier;
        for (int t = start; t < end; ++t) {
            if (signal[t] < limit) {
                ++window_sum;
            }
        }
    }

    while (window_sum > 0) {
        for (const auto &entry : rem) {
            const auto &link_id = entry.first;
            const auto &signal = entry.second;
            auto it = max_loads.find(link_id);
            if (it == max_loads.end()) {
                continue;
            }
            const double limit = it->second * multiplier;
            if (signal[start + delay] < limit) {
                --window_sum;
            }
            if (signal[end + delay] < limit) {
                ++window_sum;
            }
        }
        ++delay;
    }
    return delay;
}

int OverlapCount(int start, int end, const std::vector<std::pair<int, int>> &ranges) {
    int count = 0;
    for (const auto &range : ranges) {
        if (start <= range.second && range.first <= end) {
            ++count;
        }
    }
    return count;
}

std::string Md5Hex(const std::string &input) {
    boost::uuids::detail::md5 hash;
    boost::uuids::detail::md5::digest_type digest;
    hash.process_bytes(input.data(), input.size());
    hash.get_digest(digest);

    const unsigned int *data = reinterpret_cast<const unsigned int *>(&digest);
    std::ostringstream oss;
    for (int i = 0; i < 4; ++i) {
        oss << std::hex << std::setw(8) << std::setfill('0') << data[i];
    }
    return oss.str();
}

std::string Md5Hex16(const std::string &input) {
    auto full = Md5Hex(input);
    if (full.size() <= 16) {
        return full;
    }
    return full.substr(0, 16);
}

std::string FormatThrottle(double value) {
    constexpr double kEps = 1e-9;
    if (std::fabs(value - std::round(value)) < kEps) {
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(1) << value;
        return oss.str();
    }
    std::ostringstream oss;
    oss << value;
    return oss.str();
}

double CalculateBadRangeSum(const std::vector<std::pair<int, int>> &ranges) {
    long total = 0;
    for (const auto &[start, end] : ranges) {
        total += static_cast<long>(end - start + 1);
    }
    return static_cast<double>(total);
}

std::pair<double, double> GetBadRangeRatioV6(const std::vector<std::pair<int, int>> &new_bad_ranges,
                                             const std::vector<std::pair<int, int>> &prev_bad_ranges,
                                             int sim_length) {
    double new_ratio = 0.0;
    double prev_ratio = 0.0;
    if (sim_length > 0) {
        new_ratio = CalculateBadRangeSum(new_bad_ranges) / static_cast<double>(sim_length);
        prev_ratio = CalculateBadRangeSum(prev_bad_ranges) / static_cast<double>(sim_length);
    }
    return {new_ratio, prev_ratio};
}

std::optional<double> ExtractNumeric(const json &value) {
    if (value.is_number()) {
        return value.get<double>();
    }
    if (value.is_array()) {
        for (auto it = value.rbegin(); it != value.rend(); ++it) {
            if (it->is_number()) {
                return it->get<double>();
            }
        }
    }
    return std::nullopt;
}

void AppendToBadRanges(std::vector<std::pair<int, int>> &bad_ranges,
                       std::vector<std::pair<int, int>> new_bad_ranges) {
    if (new_bad_ranges.empty()) {
        return;
    }
    std::sort(new_bad_ranges.begin(), new_bad_ranges.end());
    const auto bad_range_to_add = new_bad_ranges.front();

    std::vector<std::pair<int, int>> copy = bad_ranges;
    bad_ranges.clear();
    for (const auto &[start, end] : copy) {
        if (start < bad_range_to_add.second) {
            bad_ranges.emplace_back(start, end);
        }
    }
    bad_ranges.push_back(bad_range_to_add);
}

std::vector<std::pair<int, int>> SummarizeBadRanges(std::vector<std::pair<int, int>> ranges) {
    if (ranges.empty()) {
        return ranges;
    }
    std::sort(ranges.begin(), ranges.end());
    std::vector<std::pair<int, int>> summarized;
    auto current = ranges.front();
    for (std::size_t i = 1; i < ranges.size(); ++i) {
        const auto &range = ranges[i];
        if (range.first <= current.second + 1) {
            current.second = std::max(current.second, range.second);
        } else {
            summarized.push_back(current);
            current = range;
        }
    }
    summarized.push_back(current);
    return summarized;
}

bool ShouldEarlyReturn(int current_round, int max_attempts) {
    return current_round < max_attempts;
}

std::optional<JobProfile> LoadJobProfile(const std::string &path) {
    std::ifstream input(path);
    if (!input) {
        return std::nullopt;
    }
    json data = json::parse(input, nullptr, false);
    if (data.is_discarded()) {
        return std::nullopt;
    }

    JobProfile profile;
    profile.period = data.value("period", 0);

    if (data.contains("flows") && data["flows"].is_array()) {
        for (const auto &flow_json : data["flows"]) {
            FlowProfile flow;
            flow.flow_id = flow_json.value("flow_id", -1);
            flow.job_id = flow_json.value("job_id", -1);
            flow.iteration = flow_json.value("iteration", 0);
            flow.subflow = flow_json.value("subflow", 0);
            flow.start_time = flow_json.value("start_time", 0);
            flow.end_time = flow_json.value("end_time", 0);
            flow.src_rack = flow_json.value("srcrack", 0);
            flow.dst_rack = flow_json.value("dstrack", 0);
            flow.dir = flow_json.value("dir", "");
            flow.fct = flow_json.value("fct", 0.0);
            flow.core = flow_json.value("core", 0);
            flow.label = flow_json.value("label", "");
            flow.flow_size = flow_json.value("flow_size", 0.0);
            
            std::cerr << "Loading profile for flow " << flow.flow_id << " of job " << flow.job_id
                      << " with size " << flow.flow_size << " bytes." << std::endl;

            if (flow_json.contains("progress_history") && flow_json["progress_history"].is_array()) {
                for (const auto &value : flow_json["progress_history"]) {
                    if (auto numeric = ExtractNumeric(value)) {
                        flow.progress_history.push_back(*numeric);
                    }
                }
            }

            std::cerr << "  Progress history has " << flow.progress_history.size() << " entries." << std::endl; 

            if (flow_json.contains("progress_history_summarized") &&
                flow_json["progress_history_summarized"].is_array()) {
                for (const auto &value : flow_json["progress_history_summarized"]) {
                    if (auto numeric = ExtractNumeric(value)) {
                        flow.progress_history_summarized.push_back(*numeric);
                    }
                }
            }

            profile.flows.push_back(std::move(flow));
        }
    }

    return profile;
}

std::unordered_map<int, std::unordered_map<Throttle, std::optional<JobProfile>>> LoadAllJobProfiles(
    const std::vector<json> &jobs,
    const json &run_context) {
    std::unordered_map<int, std::unordered_map<Throttle, std::optional<JobProfile>>> result;

    const std::string profiles_dir = run_context.value("profiles-dir", std::string{});

    std::vector<double> throttle_factors;
    if (run_context.contains("profiled-throttle-factors")) {
        for (const auto &value : run_context["profiled-throttle-factors"]) {
            throttle_factors.push_back(value.get<double>());
        }
    }

    for (const auto &job_json : jobs) {
        int job_id = job_json.at("job_id").get<int>();
        auto &profile_map = result[job_id];
        for (double throttle : throttle_factors) {
            if (profiles_dir.empty()) {
                profile_map[throttle] = std::nullopt;
                continue;
            }
            std::string path = profiles_dir + "/" + std::to_string(job_id) + "_" + FormatThrottle(throttle) + ".json";
            std::cerr << "Loading profile for job " << job_id << " at throttle " << throttle << " from " << path
                      << std::endl;
            profile_map[throttle] = LoadJobProfile(path);
        }
    }

    return result;
}

struct SchedulerContext {
    std::vector<json> jobs;
    json options;
    json run_context;
    std::string timing_file_path;
    std::string routing_file_path;
    int placement_seed = 0;
};

struct RoutingResult {
    std::vector<json> lb_decisions;
    std::vector<std::pair<int, int>> bad_ranges;
    int min_affected_time = 0;
    int max_affected_time = 0;
};

// Forward declarations for routing integration later.
RoutingResult RouteFlows(const std::vector<json> &jobs,
                         const json &options,
                         const json &run_context,
                         const std::unordered_map<int, Job> &job_map,
                         const std::vector<json> &job_timings);

ColoringOutcome RouteFlowsGraphColoringV3(
    std::vector<FlowInstance> &all_flows,
    RemMatrix &rem,
    UsageMap &usage,
    int num_spines,
    std::unordered_map<DecisionKey, std::vector<std::pair<int, double>>, DecisionKeyHash> &lb_decisions);

ColoringOutcome RouteFlowsGraphColoringV7(
    std::vector<FlowInstance> &all_flows,
    RemMatrix &rem,
    UsageMap &usage,
    int num_spines,
    std::unordered_map<DecisionKey, std::vector<std::pair<int, double>>, DecisionKeyHash> &lb_decisions,
    const json &run_context,
    int max_subflow_count,
    double link_bandwidth,
    bool early_return);

std::optional<JobProfile> LoadJobProfile(const std::string &path);

class TimingSolver {
  public:
    TimingSolver(const std::vector<json> &jobs,
                 const json &run_context,
                 const json &options,
                 std::unordered_map<int, std::unordered_map<Throttle, std::optional<JobProfile>>> job_profiles,
                 std::string scheme);

    virtual ~TimingSolver() = default;

    std::vector<json> GetZeroSolution();
    std::pair<std::vector<json>, Solution> Solve();
    std::pair<std::vector<json>, Solution> SolveWithBadRanges(const std::vector<std::pair<int, int>> &bad_ranges);
    std::pair<std::vector<json>, Solution> SolveWithInflation(double base_inflate);
    std::pair<std::vector<json>, Solution> SolveWithBadRangesAndInflation(
        const std::vector<std::pair<int, int>> &bad_ranges,
        double base_inflate);

    Solution MakeSolution(const std::vector<std::pair<int, int>> &bad_ranges = {}, double base_inflate = 1.0);

    const std::unordered_map<int, Job> &job_map() const { return job_map_; }
    double capacity() const { return capacity_; }
    int max_length() const { return max_length_; }
    const std::unordered_map<LinkId, std::shared_ptr<LinkLevelProblem>, LinkIdHash> &links() const {
        return links_;
    }

  protected:
    virtual Solution MakeSolutionImpl(const std::vector<std::pair<int, int>> &bad_ranges,
                                      double base_inflate) = 0;

    const json &run_context_;
    const json &options_;
    std::string scheme_;
    std::vector<double> throttle_rates_;
    int rack_count_ = 0;
    double link_bandwidth_ = 0.0;
    double capacity_ = 0.0;
    int max_length_ = 0;
    std::unordered_map<int, Job> job_map_;
    std::unordered_map<LinkId, std::shared_ptr<LinkLevelProblem>, LinkIdHash> links_;
    std::vector<int> cross_rack_jobs_;
};

std::vector<json> TimingSolver::GetZeroSolution() {
    Solution solution(job_map_);
    return solution.GetJobTimings();
}

std::pair<std::vector<json>, Solution> TimingSolver::Solve() {
    auto solution = MakeSolution();
    auto timings = solution.GetJobTimings();
    return {timings, std::move(solution)};
}

std::pair<std::vector<json>, Solution> TimingSolver::SolveWithBadRanges(
    const std::vector<std::pair<int, int>> &bad_ranges) {
    auto solution = MakeSolution(bad_ranges, 1.0);
    auto timings = solution.GetJobTimings();
    return {timings, std::move(solution)};
}

std::pair<std::vector<json>, Solution> TimingSolver::SolveWithInflation(double base_inflate) {
    auto solution = MakeSolution({}, base_inflate);
    auto timings = solution.GetJobTimings();
    return {timings, std::move(solution)};
}

std::pair<std::vector<json>, Solution> TimingSolver::SolveWithBadRangesAndInflation(
    const std::vector<std::pair<int, int>> &bad_ranges,
    double base_inflate) {
    auto solution = MakeSolution(bad_ranges, base_inflate);
    auto timings = solution.GetJobTimings();
    return {timings, std::move(solution)};
}

Solution TimingSolver::MakeSolution(const std::vector<std::pair<int, int>> &bad_ranges,
                                    double base_inflate) {
    return MakeSolutionImpl(bad_ranges, base_inflate);
}

TimingSolver::TimingSolver(
    const std::vector<json> &jobs,
    const json &run_context,
    const json &options,
    std::unordered_map<int, std::unordered_map<Throttle, std::optional<JobProfile>>> job_profiles,
    std::string scheme)
    : run_context_(run_context), options_(options), scheme_(std::move(scheme)) {

    if (run_context_.contains("profiled-throttle-factors")) {
        for (const auto &value : run_context_["profiled-throttle-factors"]) {
            throttle_rates_.push_back(value.get<double>());
        }
    } else {
        throttle_rates_.push_back(1.0);
    }

    rack_count_ = options_.at("machine-count").get<int>() /
                  options_.at("ft-server-per-rack").get<int>();
    link_bandwidth_ = options_.at("link-bandwidth").get<double>();
    capacity_ = options_.at("ft-core-count").get<double>() *
                options_.at("ft-agg-core-link-capacity-mult").get<double>();

    for (const auto &job_json : jobs) {
        const int job_id = job_json.at("job_id").get<int>();
        const int iter_count = job_json.at("iter_count").get<int>();
        const int base_period = job_json.at("base_period").get<int>();

        std::unordered_map<Throttle, int> periods;
        for (auto it = job_json.at("period").begin(); it != job_json.at("period").end(); ++it) {
            const double key = std::stod(it.key());
            periods[key] = it.value().get<int>();
        }

        std::unordered_map<Throttle, JobProfile> profiles_map;
        bool has_profiles = false;
        auto profile_it = job_profiles.find(job_id);
        if (profile_it != job_profiles.end()) {
            for (auto &kv : profile_it->second) {
                if (kv.second.has_value()) {
                    profiles_map.emplace(kv.first, std::move(kv.second.value()));
                    has_profiles = true;
                }
            }
        }

        job_map_.emplace(job_id,
                         Job(job_id,
                             std::move(profiles_map),
                             has_profiles,
                             iter_count,
                             std::move(periods),
                             base_period));
    }

    max_length_ = 0;
    for (const auto &[job_id, job] : job_map_) {
        max_length_ += job.base_period() * job.iter_count();
    }
    max_length_ *= 2;

    for (int rack = 0; rack < rack_count_; ++rack) {
        for (Direction dir : {Direction::kUp, Direction::kDown}) {
            LinkId link_id{rack, dir};
            links_.emplace(link_id,
                           std::make_shared<LinkLevelProblem>(link_id, max_length_,
                                                              run_context_.value("compat-score-mode", "under-cap")));
        }
    }

    auto loads = GetLinkLoads(job_map_, rack_count_, link_bandwidth_, throttle_rates_);
    cross_rack_jobs_ = std::move(loads.cross_rack_jobs);

    for (auto &link_entry : loads.link_loads) {
        const LinkId &link_id = link_entry.first;
        auto link_problem_it = links_.find(link_id);
        if (link_problem_it == links_.end()) {
            continue;
        }
        auto &link_problem = link_problem_it->second;
        for (auto &entry : link_entry.second) {
            auto job_it = job_map_.find(entry.job_id);
            if (job_it == job_map_.end()) {
                continue;
            }
            auto load = std::make_shared<LinkJobLoad>(&job_it->second, std::move(entry.profiles));
            job_it->second.AddLinkLoad(link_id, load);
            link_problem->AddJobLoad(std::move(load));
        }
    }
}

class LegoV2Solver : public TimingSolver {
  public:
    LegoV2Solver(const std::vector<json> &jobs,
                 const json &run_context,
                 const json &options,
                 std::unordered_map<int, std::unordered_map<Throttle, std::optional<JobProfile>>> job_profiles,
                 std::string scheme)
        : TimingSolver(jobs, run_context, options, std::move(job_profiles), std::move(scheme)) {}

  private:
    Solution MakeSolutionImpl(const std::vector<std::pair<int, int>> &bad_ranges,
                              double base_inflate) override;
};

Solution LegoV2Solver::MakeSolutionImpl(const std::vector<std::pair<int, int>> &bad_ranges,
                                        double base_inflate) {
    std::vector<std::shared_ptr<LinkLevelProblem>> link_list;
    link_list.reserve(links().size());
    for (const auto &kv : links()) {
        link_list.push_back(kv.second);
    }

    Solution sol(job_map());

    std::vector<int> job_ids;
    job_ids.reserve(job_map().size());
    for (const auto &[job_id, _] : job_map()) {
        job_ids.push_back(job_id);
    }

    std::vector<double> candidate_throttles;
    if (run_context_.value("throttle-search", false)) {
        candidate_throttles = throttle_rates_;
    } else {
        candidate_throttles = {1.0};
    }

    using LinkLoadMap = std::unordered_map<LinkId, double, LinkIdHash>;
    std::unordered_map<double, std::unordered_map<int, LinkLoadMap>> job_max_load;

    for (double throttle : candidate_throttles) {
        auto &per_job = job_max_load[throttle];
        for (int job_id : job_ids) {
            auto &per_link = per_job[job_id];
            for (const auto &link_problem : link_list) {
                per_link[link_problem->id()] = 0.0;
            }
        }
        for (const auto &link_problem : link_list) {
            for (const auto &job_load : link_problem->job_loads()) {
                if (!job_load->job()) {
                    continue;
                }
                int job_id = job_load->job()->job_id();
                auto profile_it = job_load->profiles().find(throttle);
                if (profile_it == job_load->profiles().end()) {
                    continue;
                }
                per_job[job_id][link_problem->id()] = profile_it->second.max_load;
            }
        }
    }

    std::unordered_map<LinkId, std::vector<double>, LinkIdHash> rem_map;
    for (const auto &link_problem : link_list) {
        rem_map[link_problem->id()] = std::vector<double>(max_length(), capacity());
    }

    std::vector<int> presence_map(max_length(), 0);
    for (const auto &[start, end] : bad_ranges) {
        const int clamped_start = std::max(0, start);
        const int clamped_end = std::min(end, max_length() - 1);
        for (int t = clamped_start; t < clamped_end; ++t) {
            presence_map[t] += 1;
        }
    }

    for (int t = 0; t < max_length(); ++t) {
        if (presence_map[t] > 1) {
            for (auto &entry : rem_map) {
                auto &signal = entry.second;
                if (t < static_cast<int>(signal.size())) {
                    signal[t] -= static_cast<double>(presence_map[t] - 1);
                    signal[t] = std::max(1.0, signal[t]);
                }
            }
        }
    }

    std::unordered_map<int, int> service_attained;
    std::unordered_map<int, int> current_iters;
    std::unordered_set<int> not_done_jobs;

    for (int job_id : job_ids) {
        service_attained[job_id] = 0;
        current_iters[job_id] = 0;
        not_done_jobs.insert(job_id);
    }

    const bool has_context_inflate = run_context_.contains("inflate");
    const double context_inflate = has_context_inflate ? run_context_["inflate"].get<double>() : 1.0;

    while (!not_done_jobs.empty()) {
        auto cmp = [&](int lhs, int rhs) {
            return service_attained[lhs] < service_attained[rhs];
        };
        auto selected_it = std::min_element(not_done_jobs.begin(), not_done_jobs.end(), cmp);
        int job_id = *selected_it;
        const Job &job = job_map().at(job_id);
        int current_iter = current_iters[job_id];

        double best_finish_time = std::numeric_limits<double>::max();
        double best_throttle_rate = 1.0;
        int best_delay = 0;
        int best_active_start = 0;
        int best_active_end = 0;
        bool best_overload = true;
        double best_load_mult = 1.0;

        for (double throttle : candidate_throttles) {
            auto &max_loads = job_max_load[throttle][job_id];
            double max_max_load = 0.0;
            for (const auto &kv : max_loads) {
                max_max_load = std::max(max_max_load, kv.second);
            }

            double load_mult = 1.0;
            double inflate = base_inflate;
            if (has_context_inflate) {
                inflate *= context_inflate;
            }

            bool is_overload = false;
            if (max_max_load > capacity()) {
                is_overload = true;
                if (capacity() > 0.0) {
                    inflate *= std::ceil(max_max_load / capacity());
                    load_mult = capacity() / max_max_load;
                }
            }

            auto active_range = sol.GetJobIterActiveTime(job_id, current_iter, throttle, inflate);
            int active_start = active_range.first;
            int active_end = active_range.second;

            int overlaps = OverlapCount(active_start, active_end, bad_ranges);
            if (overlaps > 0) {
                double factor = 1.0 + overlaps * 0.01 * (5.0 + static_cast<double>(job_id));
                inflate *= factor;
                active_range = sol.GetJobIterActiveTime(job_id, current_iter, throttle, inflate);
                active_start = active_range.first;
                active_end = active_range.second;
            }

            int delay = FindEarliestAvailableTimeAllLinks(active_start,
                                                          active_end,
                                                          rem_map,
                                                          max_loads,
                                                          load_mult);
            double finish_time = static_cast<double>(active_end + delay);

            if (finish_time < best_finish_time || (best_overload && !is_overload)) {
                best_finish_time = finish_time;
                best_throttle_rate = throttle;
                best_delay = delay;
                best_active_start = active_start;
                best_active_end = active_end;
                best_overload = is_overload;
                best_load_mult = load_mult;
            }
        }

        auto &max_loads = job_max_load[best_throttle_rate][job_id];
        for (auto &entry : rem_map) {
            auto &signal = entry.second;
            auto load_it = max_loads.find(entry.first);
            const double use_load = (load_it != max_loads.end()) ? load_it->second : 0.0;
            for (int t = best_active_start + best_delay; t < best_active_end + best_delay &&
                                                         t < static_cast<int>(signal.size());
                 ++t) {
                signal[t] -= use_load * best_load_mult;
            }
        }

        sol.deltas()[job_id][current_iter] = best_delay;
        sol.throttle_rates()[job_id][current_iter] = best_throttle_rate;

        current_iters[job_id] += 1;
        service_attained[job_id] += job.base_period();
        if (current_iters[job_id] >= job.iter_count()) {
            not_done_jobs.erase(job_id);
        }
    }

    return sol;
}

std::unordered_map<int, std::unordered_map<Throttle, std::optional<JobProfile>>> LoadAllJobProfiles(
    const std::vector<json> &jobs,
    const json &run_context);

SchedulerContext ParseInput(const json &input);

json BuildOutput(const std::vector<json> &job_timings,
                 const std::vector<json> &lb_decisions,
                 const json &add_to_context);

json FaridV6Scheduling(const SchedulerContext &ctx);

RoutingResult RouteFlows(const std::vector<json> &jobs,
                         const json &options,
                         const json &run_context,
                         const std::unordered_map<int, Job> &job_map,
                         const std::vector<json> &job_timings) {
    RoutingResult result;

    int servers_per_rack = options.value("ft-server-per-rack", 1);
    int machine_count = options.value("machine-count", 0);
    int num_leaves = (servers_per_rack > 0) ? machine_count / servers_per_rack : machine_count;
    int num_spines = options.value("ft-core-count", 0);
    double link_bandwidth = options.value("link-bandwidth", 0.0);
    int max_subflow_count = options.value("subflows", 1);

    if (num_leaves <= 0) {
        num_leaves = 1;
    }

    std::vector<int> job_ids;
    job_ids.reserve(jobs.size());
    for (const auto &job : jobs) {
        job_ids.push_back(job.value("job_id", 0));
    }

    std::unordered_map<int, std::vector<int>> job_deltas;
    std::unordered_map<int, std::vector<double>> job_throttle_rates;
    for (const auto &timing : job_timings) {
        int job_id = timing.value("job_id", 0);
        if (timing.contains("deltas")) {
            job_deltas[job_id] = timing["deltas"].get<std::vector<int>>();
        }
        if (timing.contains("throttle_rates")) {
            job_throttle_rates[job_id] = timing["throttle_rates"].get<std::vector<double>>();
        }
    }

    std::unordered_map<int, int> job_iterations;
    for (const auto &job : jobs) {
        int job_id = job.value("job_id", 0);
        job_iterations[job_id] = job.value("iter_count", 0);
    }

    std::unordered_map<int, std::vector<int>> job_periods;
    for (const auto &[job_id, job] : job_map) {
        const auto &throttles = job_throttle_rates[job_id];
        std::vector<int> periods;
        periods.reserve(throttles.size());
        for (double throttle : throttles) {
            auto period_it = job.periods().find(throttle);
            int period = (period_it != job.periods().end()) ? period_it->second : job.base_period();
            periods.push_back(period);
        }
        job_periods[job_id] = std::move(periods);
    }

    int routing_time = 0;
    for (int job_id : job_ids) {
        int productive_time = 0;
        if (job_periods.count(job_id)) {
            productive_time = std::accumulate(job_periods[job_id].begin(), job_periods[job_id].end(), 0);
        }
        int delay_time = 0;
        if (job_deltas.count(job_id)) {
            delay_time = std::accumulate(job_deltas[job_id].begin(), job_deltas[job_id].end(), 0);
        }
        routing_time = std::max(routing_time, productive_time + delay_time);
    }
    routing_time = std::max(routing_time, 1);

    RemMatrix rem = InitializeRem(num_leaves, std::max(1, num_spines), link_bandwidth, routing_time);
    UsageMap usage = InitializeUsage(job_ids, num_leaves, std::max(1, num_spines), routing_time);

    auto all_flows = BuildAllFlows(job_map, job_deltas, job_throttle_rates, job_periods, job_iterations);

    std::unordered_map<DecisionKey, std::vector<std::pair<int, double>>, DecisionKeyHash> lb_decisions_map;

    std::string fit_strategy = run_context.value("routing-fit-strategy", std::string("graph-coloring-v7"));
    bool early_return = false;

    ColoringOutcome outcome;
    if (fit_strategy == "graph-coloring-v3") {
        outcome = RouteFlowsGraphColoringV3(all_flows, rem, usage, std::max(1, num_spines), lb_decisions_map);
    } else if (fit_strategy == "graph-coloring-v7") {
        outcome = RouteFlowsGraphColoringV7(all_flows, rem, usage, std::max(1, num_spines), lb_decisions_map,
                                            run_context, max_subflow_count, link_bandwidth, early_return);
    } else {
        throw std::runtime_error("Unknown routing fit strategy: " + fit_strategy);
    }

    result.bad_ranges = std::move(outcome.bad_ranges);
    result.min_affected_time = (outcome.min_affected_time == std::numeric_limits<int>::max())
                                   ? 0
                                   : outcome.min_affected_time;
    result.max_affected_time = outcome.max_affected_time;

    for (const auto &kv : lb_decisions_map) {
        const auto &key = kv.first;
        const auto &spines = kv.second;
        json entry;
        entry["job_id"] = key.job_id;
        entry["flow_id"] = key.flow_id;
        entry["iteration"] = key.iteration;
        entry["spine_count"] = static_cast<int>(spines.size());
        json spine_rates = json::array();
        for (const auto &[spine, ratio] : spines) {
            spine_rates.push_back({spine, ratio});
        }
        entry["spine_rates"] = spine_rates;
        result.lb_decisions.push_back(std::move(entry));
    }

    return result;
}

SchedulerContext ParseInput(const json &input) {
    SchedulerContext ctx;
    if (input.contains("jobs") && input["jobs"].is_array()) {
        for (const auto &job : input["jobs"]) {
            ctx.jobs.push_back(job);
        }
    }
    ctx.options = input.value("options", json::object());
    ctx.run_context = input.value("run_context", json::object());
    ctx.timing_file_path = input.value("timing_file_path", std::string{});
    ctx.routing_file_path = input.value("routing_file_path", std::string{});
    ctx.placement_seed = input.value("placement_seed", 0);
    return ctx;
}

json BuildOutput(const std::vector<json> &job_timings,
                 const std::vector<json> &lb_decisions,
                 const json &add_to_context) {
    json result;
    result["job_timings"] = job_timings;
    result["lb_decisions"] = lb_decisions;
    result["add_to_context"] = add_to_context;
    return result;
}

json FaridV6Scheduling(const SchedulerContext &ctx) {
    std::cerr << "[FaridV6Scheduling] Loading job profiles..." << std::endl;
    auto job_profiles = LoadAllJobProfiles(ctx.jobs, ctx.run_context);

    std::cerr << "[FaridV6Scheduling] Loaded job profiles" << std::endl;    

    std::string timing_scheme = ctx.run_context.value("timing-scheme", std::string("faridv6"));
    std::cerr << "[FaridV6Scheduling] Creating solver with scheme: " << timing_scheme << std::endl;
    LegoV2Solver solver(ctx.jobs, ctx.run_context, ctx.options, std::move(job_profiles), timing_scheme);

    int max_attempts = ctx.run_context.value("farid-rounds", 0);
    int current_round = 0;

    json add_to_context;
    add_to_context["fixing_rounds"] = 0;
    add_to_context["fixed_bad_range_ratio"] = 0.0;
    add_to_context["fixed_bad_range_ratios"] = json::array();
    add_to_context["remaining_bad_range_ratio"] = 0.0;
    add_to_context["remaining_bad_range_ratios"] = json::array();

    bool is_inflation_enabled = ctx.run_context.value("use_inflation", false);

    std::cerr << "[FaridV6Scheduling] Solving initial timing..." << std::endl;
    auto solve_pair = solver.Solve();
    std::vector<json> job_timings = std::move(solve_pair.first);

    std::cerr << "[FaridV6Scheduling] Routing flows..." << std::endl;
    auto routing_result = RouteFlows(ctx.jobs, ctx.options, ctx.run_context, solver.job_map(), job_timings);
    std::vector<std::pair<int, int>> remaining_bad_ranges = SummarizeBadRanges(routing_result.bad_ranges);

    int sim_length = ctx.run_context.value("sim-length", 1);
    auto ratios = GetBadRangeRatioV6(remaining_bad_ranges, {}, sim_length);
    double remaining_bad_range_ratio = ratios.first;
    double fixed_bad_range_ratio = ratios.second;

    add_to_context["fixed_bad_range_ratio"] = fixed_bad_range_ratio;
    add_to_context["fixed_bad_range_ratios"].push_back(fixed_bad_range_ratio);
    add_to_context["remaining_bad_range_ratio"] = remaining_bad_range_ratio;
    add_to_context["remaining_bad_range_ratios"].push_back(remaining_bad_range_ratio);

    std::vector<json> lb_decisions = routing_result.lb_decisions;

    std::cerr << "[FaridV6Scheduling] Initial routing done. Remaining bad ranges: " << remaining_bad_ranges.size() << std::endl;

    if (remaining_bad_ranges.empty() || max_attempts == 0) {
        add_to_context["fixing_rounds"] = 0;
        std::cerr << "[FaridV6Scheduling] No fixing rounds needed." << std::endl;
        return BuildOutput(job_timings, lb_decisions, add_to_context);
    }

    current_round = 1;
    std::vector<std::pair<int, int>> fixed_bad_ranges;
    double inflate_factor = 1.0;

    while (!remaining_bad_ranges.empty() && current_round <= max_attempts) {
        std::cerr << "[FaridV6Scheduling] Fixing round " << current_round << "..." << std::endl;
        if (is_inflation_enabled) {
            double fallback_thresh = ctx.run_context.value("fallback-threshold", 1.0);
            if (remaining_bad_range_ratio > fallback_thresh) {
                inflate_factor += 0.05;
                fixed_bad_ranges.clear();
                std::cerr << "[FaridV6Scheduling] Inflation increased to " << inflate_factor << std::endl;
            } else {
                AppendToBadRanges(fixed_bad_ranges, remaining_bad_ranges);
            }
        } else {
            AppendToBadRanges(fixed_bad_ranges, remaining_bad_ranges);
        }

        std::cerr << "[FaridV6Scheduling] Solving with bad ranges and inflation..." << std::endl;
        auto solve_pair_loop = solver.SolveWithBadRangesAndInflation(fixed_bad_ranges, inflate_factor);
        job_timings = std::move(solve_pair_loop.first);

        std::cerr << "[FaridV6Scheduling] Routing flows after fixing..." << std::endl;
        routing_result = RouteFlows(ctx.jobs, ctx.options, ctx.run_context, solver.job_map(), job_timings);
        remaining_bad_ranges = SummarizeBadRanges(routing_result.bad_ranges);

        ratios = GetBadRangeRatioV6(remaining_bad_ranges, fixed_bad_ranges, sim_length);
        remaining_bad_range_ratio = ratios.first;
        fixed_bad_range_ratio = ratios.second;

        add_to_context["fixed_bad_range_ratio"] = fixed_bad_range_ratio;
        add_to_context["fixed_bad_range_ratios"].push_back(fixed_bad_range_ratio);
        add_to_context["remaining_bad_range_ratio"] = remaining_bad_range_ratio;
        add_to_context["remaining_bad_range_ratios"].push_back(remaining_bad_range_ratio);

        ++current_round;
        add_to_context["fixing_rounds"] = add_to_context.value("fixing_rounds", 0) + 1;
        lb_decisions = routing_result.lb_decisions;

        std::cerr << "[FaridV6Scheduling] End of fixing round. Remaining bad ranges: " << remaining_bad_ranges.size() << std::endl;
    }

    if (remaining_bad_ranges.empty() || !is_inflation_enabled) {
        std::cerr << "[FaridV6Scheduling] All bad ranges fixed or inflation not enabled." << std::endl;
        return BuildOutput(job_timings, lb_decisions, add_to_context);
    }

    std::cerr << "[FaridV6Scheduling] Fallback to zero solution." << std::endl;
    auto zero_timings = solver.GetZeroSolution();
    job_timings = zero_timings;
    routing_result = RouteFlows(ctx.jobs, ctx.options, ctx.run_context, solver.job_map(), job_timings);
    remaining_bad_ranges = routing_result.bad_ranges;
    ratios = GetBadRangeRatioV6(remaining_bad_ranges, {}, sim_length);

    add_to_context["fixing_rounds"] = max_attempts + 1;
    add_to_context["fixed_bad_range_ratio"] = ratios.second;
    add_to_context["fixed_bad_range_ratios"].push_back(ratios.second);
    add_to_context["remaining_bad_range_ratio"] = ratios.first;
    add_to_context["remaining_bad_range_ratios"].push_back(ratios.first);
    lb_decisions = routing_result.lb_decisions;

    std::cerr << "[FaridV6Scheduling] Returning fallback output." << std::endl;
    return BuildOutput(job_timings, lb_decisions, add_to_context);
}

}  // namespace

nlohmann::json RunScheduler(const nlohmann::json &input) {
    SchedulerContext ctx = ParseInput(input);
    json result = FaridV6Scheduling(ctx);

    std::vector<int> job_ids;
    job_ids.reserve(ctx.jobs.size());
    for (const auto &job : ctx.jobs) {
        job_ids.push_back(job.value("job_id", 0));
    }
    std::sort(job_ids.begin(), job_ids.end());
    result["add_to_context"]["job_costs"] = std::vector<double>(job_ids.size(), 0.0);

    if (!ctx.timing_file_path.empty()) {
        std::ofstream timing_file(ctx.timing_file_path);
        timing_file << result["job_timings"].dump(4);
        timing_file.close();
    }
    if (!ctx.routing_file_path.empty()) {
        std::ofstream routing_file(ctx.routing_file_path);
        routing_file << result["lb_decisions"].dump(4);
        routing_file.close();
    }
    return result;
}

}  // namespace scheduler
