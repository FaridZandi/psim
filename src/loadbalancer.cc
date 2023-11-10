#include "loadbalancer.h"
#include "spdlog/spdlog.h"
#include "gcontext.h"

#include <algorithm>
#include <cassert>
#include <iostream>
#include <vector>

using namespace psim;
using namespace std;

LoadBalancer::LoadBalancer(int item_count) {
    assert(item_count > 0);
    this->item_count = item_count;
}

void LoadBalancer::register_link(int lower_item, int upper_item, int dir, Bottleneck* link) {
    if (dir == 1) {
        link_up_map[std::make_pair(lower_item, upper_item)] = link;
    } else {
        link_down_map[std::make_pair(lower_item, upper_item)] = link;
    }
}

void LoadBalancer::add_flow_sizes(std::map<int, std::vector<double>>& src_flow_sizes) {
    return;
}

void LoadBalancer::update_state(Flow* arriving_flow) {
    return;
}

Bottleneck* LoadBalancer::uplink(int lower_item, int upper_item){
    return link_up_map[std::make_pair(lower_item, upper_item)];
}

Bottleneck* LoadBalancer::downlink(int lower_item, int upper_item){
    return link_down_map[std::make_pair(lower_item, upper_item)];
}

LoadBalancer* LoadBalancer::create_load_balancer(int item_count, LBScheme lb_scheme) {
    switch (lb_scheme) {
        case LBScheme::RANDOM:
            return new RandomLoadBalancer(item_count);
        case LBScheme::ROUND_ROBIN:
            return new RoundRobinLoadBalancer(item_count);
        case LBScheme::LEAST_LOADED:
            return new LeastLoadedLoadBalancer(item_count);
        case LBScheme::POWER_OF_K:
            return new PowerOfKLoadBalancer(
                item_count, GConf::inst().lb_samples);
        case LBScheme::ROBIN_HOOD:
            return new RobinHoodLoadBalancer(item_count);
        case LBScheme::FUTURE_LOAD:
            return new FutureLoadLoadBalancer(item_count);
        case LBScheme::SITA_E:
            return new SitaELoadBalancer(item_count);
        default:
            spdlog::error("Invalid load balancer type: {}", int(lb_scheme));
            exit(1);
    }
}

std::pair<int, int> psim::get_prof_limits(double start_time, double end_time) {
    int prof_interval = GConf::inst().core_status_profiling_interval;

    int prof_start = int(start_time);
    int prof_end = int(end_time);

    if (prof_start % prof_interval != 0) {
        prof_start = (prof_start / prof_interval + 1) * prof_interval;
    }

    if (prof_end % prof_interval != 0) {
        prof_end = (prof_end / prof_interval) * prof_interval;
    }

    return std::make_pair(prof_start, prof_end);
}


/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////


RandomLoadBalancer::RandomLoadBalancer(int item_count) : LoadBalancer(item_count) {}

int RandomLoadBalancer::get_upper_item(int src, int dst, Flow* flow, int timer) {
    int upper_item = rand() % item_count;
    return upper_item;
}

/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////


RoundRobinLoadBalancer::RoundRobinLoadBalancer(int item_count) : LoadBalancer(item_count) {
    current_upper_item = 0;
}

int RoundRobinLoadBalancer::get_upper_item(int src, int dst, Flow* flow, int timer) {
    int upper_item = current_upper_item;
    current_upper_item = (current_upper_item + 1) % item_count;
    return upper_item;
}

/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////


PowerOfKLoadBalancer::PowerOfKLoadBalancer(int item_count, int samples)
    : LoadBalancer(item_count), num_samples(std::min(samples, item_count)) {
    // Initially no item is loaded so they're all the best. We randomly denote
    // item 0 as the best.
    prev_best_item = 0;
}

int PowerOfKLoadBalancer::get_upper_item(int src, int dst, Flow* flow, int timer) {
    double least_load = std::numeric_limits<double>::max();

    // We sample num_samples random items and the previously least loaded item
    // and pick the least loaded among these. This is what DRILL does but at
    // the packet level.
    std::vector<int> items_to_sample(item_count);
    for (size_t idx = 0; idx < item_count; idx++) {
        items_to_sample[idx] = idx;
    }
    std::random_shuffle(items_to_sample.begin(), items_to_sample.end());
    int num_to_check = num_samples;
    if (num_samples < item_count) {
        items_to_sample[num_samples] = prev_best_item;
        num_to_check++;
    }

    for (size_t idx = 0; idx < num_to_check; idx++) {
        double up_load = uplink(src, items_to_sample[idx])->get_load();
        double down_load = downlink(dst, items_to_sample[idx])->get_load();
        double load = up_load + down_load;

        if (load < least_load){
            least_load = load;
            prev_best_item = items_to_sample[idx];
        }
    }

    return prev_best_item;
}


/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////


LeastLoadedLoadBalancer::LeastLoadedLoadBalancer(int item_count) : LoadBalancer(item_count) {}

int LeastLoadedLoadBalancer::get_upper_item(int src, int dst, Flow* flow, int timer) {
    int best_core = -1;
    double least_load = std::numeric_limits<double>::max();

    for (int c = 0; c < item_count; c++){
        double up_load = uplink(src, c)->get_load();
        double down_load = downlink(dst, c)->get_load();
        double load = up_load + down_load;

        if (load < least_load){
            least_load = load;
            best_core = c;
        }
    }

    return best_core;
}

/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////

RobinHoodLoadBalancer::RobinHoodLoadBalancer(int item_count)
    : LoadBalancer(item_count), multiplier(0.0) {
    iterations_hard_working;
    // Initially the lower bound on the optimal is 0.
    lb = 0.0;
}

void RobinHoodLoadBalancer::register_link(int lower_item, int upper_item, int dir, Bottleneck* link) {
    LoadBalancer::register_link(lower_item, upper_item, dir, link);
    iterations_hard_working[link] = 0;
}

void RobinHoodLoadBalancer::update_state(Flow* arriving_flow) {
    double flow_load = (arriving_flow == nullptr) ? 0 : arriving_flow->get_load();

    // Calculate total load on all links.
    load = flow_load;
    for (auto itr = link_up_map.begin(); itr != link_up_map.end(); itr++) {
        load += itr->second->get_load();
    }
    for (auto itr = link_down_map.begin(); itr != link_down_map.end(); itr++) {
        load += itr->second->get_load();
    }

    // Update the lower bound. Note that iterations_hard_working has all links.
    lb = std::max(flow_load, load / iterations_hard_working.size());
    double cutoff = get_multiplier() * lb;

    // Update the number of consecutive iterations each link has been
    // hard-working.
    for (auto itr = iterations_hard_working.begin();
         itr != iterations_hard_working.end();
         itr++) {
        if (itr->first->get_load() < cutoff) {
            // It is now not hard-working, so it has been hard-working for 0
            // consecutive iterations.
            itr->second = 0;
        } else {
            // It is hard-working, so we increment the number of consecutive
            // iterations for which it has been hard-working.
            itr->second++;
        }
    }
}

int RobinHoodLoadBalancer::get_upper_item(int src, int dst, Flow* flow, int timer) {
    update_state(flow);

    std::vector<int> non_hard_working_items;

    // The item that most recently became hard working (used in the case
    // that all items are hard working).
    // Here by latest we mean the least number of consecutive iterations
    // for which it has been hard working.
    int latest_hard_working_item = -1;
    int latest_hard_working_iterations = std::numeric_limits<int>::max();

    int item_hard_working_iterations;
    for (int c = 0; c < item_count; c++) {
        item_hard_working_iterations = std::max(
            iterations_hard_working[uplink(src, c)],
            iterations_hard_working[downlink(src, c)]);
        if (item_hard_working_iterations == 0) {
            non_hard_working_items.push_back(c);
        } else {
            if (item_hard_working_iterations < latest_hard_working_iterations) {
                latest_hard_working_item = c;
                latest_hard_working_iterations = item_hard_working_iterations;
            }
        }
    }

    if (non_hard_working_items.size() > 0) {
        // We have some non hard working items so we pick one at random.
        int idx = rand() % non_hard_working_items.size();
        return non_hard_working_items[idx];
    }

    // Otherwise, all items are hardworking and we return the one that most
    // recently became hard working.
    return latest_hard_working_item;
}

double RobinHoodLoadBalancer::get_multiplier() {
    if (multiplier <= 0) {
        multiplier = std::sqrt((link_up_map.size() + link_down_map.size()));
    }
    return multiplier;
}

/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////

FutureLoadLoadBalancer::FutureLoadLoadBalancer(int item_count) : LoadBalancer(item_count) {
    current_upper_item = 0;
}

int FutureLoadLoadBalancer::my_round_robin() {
    int upper_item = current_upper_item;
    current_upper_item = (current_upper_item + 1) % item_count;
    return upper_item;
}

double FutureLoadLoadBalancer::get_flow_load_estimate(Flow* flow){
    switch (GConf::inst().load_metric) {
        case LoadMetric::FLOWCOUNT:
            return 1;

        case LoadMetric::UTILIZATION:
        case LoadMetric::ALLOCATED:
        case LoadMetric::REGISTERED: {
            auto& last_run = GContext::last_run();
            double last_flow_fct = last_run.flow_fct[flow->id];
            return flow->size / last_flow_fct;
        }
        case LoadMetric::FLOWSIZE:
            return flow->size;

        default:
            spdlog::error("Invalid load metric type: {}", int(GConf::inst().load_metric));
            exit(1);
    }
}

void FutureLoadLoadBalancer::remove_flow_from_last_run_data(Flow* flow, int src, int dst,
                                                            std::pair<int, int> last_run_prof) {

    double prof_interval = GConf::inst().core_status_profiling_interval;

    auto& last_run = GContext::last_run();
    int last_decision = GContext::last_decision(flow->id);

    // last_run_prof is the start and end point of the time interval for the
    // previous run so this loops from the start to the end of the last run time
    // interval taking prof_interval size steps.
    for (int t = last_run_prof.first + prof_interval; t <= last_run_prof.second; t += prof_interval)  {
        if (t > last_run.max_time_step) {
            spdlog::error("t: {} > last_run.max_time_step: {}", t, last_run.max_time_step);
            break;
        }

        auto& status = last_run.network_status[t];
        auto& flow_loads = status.flow_loads;

        if (flow_loads.find(flow->id) == flow_loads.end()) {
            spdlog::error("flow load not found flow: {}, t: {}, last_run.max_time_step: {}, last_decision: {}",
                          flow->id, t, last_run.max_time_step, last_decision);
            exit(1);
        }

        // Removing this flow from the links it traversed in the run. The
        // result will show the loads in the network minus this flow.
        double flow_load = flow_loads[flow->id];
        auto& link_loads = status.link_loads;
        link_loads[uplink(src, last_decision)->id] -= flow_load;
        link_loads[downlink(dst, last_decision)->id] -= flow_load;
    }
}

void FutureLoadLoadBalancer::add_flow_load_to_last_run_data(Flow* flow, int src, int dst,
                                                            std::pair<int, int> this_run_prof,
                                                            double flow_load_estimate,
                                                            int best_core) {

    double prof_interval = GConf::inst().core_status_profiling_interval;
    auto& last_run = GContext::last_run();

    // We loop from the start to the end of the last run and add the estimate of
    // the flow load to the links it traversed in this previous iteration.
    for (int t = this_run_prof.first; t <= this_run_prof.second; t += prof_interval)  {
        if (t > last_run.max_time_step) break;
        auto& link_loads = last_run.network_status[t].link_loads;
        link_loads[uplink(src, best_core)->id] += flow_load_estimate;
        link_loads[downlink(dst, best_core)->id] += flow_load_estimate;
    }
}


// Estimate of loads to each core for the (src, dst) pair from the last run.
std::vector<double>
FutureLoadLoadBalancer::get_core_loads_estimate(Flow* flow, int src, int dst,
                                                std::pair<int, int> this_run_prof) {

    std::vector<double> core_loads(item_count, 0.0);


    double prof_interval = GConf::inst().core_status_profiling_interval;
    auto& last_run = GContext::last_run();

    for (int t = this_run_prof.first; t <= this_run_prof.second; t += prof_interval)  {
        if (t > last_run.max_time_step) {
            break;
        }

        auto& link_loads = last_run.network_status[t].link_loads;

        for (int c = 0; c < item_count; c++) {
            double up_load = link_loads[uplink(src, c)->id];
            double down_load = link_loads[downlink(dst, c)->id];
            core_loads[c] += (up_load + down_load);
        }
    }

    std::string load_string = "";
    for (int c = 0; c < item_count; c++) {
        load_string += std::to_string(core_loads[c]) + ", ";
    }

    spdlog::debug("flow: {}, core load: {}", flow->id, load_string);

    return core_loads;
}

int find_least_loaded_core(std::vector<double> core_loads) {
    int best_core = -1;
    double least_load = std::numeric_limits<double>::max();

    for (int c = 0; c < core_loads.size(); c++) {
        if (core_loads[c] < least_load) {
            least_load = core_loads[c];
            best_core = c;
        }
    }
    return best_core;
}


int FutureLoadLoadBalancer::get_upper_item(int src, int dst, Flow* flow, int timer) {
    if (GContext::is_first_run()) {
        return my_round_robin();

    } else {
        int last_decision = GContext::last_decision(flow->id);
        auto& last_run = GContext::last_run();

        // get the info on the flow from the last run
        double last_flow_fct = last_run.flow_fct[flow->id];
        double last_flow_start = last_run.flow_start[flow->id];
        double last_flow_end = last_run.flow_end[flow->id];

        // the most crude estimate of the flow_completion_time,
        // is that it will take the same time as the last run
        // which is clearly not true, but it's a start.
        // this should definitely be improved in the future.
        double flow_finish_estimate = timer + last_flow_fct;

        // get the profiling bounds for the last run and this run's estimate.
        auto this_run_prof = get_prof_limits(timer, timer + last_flow_fct);
        auto last_run_prof = get_prof_limits(last_flow_start, last_flow_end);

        // remove the flow from the last run data. We would like to see the network
        // load without this flow. Otherwise we would be double counting the load
        // of this flow.
        remove_flow_from_last_run_data(flow, src, dst, last_run_prof);

        // get the core loads estimate for this run.
        // We've already removed the flow from the estimates of the last run so
        // this call gives estimates of load to each core between src and dst
        // from the last run (minus the flow).
        auto core_loads = get_core_loads_estimate(flow, src, dst, this_run_prof);
        // pick the least loaded from the load estimates.
        int best_core = find_least_loaded_core(core_loads);

        // now that we have made our decision, we add the flow load to the last run data
        // such that we can use it for the next flow. However, this is not the actual
        // flow load, but the estimate.
        double flow_rate_estimate = get_flow_load_estimate(flow);
        add_flow_load_to_last_run_data(flow, src, dst, this_run_prof, flow_rate_estimate, best_core);

        return best_core;
    }
}

/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////


SitaELoadBalancer::SitaELoadBalancer(int item_count) : LoadBalancer(item_count) {
    current_upper_item = 0;
}

void SitaELoadBalancer::add_flow_sizes(std::map<int, std::vector<double>>& src_flow_sizes) {
    if (src_flow_sizes.size() == 0) {
        return;
    }
    auto itr = src_flow_sizes.begin();
    for (; itr != src_flow_sizes.end(); itr++) {
        // We keep a vector of thresholds for which upper item to send a flow
        // to given the flows src. The thresholds are such that if for a given
        // index i we have that the flow size is greater than the threshold
        // for upper item i - 1 but at most the threshold for upper item i then
        // the flow is sent to upper item i.
        src_size_thresholds[itr->first] = std::vector<double>(item_count, -1.0);
        src_size_thresholds[itr->first][item_count - 1] = std::numeric_limits<double>::max();

        // The goal of SITA-E is to assign equal load to each item. So we
        // compute the cumulative flow sizes after sorting the flows in
        // ascending order. So for example if the flow sizes are
        // [10, 11, 11, 12, 13, 13, 13, 14, 15, 100] then the cumulative sizes
        // are [10, 21, 32, 44, 57, 70, 83, 97, 112, 212].
        std::sort(itr->second.begin(), itr->second.end());
        long double cumulative_size = 0;
        std::vector<long double> cumulative_flow_sizes(itr->second.size(), 0);
        for (size_t idx = 0; idx < itr->second.size(); idx++) {
            cumulative_size += itr->second[idx];
            cumulative_flow_sizes[idx] = cumulative_size;
        }

        // We now determine the cutoff points for the various items. For example
        // if we have flow sizes [10, 11, 11, 12, 13, 13, 13, 14, 15, 100] and
        // cumulative sizes [10, 21, 32, 44, 57, 70, 83, 97, 112, 212] then the
        // total traffic has size 212 so we want about 212/5 = 42.4 to go to
        // each item (we bias downwards as datacenter traffic is usually heavy
        // tailed so there are a few large flows that take up many bytes and
        // we don't want to have 2 items for a range that only contains one
        // flow).
        // So the first threshold is for cumulative size < 1 * 42.4 and so flows
        // at or below size 11 go to the first item. Flows where the cumulative
        // size is at least 1 * 42.4 and < 2 * 42.4 go to the second item. This
        // cutoff is 13 and so on gives thresholds [11, 13, 15, 15, inf]. Note
        // that we don't update the last threshold, this just stays as the
        // maximum double.
        long double step_size = cumulative_size / item_count;
        long double curr_cutoff = step_size;
        int curr_item = 0;
        size_t curr_idx = 0;
        while (curr_cutoff < cumulative_size &&
               curr_idx < itr->second.size() - 1 &&
               curr_item < item_count - 1) {
            if (cumulative_flow_sizes[curr_idx + 1] <= curr_cutoff) {
                curr_idx++;
            } else {
                src_size_thresholds[itr->first][curr_item] = itr->second[curr_idx];
                std::cout << "Setting threshold for " << curr_item << " at ";
                std::cout << itr->first << " to " << itr->second[curr_idx];
                std::cout << std::endl;
                curr_item++;
                curr_cutoff += step_size;
            }
        }
    }
}

int SitaELoadBalancer::get_upper_item(int src, int dst, Flow* flow, int timer) {
    if (src_size_thresholds.find(src) == src_size_thresholds.end()) {
        // If we don't know the flow distribution, just do round robin.
        int upper_item = current_upper_item;
        current_upper_item = (current_upper_item + 1) % item_count;
        return upper_item;
    }
    // TODO: optimize this to use binary search.
    for (int idx = 0; idx < item_count - 1; idx++) {
        if (flow->size <= src_size_thresholds[src][idx]) {
            int min_item = idx;
            idx++;
            // Note that we could have multiple items with the same size
            // threshold, for example if the size thresholds are
            // [11, 13, 15, 15, inf] then a flow of size 14 can go to item 2 or
            // 3 so we want to pick which of these two at random.
            while (idx < item_count &&
                   src_size_thresholds[src][idx-1] <= flow->size &&
                   src_size_thresholds[src][idx] >= flow->size) {
                idx++;
            }
            int best_item = -1;
            double least_load = std::numeric_limits<double>::max();

            for (int c = min_item; c < idx; c++) {
                double up_load = uplink(src, c)->get_load();
                double down_load = downlink(dst, c)->get_load();
                double load = up_load + down_load;

                if (load < least_load) {
                    least_load = load;
                    best_item = c;
                }
            }

            return best_item;
        }
    }
    return item_count - 1;
}
