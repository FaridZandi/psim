#include "loadbalancer.h"
#include "spdlog/spdlog.h"
#include "gcontext.h"

#include <algorithm>
#include <vector>

using namespace psim;
using namespace std;

LoadBalancer::LoadBalancer(int item_count) {
    this->item_count = item_count;
}

void LoadBalancer::register_link(int lower_item, int upper_item, int dir, Bottleneck* link) {
    if (dir == 1) {
        link_up_map[std::make_pair(lower_item, upper_item)] = link;
    } else {
        link_down_map[std::make_pair(lower_item, upper_item)] = link;
    }
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
        default:
            spdlog::error("Invalid load balancer type: {}", int(lb_scheme));
            exit(1);
    }
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
    lb = std::max(lb,
                  std::max(flow_load, load / iterations_hard_working.size()));
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

int FutureLoadLoadBalancer::get_upper_item(int src, int dst, Flow* flow, int timer) {
    if (GContext::is_first_run()) {
        return my_round_robin();
    } else {
        int last_decision = GContext::last_decision(flow->id);
        auto& last_run = GContext::last_run();

        // if (timer > GContext::inst().cut_off_time) {
        //     return last_decision;
        // }

        int prof_inter = GConf::inst().core_status_profiling_interval;

        double last_flow_fct = last_run.flow_fct[flow->id];
        double last_flow_start = last_run.flow_start[flow->id];
        double last_flow_end = last_run.flow_end[flow->id];

        double last_flow_rate = 0;

        switch (GConf::inst().load_metric) {
            case LoadMetric::FLOWCOUNT:
                last_flow_rate = 1.0;
                break;
            case LoadMetric::UTILIZATION:
                last_flow_rate = flow->size / last_flow_fct;
                break;
            case LoadMetric::FLOWSIZE:
                last_flow_rate = flow->size;
                break;
            default:
                exit(1);
        }

        double flow_finish_estimate = timer + last_flow_fct;

        auto this_run_prof = get_prof_limits(timer, timer + last_flow_fct);
        auto last_run_prof = get_prof_limits(last_flow_start, last_flow_end);

        std::vector<double> core_load(item_count, 0.0);

        bool no_profiling_found = true;
        for (int t = this_run_prof.first; t <= this_run_prof.second; t += prof_inter)  {

            if (t > last_run.max_time_step) break;

            no_profiling_found = false;

            auto& status_map = last_run.network_status;
            auto& link_loads = status_map[t].link_loads;
            auto& flow_loads = status_map[t].flow_loads;

            for (int c = 0; c < item_count; c++) {
                double up_load = link_loads[uplink(src, c)->id];
                double down_load = link_loads[downlink(dst, c)->id];
                double total_rate = up_load + down_load;

                if (c == last_decision) {
                    if (t > last_run_prof.first and t <= last_run_prof.second) {
                        // if we are looking at the previous run for the same core
                        // during the time that the flow was running, then we need to
                        // subtract the flow rate from the total rate, since this core
                        // current contains thet flow that we're trying to place.

                        if (flow_loads.find(flow->id) == flow_loads.end()){
                            spdlog::error("flow load not found flow: {}.", flow->id);
                        }

                        total_rate -= 2 * flow_loads[flow->id];
                    }
                }
                core_load[c] += total_rate;
            }
        }

        std::string load_string = "";
        for (int c = 0; c < item_count; c++) {
            load_string += std::to_string(core_load[c]) + ", ";
        }
        spdlog::debug("core load: {}", load_string);

        if (no_profiling_found) {
            return my_round_robin();
        }

        int best_core = -1;
        double least_load = std::numeric_limits<double>::max();
        for (int c = 0; c < item_count; c++){
            if (core_load[c] < least_load){
                least_load = core_load[c];
                best_core = c;
            }
        }
        GContext::this_run().least_load[flow->id] = least_load / last_flow_fct;

        spdlog::debug("last decision: {}, this decision: {}", last_decision, best_core);
        spdlog::debug("-----------------------------------------------------------------");

        // todo: can I get a better estimate of the flow finishing time now?

        auto last_run_bn_up = uplink(src, last_decision);
        auto last_run_bn_down = downlink(dst, last_decision);

        auto this_run_bn_up = uplink(src, best_core);
        auto this_run_bn_down = downlink(dst, best_core);

        // update the link loads in the last run, to account for the different decision.
        for (int t = last_run_prof.first + prof_inter; t <= last_run_prof.second; t += prof_inter)  {
            if (t > last_run.max_time_step) break;

            auto& status = last_run.network_status[t];
            auto& link_loads = status.link_loads;

            if (status.flow_loads.find(flow->id) == status.flow_loads.end()) {
                spdlog::error("flow load not found flow: {}.", flow->id);
            }

            double flow_load = status.flow_loads[flow->id];

            link_loads[last_run_bn_up->id] -= flow_load;
            link_loads[last_run_bn_down->id] -= flow_load;
        }

        for (int t = this_run_prof.first; t <= this_run_prof.second; t += prof_inter)  {
            if (t > last_run.max_time_step) break;
            auto& status = last_run.network_status[t];
            auto& link_loads = status.link_loads;

            link_loads[this_run_bn_up->id] += last_flow_rate;
            link_loads[this_run_bn_down->id] += last_flow_rate;
        }

        return best_core;
    }
}
