#include "loadbalancer.h"
#include "spdlog/spdlog.h"
#include "gcontext.h"

using namespace psim;
using namespace std; 

LoadBalancer::LoadBalancer(int item_count) {
    this->item_count = item_count; 

    std::string load_metric_str = GConf::inst().load_metric;

    if (load_metric_str == "register") {
        this->load_metric = LoadMetric::REGISTER;
    } else if (load_metric_str == "utilization") {
        this->load_metric = LoadMetric::UTILIZATION;
    } else if (load_metric_str == "allocated") {
        this->load_metric = LoadMetric::ALLOCATED;
    } else {
        spdlog::error("Invalid load metric: {}", load_metric_str);
        exit(1);
    }
}

void LoadBalancer::register_link(int lower_item, int upper_item, int dir, Bottleneck* link) {
    if (dir == 1) {
        link_up[std::make_pair(lower_item, upper_item)] = link; 
    } else {
        link_down[std::make_pair(lower_item, upper_item)] = link; 
    }
}

double LoadBalancer::get_bottleneck_load(Bottleneck* bn) {
    LoadMetric load_metric = this->load_metric;
    
    if (this->load_metric == LoadMetric::REGISTER) {
        return bn->bwalloc->total_registered;
    } else if (this->load_metric == LoadMetric::UTILIZATION) {
        return bn->bwalloc->utilized_bandwidth;
    } else if (this->load_metric == LoadMetric::ALLOCATED) {
        return bn->bwalloc->total_allocated;
    } else {
        spdlog::error("Invalid load metric");
        exit(1);
    }
}

LoadBalancer* LoadBalancer::create_load_balancer(std::string type, int item_count, core_selection& cs){
    if (type == "random") {
        cs = core_selection::RANDOM;
        return new RandomLoadBalancer(item_count);

    } else if (type == "roundrobin") {
        cs = core_selection::ROUND_ROBIN;
        return new RoundRobinLoadBalancer(item_count);

    } else if (type == "powerof2") {
        cs = core_selection::POWER_OF_2;
        return new PowerOf2LoadBalancer(item_count);

    } else if (type == "leastloaded") {
        cs = core_selection::LEAST_LOADED;
        return new LeastLoadedLoadBalancer(item_count);

    } else if (type == "robinhood") {
        cs = core_selection::ROBIN_HOOD; 
        return new RobinHoodLoadBalancer(item_count);
 
    } else if (type == "futureload") {
        cs = core_selection::FUTURE_LOAD;
        return new FutureLoadLoadBalancer(item_count);
 
    } else {
        spdlog::error("Invalid load balancer type: {}", type);
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


PowerOf2LoadBalancer::PowerOf2LoadBalancer(int item_count) : LoadBalancer(item_count) {}

int PowerOf2LoadBalancer::get_upper_item(int src, int dst, Flow* flow, int timer) {
    // Initially no core is loaded so they're all best. We randomly denote
    // core 0 as the best.
    static int prev_best_core = 0;
    double least_load = std::numeric_limits<double>::max();

    // We sample 2 random cores and the previously least loaded core and
    // pick the least loaded among these. This is what DRILL does but at
    // the packet level.
    int cores_to_sample[3];
    cores_to_sample[0] = rand() % item_count;
    int r;
    do {
        r = rand() % item_count;
    } while (r == cores_to_sample[0]);
    
    cores_to_sample[1] = r;
    cores_to_sample[2] = prev_best_core;

    for (int c : cores_to_sample) {
        auto up = link_up[make_pair(src, c)];
        auto down = link_down[make_pair(dst, c)];

        double load = get_bottleneck_load(up) + get_bottleneck_load(down);

        if (load < least_load){
            least_load = load;
            prev_best_core = c;
        }
    }

    return prev_best_core;
}


/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////


LeastLoadedLoadBalancer::LeastLoadedLoadBalancer(int item_count) : LoadBalancer(item_count) {}

int LeastLoadedLoadBalancer::get_upper_item(int src, int dst, Flow* flow, int timer) {
    int best_core = -1;
    double least_load = std::numeric_limits<double>::max();

    for (int c = 0; c < item_count; c++){

        auto up = link_up[make_pair(src, c)];
        auto down = link_down[make_pair(dst, c)];
        double load = get_bottleneck_load(up) + get_bottleneck_load(down);

        if (load < least_load){
            least_load = load;
            best_core = c;
        }
    }

    spdlog::critical("Least loaded core: {} with the load: {}", best_core, least_load);
    
    return best_core;
}

/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////

RobinHoodLoadBalancer::RobinHoodLoadBalancer(int item_count) : LoadBalancer(item_count) {
    iterations_hard_working = std::vector<int>(item_count, 0);

    rh_multiplier = std::sqrt(item_count);

    rh_lb = 0.0;
}


int RobinHoodLoadBalancer::get_upper_item(int src, int dst, Flow* flow, int timer) { 
    double total_load = 0.0;
    std::vector<double> core_loads(item_count, 0.0);

    for (int c = 0; c < item_count; c++) {
        auto up = link_up[make_pair(src, c)];
        auto down = link_down[make_pair(dst, c)];
        core_loads[c] = get_bottleneck_load(up) + get_bottleneck_load(down);
        total_load += core_loads[c];
    }

    rh_lb = std::max(rh_lb, total_load / item_count);
    double rh_cutoff = rh_multiplier * rh_lb;
    std::vector<int> non_hard_working_cores;

    // The core that most recently became hard working (used in the case
    // that all cores are hard working).
    // Here by latest we mean the least number of consecutive iterations
    // for which it has been hard working.
    int latest_hard_working_core = -1;
    int latest_hard_working_iterations = std::numeric_limits<int>::max();

    for (int c = 0; c < item_count; c++) {
        if (core_loads[c] < rh_cutoff) {
            // It is now 0 iterations since it was last non hard working.
            core_loads[c] = 0;
            non_hard_working_cores.push_back(c);
        } else {
            iterations_hard_working[c]++;
            if (iterations_hard_working[c] < latest_hard_working_iterations) {
                latest_hard_working_core = c;
                latest_hard_working_iterations = iterations_hard_working[c];
            }
        }
    }

    if (non_hard_working_cores.size() > 0) {
        // We have some non hard working cores so we pick one at random.
        int idx = rand() % non_hard_working_cores.size();
        return non_hard_working_cores[idx];
    }
    
    // Otherwise, all cores are hardworking and we return the one that most
    // recently became hard working.
    return latest_hard_working_core;
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

        double last_flow_rate = flow->size / last_flow_fct;
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

                auto up = link_up[make_pair(src, c)];
                auto down = link_down[make_pair(dst, c)];

                double total_rate = link_loads[up->id] + link_loads[down->id];

                if (c == last_decision) {
                    if (t > last_run_prof.first and t <= last_run_prof.second) {
                        // if we are looking at the previous run for the same core
                        // during the time that the flow was running, then we need to
                        // subtract the flow rate from the total rate, since this core
                        // current contains thet flow that we're trying to place.

                        if (flow_loads.find(flow->id) == flow_loads.end()){
                            spdlog::error("flow load not found flow: {}, t: {}, last_start: {}, last_finish: {}, prof_start: {}, prof_end: {}",
                                            flow->id, t, last_flow_start, last_flow_end, last_run_prof.first, last_run_prof.second);
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

        
        auto last_run_bn_up = link_up[make_pair(src, last_decision)];
        auto last_run_bn_down = link_down[make_pair(dst, last_decision)];
        
        auto this_run_bn_up = link_up[make_pair(src, best_core)];
        auto this_run_bn_down = link_down[make_pair(dst, best_core)];

        // update the link loads in the last run, to account for the different decision.
        for (int t = last_run_prof.first + prof_inter; t <= last_run_prof.second; t += prof_inter)  {
            if (t > last_run.max_time_step) break;

            auto& status = last_run.network_status[t];
            auto& link_loads = status.link_loads;

            if (status.flow_loads.find(flow->id) == status.flow_loads.end()){
                spdlog::error("flow load not found flow: {}, t: {}, last_start: {}, last_finish: {}, prof_start: {}, prof_end: {}",
                    flow->id, t, last_flow_start, last_flow_end, last_run_prof.first, last_run_prof.second);
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