
#include <iostream>
#include <cassert>
#include <algorithm>
#include <limits>
#include <set> 

#include "network.h"
#include "config.h"
#include "context.h"
#include "spdlog/spdlog.h"

using namespace psim; 

FatTreeNetwork::FatTreeNetwork() : Network() {
    int link_bandwidth = GConf::inst().link_bandwidth;

    server_count = GConf::inst().machine_count;
    server_per_rack = GConf::inst().ft_server_per_rack; 
    rack_per_pod = GConf::inst().ft_rack_per_pod;
    agg_per_pod = GConf::inst().ft_agg_per_pod;
    pod_count = GConf::inst().ft_pod_count;
    core_count = GConf::inst().ft_core_count;

    server_tor_link_capacity = GConf::inst().ft_server_tor_link_capacity_mult * link_bandwidth; 
    tor_agg_link_capacity = GConf::inst().ft_tor_agg_link_capacity_mult * link_bandwidth; 
    agg_core_link_capacity = GConf::inst().ft_agg_core_link_capacity_mult * link_bandwidth;

    if (server_count != server_per_rack * rack_per_pod * pod_count) {
        spdlog::error("server_count must be equal to server_per_rack * rack_per_pod * pod_count");
        exit(1);
    }

    if (core_count % agg_per_pod != 0) {
        spdlog::error("core_count must be divisible by agg_per_pod");
        exit(1);
    }

    core_selection cs; 
    auto mech = GConf::inst().core_selection_mechanism;
    if (mech == "roundrobin") {
        cs = core_selection::ROUND_ROBIN;
    } else if (mech == "random") {
        cs = core_selection::RANDOM;
    } else if (mech == "leastloaded") {
        cs = core_selection::LEAST_LOADED;
    } else if (mech == "futureload") {
        cs = core_selection::FUTURE_LOAD;
    } else {
        spdlog::critical("core selection mechanism {} not supported.", mech);
        exit(1);
    }
    
    core_selection_mechanism = cs;


    core_link_per_agg = core_count / agg_per_pod;

    for (int i = 0; i < pod_count; i++) {
        for (int j = 0; j < rack_per_pod; j++) {
            for (int k = 0; k < server_per_rack; k++) {
                int machine_num = i * rack_per_pod * server_per_rack + j * server_per_rack + k;
                Machine *machine = get_machine(machine_num);
                server_loc_map[machine_num] = ft_loc{i, j, k, -1, -1};

                Bottleneck *bn_up = create_bottleneck(server_tor_link_capacity);
                server_tor_bottlenecks[ft_loc{i, j, k, 1, -1}] = bn_up;

                Bottleneck *bn_down = create_bottleneck(server_tor_link_capacity);
                server_tor_bottlenecks[ft_loc{i, j, k, 2, -1}] = bn_down;
            }
        }

        for (int j = 0; j < rack_per_pod; j++){
            for (int k = 0; k < agg_per_pod; k++) {
                Bottleneck *bn_up = create_bottleneck(tor_agg_link_capacity);
                tor_agg_bottlenecks[ft_loc{i, j, k, 1, -1}] = bn_up;

                Bottleneck *bn_down = create_bottleneck(tor_agg_link_capacity);
                tor_agg_bottlenecks[ft_loc{i, j, k, 2, -1}] = bn_down;
            }
        }

        for (int c = 0; c < core_count; c++) {
            int agg_num = c / (core_link_per_agg);

            Bottleneck *bn_up = create_bottleneck(agg_core_link_capacity);
            pod_core_bottlenecks[ft_loc{i, -1, -1, 1, c}] = bn_up;

            Bottleneck *bn_down = create_bottleneck(agg_core_link_capacity);
            pod_core_bottlenecks[ft_loc{i, -1, -1, 2, c}] = bn_down;

            pod_core_agg_map[ft_loc{i, -1, -1, -1, c}] = agg_num;
        }
    }

    last_agg_in_pod = new int[pod_count];
    for (int i = 0; i < pod_count; i++) {
        last_agg_in_pod[i] = 0;
    }
}

FatTreeNetwork::~FatTreeNetwork() {
    
}

void FatTreeNetwork::record_core_link_status(double timer) {

    if (core_selection_mechanism != core_selection::FUTURE_LOAD) {
        return; 
    }

    auto& curr_run_info = GContext::inst().run_info_list.back();

    int timer_int = int(timer);

    auto& status = curr_run_info.network_status[timer_int]; 
    status.time = timer_int;
    
    // status = core_link_status();

    auto& link_loads = status.link_loads;
    for (auto bn: bottlenecks) {
        link_loads[bn->id] = get_bottleneck_load(bn);
    }

    auto& flow_loads = status.flow_loads;
    for (auto flow: flows) {
        flow_loads[flow->id] = flow->last_rate;
    }

    curr_run_info.max_time_step = timer_int;
}


int FatTreeNetwork::select_agg(Flow* flow, int pod_number, core_selection mechanism) {

    if(mechanism == core_selection::FUTURE_LOAD){
        if (GContext::is_first_run()) {
            return select_agg(flow, pod_number, core_selection::ROUND_ROBIN);
        } 
        int last_decision = GContext::inst().last_decision(flow->id);
        return last_decision;
    } else {
        int agg_num = last_agg_in_pod[pod_number];
        last_agg_in_pod[pod_number] = (last_agg_in_pod[pod_number] + 1) % agg_per_pod;
        return agg_num;
    }

}




int FatTreeNetwork::select_core(Flow* flow, double timer, core_selection mechanism) {

    int src = flow->src_dev_id;
    int dst = flow->dst_dev_id;

    ft_loc src_loc = server_loc_map[src];
    ft_loc dst_loc = server_loc_map[dst];
    int src_pod = src_loc.pod;
    int dst_pod = dst_loc.pod;

    if (mechanism == core_selection::LEAST_LOADED) {
        int best_core = -1;
        double least_load = std::numeric_limits<double>::max();

        for (int c = 0; c < core_count; c++){

            Bottleneck* bn_up = pod_core_bottlenecks[ft_loc{src_loc.pod, -1, -1, 1, c}];
            Bottleneck* bn_down = pod_core_bottlenecks[ft_loc{dst_loc.pod, -1, -1, 2, c}];
            double load = get_bottleneck_load(bn_up) + get_bottleneck_load(bn_down);

            if (load < least_load){
                least_load = load;
                best_core = c;
            }
        }
        GContext::inst().core_selection[flow->id] = best_core;
        return best_core;
    ///////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////
    } else if (mechanism == core_selection::RANDOM) {
        int core_num = rand() % core_count;
        return core_num;
    ///////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////
    } else if (mechanism == core_selection::FUTURE_LOAD) {
        if (GContext::is_first_run()) {
            int core_num = select_core(flow, timer, core_selection::ROUND_ROBIN);
            return core_num; 
        } else { 

            int last_decision = GContext::last_decision(flow->id);
            auto& last_run = GContext::last_run();

            // return last_decision;

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

            double core_load[core_count];
            for (int c = 0; c < core_count; c++) {
                core_load[c] = 0;
            }

            bool no_profiling_found = true;

            for (int t = this_run_prof.first; t <= this_run_prof.second; t += prof_inter)  {

                if (t > last_run.max_time_step) break; 

                no_profiling_found = false;

                auto& status_map = last_run.network_status;
                auto& link_loads = status_map[t].link_loads;
                auto& flow_loads = status_map[t].flow_loads;

                for (int c = 0; c < core_count; c++) {
                    auto bn_up = pod_core_bottlenecks[ft_loc{src_pod, -1, -1, 1, c}];
                    auto bn_down = pod_core_bottlenecks[ft_loc{dst_pod, -1, -1, 2, c}];
                    double total_rate = link_loads[bn_up->id] + link_loads[bn_down->id];
                    
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
            for (int c = 0; c < core_count; c++) {
                load_string += std::to_string(core_load[c]) + ", ";
            }
            spdlog::debug("core load: {}", load_string);

            if (no_profiling_found) {
                 return select_core(flow, timer, core_selection::ROUND_ROBIN);
            }

            int best_core = -1;
            double least_load = std::numeric_limits<double>::max();
            for (int c = 0; c < core_count; c++){
                if (core_load[c] < least_load){
                    least_load = core_load[c];
                    best_core = c;
                }
            }
            GContext::this_run().least_load[flow->id] = least_load / last_flow_fct;

            spdlog::debug("last decision: {}, this decision: {}", last_decision, best_core);
            spdlog::debug("-----------------------------------------------------------------");

            // todo: can I get a better estimate of the flow finishing time now? 


            auto last_run_bn_up = pod_core_bottlenecks[ft_loc{src_pod, -1, -1, 1, last_decision}];
            auto last_run_bn_down = pod_core_bottlenecks[ft_loc{dst_pod, -1, -1, 2, last_decision}];
            auto this_run_bn_up = pod_core_bottlenecks[ft_loc{src_pod, -1, -1, 1, best_core}];
            auto this_run_bn_down = pod_core_bottlenecks[ft_loc{dst_pod, -1, -1, 2, best_core}];

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
    ///////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////
    } else if (mechanism == core_selection::ROUND_ROBIN) {
        static int last_core = 0;
        int core_num = last_core;
        last_core = (last_core + 1) % core_count;
        return core_num;
    ///////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////
    } else {
        spdlog::error("Invalid core selection mechanism");
        exit(1);
    }
}


void FatTreeNetwork::set_path(Flow* flow, double timer) {
    int src = flow->src_dev_id;
    int dst = flow->dst_dev_id;

    ft_loc src_loc = server_loc_map[src];
    ft_loc dst_loc = server_loc_map[dst];

    bool same_pod = (src_loc.pod == dst_loc.pod);
    bool same_rack = same_pod && (src_loc.rack == dst_loc.rack);
    bool same_machine = same_rack && (src_loc.server == dst_loc.server);

    // flow->path.push_back(x)

    if (same_machine) {
        return; 
    } else if (same_rack) {
        flow->path.push_back(server_tor_bottlenecks[ft_loc{src_loc.pod, src_loc.rack, src_loc.server, 1, -1}]);
        // flow->path.push_back(tor_bottlenecks[ft_loc{src_loc.pod, src_loc.rack, -1, -1, -1}]);
        flow->path.push_back(server_tor_bottlenecks[ft_loc{dst_loc.pod, dst_loc.rack, dst_loc.server, 2, -1}]);
    }
    else if (same_pod) {

        int agg_num = select_agg(flow, src_loc.pod, core_selection_mechanism);
        GContext::inst().save_decision(flow->id, agg_num);

        flow->path.push_back(server_tor_bottlenecks[ft_loc{src_loc.pod, src_loc.rack, src_loc.server, 1, -1}]);
        // flow->path.push_back(tor_bottlenecks[ft_loc{src_loc.pod, src_loc.rack, -1, -1, -1}]);
        flow->path.push_back(tor_agg_bottlenecks[ft_loc{src_loc.pod, src_loc.rack, agg_num, 1, -1}]);
        // flow->path.push_back(agg_bottlenecks[ft_loc{src_loc.pod, agg_num, -1, -1, -1}]);
        flow->path.push_back(tor_agg_bottlenecks[ft_loc{dst_loc.pod, dst_loc.rack, agg_num, 2, -1}]);
        // flow->path.push_back(tor_bottlenecks[ft_loc{dst_loc.pod, dst_loc.rack, -1, -1, -1}]);
        flow->path.push_back(server_tor_bottlenecks[ft_loc{dst_loc.pod, dst_loc.rack, dst_loc.server, 2, -1}]);
    } else {
        int core_num = select_core(flow, timer, core_selection_mechanism);
        GContext::inst().save_decision(flow->id, core_num);


        int src_agg = pod_core_agg_map[ft_loc{src_loc.pod, -1, -1, -1, core_num}];
        int dst_agg = pod_core_agg_map[ft_loc{dst_loc.pod, -1, -1, -1, core_num}];

        flow->path.push_back(server_tor_bottlenecks[ft_loc{src_loc.pod, src_loc.rack, src_loc.server, 1, -1}]);
        // flow->path.push_back(tor_bottlenecks[ft_loc{src_loc.pod, src_loc.rack, -1, -1, -1}]);
        flow->path.push_back(tor_agg_bottlenecks[ft_loc{src_loc.pod, src_loc.rack, src_agg, 1, -1}]);
        // flow->path.push_back(agg_bottlenecks[ft_loc{src_loc.pod, src_agg, -1, -1, -1}]);
        flow->path.push_back(pod_core_bottlenecks[ft_loc{src_loc.pod, -1, -1, 1, core_num}]);
        // flow->path.push_back(core_bottlenecks[ft_loc{-1, -1, -1, -1, core_num}]);
        flow->path.push_back(pod_core_bottlenecks[ft_loc{dst_loc.pod, -1, -1, 2, core_num}]);
        // flow->path.push_back(agg_bottlenecks[ft_loc{dst_loc.pod, dst_agg, -1, -1, -1}]);
        flow->path.push_back(tor_agg_bottlenecks[ft_loc{dst_loc.pod, dst_loc.rack, dst_agg, 2, -1}]);
        // flow->path.push_back(tor_bottlenecks[ft_loc{dst_loc.pod, dst_loc.rack, -1, -1, -1}]);
        flow->path.push_back(server_tor_bottlenecks[ft_loc{dst_loc.pod, dst_loc.rack, dst_loc.server, 2, -1}]);
    }

}


double FatTreeNetwork::total_core_bw_utilization(){
    double total_utilization = 0; 

    for (int p = 0; p < pod_count; p++) {
        for (int c = 0; c < core_count; c++) {
            Bottleneck* bn_up = pod_core_bottlenecks[ft_loc{p, -1, -1, 1, c}];
            Bottleneck* bn_down = pod_core_bottlenecks[ft_loc{p, -1, -1, 2, c}];

            total_utilization += bn_up->bwalloc->utilized_bandwidth;
            total_utilization += bn_down->bwalloc->utilized_bandwidth;
        }
    }

    return total_utilization;
}

double FatTreeNetwork::min_core_link_bw_utilization(){
    double min_utilization = std::numeric_limits<double>::max();

    for (int p = 0; p < pod_count; p++) {
        for (int c = 0; c < core_count; c++) {
            Bottleneck* bn_up = pod_core_bottlenecks[ft_loc{p, -1, -1, 1, c}];
            Bottleneck* bn_down = pod_core_bottlenecks[ft_loc{p, -1, -1, 2, c}];

            double utilization = std::min(bn_up->bwalloc->utilized_bandwidth, bn_down->bwalloc->utilized_bandwidth);
            min_utilization = std::min(min_utilization, utilization);
        }
    }

    return min_utilization;
}

double FatTreeNetwork::max_core_link_bw_utilization(){
    double max_utilization = 0;

    for (int p = 0; p < pod_count; p++) {
        for (int c = 0; c < core_count; c++) {
            Bottleneck* bn_up = pod_core_bottlenecks[ft_loc{p, -1, -1, 1, c}];
            Bottleneck* bn_down = pod_core_bottlenecks[ft_loc{p, -1, -1, 2, c}];

            double utilization = std::max(bn_up->bwalloc->utilized_bandwidth, bn_down->bwalloc->utilized_bandwidth);
            max_utilization = std::max(max_utilization, utilization);
        }
    }

    return max_utilization;
}