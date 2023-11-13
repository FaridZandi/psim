
#include <iostream>
#include <cassert>
#include <cmath>
#include <algorithm>
#include <limits>
#include <set>

#include "network.h"
#include "gconfig.h"
#include "gcontext.h"
#include "spdlog/spdlog.h"

using namespace psim;


CoreConnectedNetwork::CoreConnectedNetwork() : Network() {}

CoreConnectedNetwork::~CoreConnectedNetwork() {
    if (core_load_balancer){
        delete core_load_balancer;
    }
}

void CoreConnectedNetwork::record_link_status(double timer) {
    int timer_int = int(timer);

    if (lb_scheme != LBScheme::FUTURE_LOAD and 
        lb_scheme != LBScheme::FUTURE_LOAD_2) {
        return;
    }

    auto& this_run = GContext::this_run();
    auto& status = this_run.network_status[timer_int];
    status.time = timer_int;
    this_run.max_time_step = timer_int;

    for (auto bn: bottlenecks) {
        status.link_loads[bn->id] = bn->get_load();

        if(GConf::inst().record_link_flow_loads){
            for (auto flow: bn->flows) {
                status.link_flow_loads[bn->id].push_back(flow->get_load());
            }
        }
    }

    for (auto flow: flows) {
        status.flow_loads[flow->id] = flow->get_load();
    }
}

double CoreConnectedNetwork::total_core_bw_utilization(){
    double total_utilization = 0;

    for (auto& bn: core_bottlenecks) {
        total_utilization += bn.second->bwalloc->utilized_bandwidth;
    }

    return total_utilization;
}

double CoreConnectedNetwork::min_core_link_bw_utilization(){
    double min_utilization = std::numeric_limits<double>::max();

    for (auto& bn: core_bottlenecks) {
        min_utilization = std::min(min_utilization, bn.second->bwalloc->utilized_bandwidth);
    }

    return min_utilization;
}

double CoreConnectedNetwork::max_core_link_bw_utilization(){
    double max_utilization = 0;

    for (auto& bn: core_bottlenecks) {
        max_utilization = std::max(max_utilization, bn.second->bwalloc->utilized_bandwidth);
    }

    return max_utilization;
}


std::vector<int> CoreConnectedNetwork::get_core_bottleneck_ids(){
    std::vector<int> ids;
    for (auto& bn: core_bottlenecks) {
        ids.push_back(bn.second->id);
    }
    return ids;
}




/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////



FatTreeNetwork::FatTreeNetwork() : CoreConnectedNetwork() {

    server_count = GConf::inst().machine_count;
    server_per_rack = GConf::inst().ft_server_per_rack;
    rack_per_pod = GConf::inst().ft_rack_per_pod;
    agg_per_pod = GConf::inst().ft_agg_per_pod;
    pod_count = GConf::inst().ft_pod_count;
    core_count = GConf::inst().ft_core_count;

    int link_bandwidth = GConf::inst().link_bandwidth;
    server_tor_link_capacity = GConf::inst().ft_server_tor_link_capacity_mult * link_bandwidth;
    tor_agg_link_capacity = GConf::inst().ft_tor_agg_link_capacity_mult * link_bandwidth;
    core_link_capacity = GConf::inst().ft_agg_core_link_capacity_mult * link_bandwidth;

    if (server_count != server_per_rack * rack_per_pod * pod_count) {
        spdlog::error("server_count must be equal to server_per_rack * rack_per_pod * pod_count");
        exit(1);
    }

    if (core_count % agg_per_pod != 0) {
        spdlog::error("core_count must be divisible by agg_per_pod");
        exit(1);
    } else {
        core_link_per_agg = core_count / agg_per_pod;
    }

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

            Bottleneck *bn_up = create_bottleneck(core_link_capacity);
            core_bottlenecks[ft_loc{i, -1, -1, 1, c}] = bn_up;

            Bottleneck *bn_down = create_bottleneck(core_link_capacity);
            core_bottlenecks[ft_loc{i, -1, -1, 2, c}] = bn_down;

            pod_core_agg_map[ft_loc{i, -1, -1, -1, c}] = agg_num;
        }
    }

    last_agg_in_pod = new int[pod_count];
    for (int i = 0; i < pod_count; i++) {
        last_agg_in_pod[i] = 0;
    }

    lb_scheme = GConf::inst().lb_scheme;
    core_load_balancer = LoadBalancer::create_load_balancer(core_count, lb_scheme);

    for (int p = 0; p < pod_count; p++) {
        for (int c = 0; c < core_count; c++) {
            auto bn_up = core_bottlenecks[ft_loc{p, -1, -1, 1, c}];
            auto bn_down = core_bottlenecks[ft_loc{p, -1, -1, 2, c}];

            core_load_balancer->register_link(p, c, 1, bn_up);
            core_load_balancer->register_link(p, c, 2, bn_down);
        }
    }
}

FatTreeNetwork::~FatTreeNetwork() {}


int FatTreeNetwork::select_agg(Flow* flow, int pod_number, LBScheme mechanism) {

    if(mechanism == LBScheme::FUTURE_LOAD){
        if (GContext::is_first_run()) {
            return select_agg(flow, pod_number, LBScheme::ROUND_ROBIN);
        }
        int last_decision = GContext::inst().last_decision(flow->id);
        return last_decision;
    } else {
        int agg_num = last_agg_in_pod[pod_number];
        last_agg_in_pod[pod_number] = (last_agg_in_pod[pod_number] + 1) % agg_per_pod;
        return agg_num;
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
        flow->path.push_back(server_tor_bottlenecks[ft_loc{dst_loc.pod, dst_loc.rack, dst_loc.server, 2, -1}]);
    }
    else if (same_pod) {

        int agg_num = select_agg(flow, src_loc.pod, lb_scheme);
        GContext::inst().save_decision(flow->id, agg_num);

        flow->path.push_back(server_tor_bottlenecks[ft_loc{src_loc.pod, src_loc.rack, src_loc.server, 1, -1}]);
        flow->path.push_back(tor_agg_bottlenecks[ft_loc{src_loc.pod, src_loc.rack, agg_num, 1, -1}]);
        flow->path.push_back(tor_agg_bottlenecks[ft_loc{dst_loc.pod, dst_loc.rack, agg_num, 2, -1}]);
        flow->path.push_back(server_tor_bottlenecks[ft_loc{dst_loc.pod, dst_loc.rack, dst_loc.server, 2, -1}]);
    } else {
        int core_num = core_load_balancer->get_upper_item(src_loc.pod, dst_loc.pod, flow, timer);
        GContext::inst().save_decision(flow->id, core_num);

        int src_agg = pod_core_agg_map[ft_loc{src_loc.pod, -1, -1, -1, core_num}];
        int dst_agg = pod_core_agg_map[ft_loc{dst_loc.pod, -1, -1, -1, core_num}];

        flow->path.push_back(server_tor_bottlenecks[ft_loc{src_loc.pod, src_loc.rack, src_loc.server, 1, -1}]);
        flow->path.push_back(tor_agg_bottlenecks[ft_loc{src_loc.pod, src_loc.rack, src_agg, 1, -1}]);
        flow->path.push_back(core_bottlenecks[ft_loc{src_loc.pod, -1, -1, 1, core_num}]);
        flow->path.push_back(core_bottlenecks[ft_loc{dst_loc.pod, -1, -1, 2, core_num}]);
        flow->path.push_back(tor_agg_bottlenecks[ft_loc{dst_loc.pod, dst_loc.rack, dst_agg, 2, -1}]);
        flow->path.push_back(server_tor_bottlenecks[ft_loc{dst_loc.pod, dst_loc.rack, dst_loc.server, 2, -1}]);
    }
}



int FatTreeNetwork::get_source_for_flow(Flow* flow) {
    return server_loc_map[flow->src_dev_id].pod;
}



/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////



LeafSpineNetwork::LeafSpineNetwork() : CoreConnectedNetwork() {
    int link_bandwidth = GConf::inst().link_bandwidth;

    server_count = GConf::inst().machine_count;
    // to make it compatible with fat tree, we assume the racks are same as pods
    server_per_rack = GConf::inst().ft_server_per_rack * GConf::inst().ft_rack_per_pod;
    tor_count = server_count / server_per_rack;
    core_count = GConf::inst().ft_core_count;

    server_tor_link_capacity = GConf::inst().ft_server_tor_link_capacity_mult * link_bandwidth;
    core_link_capacity = GConf::inst().ft_agg_core_link_capacity_mult * link_bandwidth;

    for (int i = 0; i < tor_count; i++) {
        for (int k = 0; k < server_per_rack; k++) {
            int machine_num = i * server_per_rack + k;
            Machine *machine = get_machine(machine_num);
            server_loc_map[machine_num] = ft_loc{-1, i, k, -1, -1};

            Bottleneck *bn_up = create_bottleneck(server_tor_link_capacity);
            server_tor_bottlenecks[ft_loc{-1, i, k, 1, -1}] = bn_up;

            Bottleneck *bn_down = create_bottleneck(server_tor_link_capacity);
            server_tor_bottlenecks[ft_loc{-1, i, k, 2, -1}] = bn_down;
        }

        for (int c = 0; c < core_count; c++) {
            Bottleneck *bn_up = create_bottleneck(core_link_capacity);
            core_bottlenecks[ft_loc{-1, i, -1, 1, c}] = bn_up;

            Bottleneck *bn_down = create_bottleneck(core_link_capacity);
            core_bottlenecks[ft_loc{-1, i, -1, 2, c}] = bn_down;
        }
    }

    lb_scheme = GConf::inst().lb_scheme;
    core_load_balancer = LoadBalancer::create_load_balancer(core_count, lb_scheme);

    for (int t = 0; t < tor_count; t++) {
        for (int c = 0; c < core_count; c++) {
            auto bn_up = core_bottlenecks[ft_loc{-1, t, -1, 1, c}];
            auto bn_down = core_bottlenecks[ft_loc{-1, t, -1, 2, c}];

            core_load_balancer->register_link(t, c, 1, bn_up);
            core_load_balancer->register_link(t, c, 2, bn_down);
        }
    }
}

LeafSpineNetwork::~LeafSpineNetwork() {}


void LeafSpineNetwork::set_path(Flow* flow, double timer) {
    ft_loc src_loc = server_loc_map[flow->src_dev_id];
    ft_loc dst_loc = server_loc_map[flow->dst_dev_id];

    bool same_rack = (src_loc.rack == dst_loc.rack);
    bool same_machine = same_rack && (src_loc.server == dst_loc.server);

    if (same_machine) {
        return;

    } else if (same_rack) {
        flow->path.push_back(server_tor_bottlenecks[ft_loc{-1, src_loc.rack, src_loc.server, 1, -1}]);
        flow->path.push_back(server_tor_bottlenecks[ft_loc{-1, dst_loc.rack, dst_loc.server, 2, -1}]);

    } else {
        int core_num = core_load_balancer->get_upper_item(src_loc.rack, dst_loc.rack, flow, timer);
        GContext::inst().save_decision(flow->id, core_num);

        flow->path.push_back(server_tor_bottlenecks[ft_loc{-1, src_loc.rack, src_loc.server, 1, -1}]);
        flow->path.push_back(core_bottlenecks[ft_loc{-1, src_loc.rack, -1, 1, core_num}]);
        flow->path.push_back(core_bottlenecks[ft_loc{-1, dst_loc.rack, -1, 2, core_num}]);
        flow->path.push_back(server_tor_bottlenecks[ft_loc{-1, dst_loc.rack, dst_loc.server, 2, -1}]);
    }
}


int LeafSpineNetwork::get_source_for_flow(Flow* flow) {
    return server_loc_map[flow->src_dev_id].rack;
}
