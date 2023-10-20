
#include <iostream>
#include <cassert>
#include <algorithm>
#include <limits>
#include <set>

#include "network.h"
#include "gconfig.h"
#include "gcontext.h"
#include "spdlog/spdlog.h"

using namespace psim;

LeafSpineNetwork::LeafSpineNetwork() : Network() {
    int link_bandwidth = GConf::inst().link_bandwidth;

    server_count = GConf::inst().machine_count;
    server_per_rack = GConf::inst().ft_server_per_rack;
    tor_count = server_count / server_per_rack;
    core_count = GConf::inst().ft_core_count;

    server_tor_link_capacity = GConf::inst().ft_server_tor_link_capacity_mult * link_bandwidth;
    tor_core_link_capacity = GConf::inst().ft_agg_core_link_capacity_mult * link_bandwidth;

    


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
            Bottleneck *bn_up = create_bottleneck(tor_core_link_capacity);
            tor_core_bottlenecks[ft_loc{-1, i, -1, 1, c}] = bn_up;

            Bottleneck *bn_down = create_bottleneck(tor_core_link_capacity);
            tor_core_bottlenecks[ft_loc{-1, i, -1, 2, c}] = bn_down;
        }
    }
    


    core_load_balancer = LoadBalancer::create_load_balancer(GConf::inst().core_selection_mechanism, 
                                                            core_count, 
                                                            core_selection_mechanism);

    for (int t = 0; t < tor_count; t++) {
        for (int c = 0; c < core_count; c++) {
            auto bn_up = tor_core_bottlenecks[ft_loc{-1, t, -1, 1, c}];
            auto bn_down = tor_core_bottlenecks[ft_loc{-1, t, -1, 2, c}];

            core_load_balancer->register_link(t, c, 1, bn_up);
            core_load_balancer->register_link(t, c, 2, bn_down);
        }
    }
    
}

LeafSpineNetwork::~LeafSpineNetwork() {
}

void LeafSpineNetwork::record_link_status(double timer) {
    if (core_selection_mechanism != core_selection::FUTURE_LOAD) {
        return;
    }

    int timer_int = int(timer);

    auto& this_run = GContext::this_run();
    auto& status = this_run.network_status[timer_int];
    status.time = timer_int;

    auto& link_loads = status.link_loads;
    for (auto bn: bottlenecks) {
        link_loads[bn->id] = get_bottleneck_load(bn);
    }

    auto& flow_loads = status.flow_loads;
    for (auto flow: flows) {
        flow_loads[flow->id] = flow->last_rate;
    }
    this_run.max_time_step = timer_int;
}


void LeafSpineNetwork::set_path(Flow* flow, double timer) {
    int src = flow->src_dev_id;
    int dst = flow->dst_dev_id;

    ft_loc src_loc = server_loc_map[src];
    ft_loc dst_loc = server_loc_map[dst];

    bool same_rack = (src_loc.rack == dst_loc.rack);
    bool same_machine = same_rack && (src_loc.server == dst_loc.server);

    // flow->path.push_back(x)

    if (same_machine) {
        return;
    } else if (same_rack) {
        flow->path.push_back(server_tor_bottlenecks[ft_loc{-1, src_loc.rack, src_loc.server, 1, -1}]);
        flow->path.push_back(server_tor_bottlenecks[ft_loc{-1, dst_loc.rack, dst_loc.server, 2, -1}]);
    }
    else {
        int core_num = core_load_balancer->get_upper_item(src_loc.rack, dst_loc.rack, flow, timer);
        GContext::inst().save_decision(flow->id, core_num);

        flow->path.push_back(server_tor_bottlenecks[ft_loc{-1, src_loc.rack, src_loc.server, 1, -1}]);
        flow->path.push_back(tor_core_bottlenecks[ft_loc{-1, src_loc.rack, -1, 1, core_num}]);
        flow->path.push_back(tor_core_bottlenecks[ft_loc{-1, dst_loc.rack, -1, 2, core_num}]);
        flow->path.push_back(server_tor_bottlenecks[ft_loc{-1, dst_loc.rack, dst_loc.server, 2, -1}]);
    }
}


double LeafSpineNetwork::total_core_bw_utilization(){
    double total_utilization = 0;

    for (int t = 0; t < tor_count; t++) {
        for (int c = 0; c < core_count; c++) {
            Bottleneck* bn_up = tor_core_bottlenecks[ft_loc{-1, t, -1, 1, c}];
            Bottleneck* bn_down = tor_core_bottlenecks[ft_loc{-1, t, -1, 2, c}];

            total_utilization += bn_up->bwalloc->utilized_bandwidth;
            total_utilization += bn_down->bwalloc->utilized_bandwidth;
        }
    }

    return total_utilization;
}

double LeafSpineNetwork::min_core_link_bw_utilization(){
    double min_utilization = std::numeric_limits<double>::max();


    for (int t = 0; t < tor_count; t++) {
        for (int c = 0; c < core_count; c++) {
            Bottleneck* bn_up = tor_core_bottlenecks[ft_loc{-1, t, -1, 1, c}];
            Bottleneck* bn_down = tor_core_bottlenecks[ft_loc{-1, t, -1, 2, c}];

            double utilization = std::min(bn_up->bwalloc->utilized_bandwidth, bn_down->bwalloc->utilized_bandwidth);
            min_utilization = std::min(min_utilization, utilization);

        }
    }

    return min_utilization;
}

double LeafSpineNetwork::max_core_link_bw_utilization(){
    double max_utilization = 0;

    for (int t = 0; t < tor_count; t++) {
        for (int c = 0; c < core_count; c++) {
            Bottleneck* bn_up = tor_core_bottlenecks[ft_loc{-1, t, -1, 1, c}];
            Bottleneck* bn_down = tor_core_bottlenecks[ft_loc{-1, t, -1, 2, c}];

            double utilization = std::max(bn_up->bwalloc->utilized_bandwidth, bn_down->bwalloc->utilized_bandwidth);
            max_utilization = std::max(max_utilization, utilization);
        }
    }

    return max_utilization;
}
