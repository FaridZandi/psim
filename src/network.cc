
#include <iostream>
#include <cassert>
#include <algorithm>
#include <limits>
#include <set> 

#include "spdlog/spdlog.h"

#include "network.h"
#include "context.h"

using namespace psim;

Network::Network() {
    Bottleneck::bottleneck_counter = 0;

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

double Network::get_bottleneck_load(Bottleneck* bn) {
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

Network::~Network() {
    for (auto bottleneck : this->bottlenecks) {
        delete bottleneck;
    }

    for (auto machine : this->machines) {
        delete machine;
    }
}


double Network::make_progress_on_machines(double current_time, double step_size, 
                                          std::vector<PComp*> & step_finished_tasks){
    double step_comp = 0;

    for (auto& machine : this->machines) {
        step_comp += machine->make_progress(current_time, step_size, step_finished_tasks);
    }

    if (GConf::inst().record_machine_history){
        for (auto& machine: this->machines) {
            machine->task_queue_length_history.push_back(machine->task_queue.size());
        }
    }

    return step_comp; 
}

void 
Network::reset_bottleneck_registers(){
    for (auto bottleneck : this->bottlenecks) {
        bottleneck->reset_register();
    }
} 

void 
Network::compute_bottleneck_allocations(){
    for (auto bottleneck : this->bottlenecks) {
        bottleneck->allocate_bandwidths();
    }
} 



psim::Machine* Network::get_machine(int name){
    if (this->machine_map.find(name) == this->machine_map.end()) {
        Machine *machine = new Machine(name);
        this->machines.push_back(machine);
        this->machine_map[name] = machine;
        return machine;
    } else {
        return this->machine_map[name];
    }
}



Bottleneck* Network::create_bottleneck(double bandwidth) {
    Bottleneck *bottleneck = new Bottleneck(bandwidth);
    bottlenecks.push_back(bottleneck);
    bottleneck_map[bottleneck->id] = bottleneck;
    return bottleneck;
}


double Network::total_link_bandwidth() {
    static bool is_cached = false;
    static double cached_total = 0;

    if (is_cached) {
        return cached_total;
    } else {
        double total = 0;

        for (auto& bn : bottlenecks) {
            total += bn->bandwidth;
        }

        cached_total = total;
        is_cached = true;

        return total;
    }
}

double Network::total_bw_utilization () {
    double total = 0;

    for (auto& bn : bottlenecks) {
        total += bn->bwalloc->utilized_bandwidth;
    }

    return total;
}

double Network::total_core_bw_utilization() {
    return 0; 
}


double Network::min_core_link_bw_utilization(){ 
    return 0;
} 

double Network::max_core_link_bw_utilization(){
    return 0;
}



//==============================================================================


BigSwitchNetwork::BigSwitchNetwork(): Network() {
    this->server_switch_link_capacity = GConf::inst().link_bandwidth;
    this->server_count = GConf::inst().machine_count;

    for (int i = 0; i < this->server_count; i++) {
        Machine *machine = get_machine(i);
    }

    for (int i = 0; i < this->server_count; i++) {
        Bottleneck *ds_bn = create_bottleneck(server_switch_link_capacity);
        this->server_bottlenecks_downstream[i] = ds_bn;

        Bottleneck *us_bn = create_bottleneck(server_switch_link_capacity);
        this->server_bottlenecks_upstream[i] = us_bn;
    }
}

BigSwitchNetwork::~BigSwitchNetwork() {
    
}

void BigSwitchNetwork::set_path(Flow* flow, double timer) {
    flow->path.push_back(server_bottlenecks_upstream[flow->src_dev_id]);
    flow->path.push_back(server_bottlenecks_downstream[flow->dst_dev_id]);
}


//==============================================================================
//==============================================================================
//==============================================================================



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


    core_usage_count = new int[core_count];
    core_usage_sum = new double[core_count];
    for (int i = 0; i < core_count; i++) {
        core_usage_count[i] = 0;
        core_usage_sum[i] = 0; 
    }

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
    status = core_link_status();

    auto& link_loads = status.link_loads;
    for (auto bn: bottlenecks) {
        link_loads[bn->id] = get_bottleneck_load(bn);
    }

    curr_run_info.max_time_step = timer_int;
}


int FatTreeNetwork::select_agg(Flow* flow, int pod_number) {
    int agg_num = last_agg_in_pod[pod_number];
    last_agg_in_pod[pod_number] = (last_agg_in_pod[pod_number] + 1) % agg_per_pod;
    return agg_num;
}

std::pair<int, int> get_prof_limits(double start_time, double end_time){
    int prof_inter = GConf::inst().core_status_profiling_interval; 

    int prof_start = int(start_time);
    int prof_end = int(end_time);

    if (prof_start % prof_inter != 0) {
        prof_start = (prof_start / prof_inter + 1) * prof_inter;
    }

    if (prof_end % prof_inter != 0) {
        prof_end = (prof_end / prof_inter) * prof_inter;
    }

    return std::make_pair(prof_start, prof_end);
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
        if (GContext::first_run()) {
            int core_num = select_core(flow, timer, core_selection::ROUND_ROBIN);
            return core_num; 
        } else { 

            int last_decision = GContext::inst().last_decision(flow->id);
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

            if (last_flow_fct == 0) {
                spdlog::error("last flow fct is 0");
            }

            spdlog::debug("flow {}: last start: {}, last finish: {}", 
                          flow->id, last_flow_start, last_flow_end);

            spdlog::debug("last decision: {}, last rate: {}, last transfer time: {}", 
                          last_decision, last_flow_rate, last_flow_fct);
            
            auto last_run_prof = get_prof_limits(timer, timer + last_flow_fct);
            auto this_run_prof = get_prof_limits(last_flow_start, last_flow_end);
            
            spdlog::debug("last run: profiling interval: {}, prof_start: {}, prof_end: {}", 
                          prof_inter, last_run_prof.first, last_run_prof.second);
            
            spdlog::debug("this run: profiling interval: {}, prof_start: {}, prof_end: {}",
                            prof_inter, this_run_prof.first, this_run_prof.second);


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

                for (int c = 0; c < core_count; c++) {
                    auto bn_up = pod_core_bottlenecks[ft_loc{src_pod, -1, -1, 1, c}];
                    auto bn_down = pod_core_bottlenecks[ft_loc{dst_pod, -1, -1, 2, c}];
                    double total_rate = link_loads[bn_up->id] + link_loads[bn_down->id];
                    
                    if (c == last_decision) {
                        if (t >= last_run_prof.first and t <= last_run_prof.second) {
                            // if we are looking at the previous run for the same core 
                            // during the time that the flow was running, then we need to
                            // subtract the flow rate from the total rate.
                            total_rate -= 2 * last_flow_rate;
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

            spdlog::debug("last decision: {}, this decision: {}", last_decision, best_core);
            spdlog::debug("-----------------------------------------------------------------");

            // todo: can I get a better estimate of the flow finishing time now? 


            auto last_run_bn_up = pod_core_bottlenecks[ft_loc{src_pod, -1, -1, 1, last_decision}];
            auto last_run_bn_down = pod_core_bottlenecks[ft_loc{dst_pod, -1, -1, 2, last_decision}];
            auto this_run_bn_up = pod_core_bottlenecks[ft_loc{src_pod, -1, -1, 1, best_core}];
            auto this_run_bn_down = pod_core_bottlenecks[ft_loc{dst_pod, -1, -1, 2, best_core}];

            // update the link loads in the last run, to account for the different decision.
            for (int t = last_run_prof.first; t <= last_run_prof.second; t += prof_inter)  {
                if (t > last_run.max_time_step) break;

                auto& status = last_run.network_status[t];
                auto& link_loads = status.link_loads;
                link_loads[last_run_bn_up->id] -= last_flow_rate;
                link_loads[last_run_bn_down->id] -= last_flow_rate;
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



        int agg_num = select_agg(flow, src_loc.pod);

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


        core_usage_count[core_num] += 1;
        core_usage_sum[core_num] += flow->size; 

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

//==============================================================================
//==============================================================================
//==============================================================================

psim::Machine::Machine(int name) {
    this->name = name;
}

psim::Machine::~Machine() {
    
}

double psim::Machine::make_progress(double current_time, double step_size, 
                                    std::vector<PComp*> & step_finished_tasks) {
    if (this->task_queue.empty()) {
        return 0; 
    }

    double epsilon = step_size / 1000; // floating point errors ...
    double step_comp = 0;
    double avail_comp = step_size;

    int handled_tasks = 0; 

    while (not this->task_queue.empty()){
        PComp* compute_task = this->task_queue.front();
        handled_tasks += 1;
        
        if (compute_task->progress == 0){
            compute_task->start_time = current_time; 
        }   

        double task_remaining = compute_task->size - compute_task->progress;
        double progress_to_make = std::min(avail_comp, task_remaining);

        compute_task->progress += progress_to_make;
        step_comp += progress_to_make;
        avail_comp -= progress_to_make;
        task_remaining -= progress_to_make;

        if (task_remaining < epsilon) {
            compute_task->progress = compute_task->size;
            this->task_queue.pop();
            compute_task->status = PTaskStatus::FINISHED;
            step_finished_tasks.push_back(compute_task);
        }

        if (avail_comp < epsilon) {
            break;
        }
    }

    return step_comp; 
}


int Bottleneck::bottleneck_counter = 0;


Bottleneck::Bottleneck(double bandwidth) {
    id = bottleneck_counter;
    bottleneck_counter += 1;

    this->bandwidth = bandwidth;
    this->current_flow_count = 0;
    this->current_flow_size_sum = 0;

    setup_bwalloc(); 
}


Bottleneck::~Bottleneck() {
    delete bwalloc;
}


void Bottleneck::setup_bwalloc() {
    std::string priority_allocator = GConf::inst().priority_allocator;
    
    if (priority_allocator == "priorityqueue"){
        bwalloc = new PriorityQueueBandwidthAllocator(bandwidth);
    } else if (priority_allocator == "fixedlevels"){
        bwalloc = new FixedLevelsBandwidthAllocator(bandwidth);
    } else if (priority_allocator == "fairshare"){
        bwalloc = new FairShareBandwidthAllocator(bandwidth);
    } else {
        spdlog::error("Invalid priority allocator");
        exit(1);
    }
}

void Bottleneck::reset_register(){
    bwalloc->reset();
}

void Bottleneck::register_rate(int id, double rate, int priority){
    bwalloc->register_rate(id, rate, priority);
}

void Bottleneck::allocate_bandwidths(){
    bwalloc->compute_allocations(); 
}

double Bottleneck::get_allocated_rate(int id, double registered_rate, int priority){
    return bwalloc->get_allocated_rate(id, registered_rate, priority);
}

bool Bottleneck::should_drop(double step_size){
    double excess = bwalloc->total_registered - bandwidth;

    if (excess > 0) {
        // probablity of dropping a packet is proportional to the excess rate 
        double drop_prob = excess / bandwidth;
        // drop_prob = 1 - pow(1 - drop_prob, step_size);
        drop_prob *= step_size;

        double rand_num = (double) rand() / RAND_MAX;

        if (rand_num < drop_prob) {
            // std::cout << "dropping rate! who would have thought! \n" << std::endl;
            return true;
        }
    } 

    return false;
}


bool ft_loc::operator<(const ft_loc& rhs) const {
    if (pod < rhs.pod) return true;
    if (pod > rhs.pod) return false;
    if (rack < rhs.rack) return true;
    if (rack > rhs.rack) return false;
    if (server < rhs.server) return true;
    if (server > rhs.server) return false;
    if (dir < rhs.dir) return true;
    if (dir > rhs.dir) return false;
    if (core < rhs.core) return true;
    if (core > rhs.core) return false; 
    
    return false;
}
