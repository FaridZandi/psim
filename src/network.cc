
#include "network.h"
#include <iostream>
#include <cassert>
#include "spdlog/spdlog.h"
#include <algorithm>

using namespace psim;

Network::Network() {
}

Network::~Network() {
    for (auto bottleneck : this->bottlenecks) {
        delete bottleneck;
    }

    for (auto machine : this->machines) {
        delete machine;
    }
}


double Network::make_progress_on_machines(double step_size, 
                                          std::vector<PComp*> & step_finished_tasks){
    double step_comp = 0;

    for (auto& machine : this->machines) {
        step_comp += machine->make_progress(step_size, step_finished_tasks);
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
Network::compute_bottleneck_availability(){
    for (auto bottleneck : this->bottlenecks) {
        bottleneck->compute_availability();
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
    
    bottleneck_map[bottleneck_counter] = bottleneck;
    bottleneck->id = bottleneck_counter;
    bottleneck_counter += 1; 

    return bottleneck;
}

//==============================================================================


BigSwitchNetwork::BigSwitchNetwork(): Network() {

    double link_bandwidth = GConf::inst().link_bandwidth;
    this->server_count = GConf::inst().machine_count;

    for (int i = 0; i < this->server_count; i++) {
        Machine *machine = get_machine(i);
    }

    this->switch_bottleneck = create_bottleneck(link_bandwidth);
    
    for (int i = 0; i < this->server_count; i++) {
        Bottleneck *ds_bn = create_bottleneck(link_bandwidth);
        this->server_bottlenecks_downstream[i] = ds_bn;

        Bottleneck *us_bn = create_bottleneck(link_bandwidth);
        this->server_bottlenecks_upstream[i] = us_bn;
    }
}

BigSwitchNetwork::~BigSwitchNetwork() {
    
}

void BigSwitchNetwork::set_path(Flow* flow) {
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

    core_capacity = GConf::inst().ft_core_capacity_mult * link_bandwidth;
    agg_capacity = GConf::inst().ft_agg_capacity_mult * link_bandwidth;
    tor_capacity = GConf::inst().ft_tor_capacity_mult * link_bandwidth;
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

    core_link_per_agg = core_count / agg_per_pod;

    for (int i = 0; i < core_count; i++) {
        Bottleneck *bn = create_bottleneck(core_capacity);
        core_bottlenecks[ft_loc{-1, -1, -1, -1, i}] = bn;
    }

    for (int i = 0; i < pod_count; i++) {
        for (int j = 0; j < agg_per_pod; j++) {
            Bottleneck *bn = create_bottleneck(agg_capacity);
            agg_bottlenecks[ft_loc{i, j, -1, -1, -1}] = bn;
        }

        for (int j = 0; j < rack_per_pod; j++) {
            Bottleneck *bn = create_bottleneck(tor_capacity);
            tor_bottlenecks[ft_loc{i, j, -1, -1, -1}] = bn;


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

}

FatTreeNetwork::~FatTreeNetwork() {
    
}

void FatTreeNetwork::set_path(Flow* flow) {
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
        flow->path.push_back(tor_bottlenecks[ft_loc{src_loc.pod, src_loc.rack, -1, -1, -1}]);
        flow->path.push_back(server_tor_bottlenecks[ft_loc{dst_loc.pod, dst_loc.rack, dst_loc.server, 2, -1}]);
    }
    else if (same_pod) {

        int agg_num = rand() % agg_per_pod;

        flow->path.push_back(server_tor_bottlenecks[ft_loc{src_loc.pod, src_loc.rack, src_loc.server, 1, -1}]);
        flow->path.push_back(tor_bottlenecks[ft_loc{src_loc.pod, src_loc.rack, -1, -1, -1}]);
        flow->path.push_back(tor_agg_bottlenecks[ft_loc{src_loc.pod, src_loc.rack, agg_num, 1, -1}]);
        flow->path.push_back(agg_bottlenecks[ft_loc{src_loc.pod, agg_num, -1, -1, -1}]);
        flow->path.push_back(tor_agg_bottlenecks[ft_loc{dst_loc.pod, dst_loc.rack, agg_num, 2, -1}]);
        flow->path.push_back(tor_bottlenecks[ft_loc{dst_loc.pod, dst_loc.rack, -1, -1, -1}]);
        flow->path.push_back(server_tor_bottlenecks[ft_loc{dst_loc.pod, dst_loc.rack, dst_loc.server, 2, -1}]);
    } else {
        int core_num = rand() % core_count;

        int src_agg = pod_core_agg_map[ft_loc{src_loc.pod, -1, -1, -1, core_num}];
        int dst_agg = pod_core_agg_map[ft_loc{dst_loc.pod, -1, -1, -1, core_num}];

        flow->path.push_back(server_tor_bottlenecks[ft_loc{src_loc.pod, src_loc.rack, src_loc.server, 1, -1}]);
        flow->path.push_back(tor_bottlenecks[ft_loc{src_loc.pod, src_loc.rack, -1, -1, -1}]);
        flow->path.push_back(tor_agg_bottlenecks[ft_loc{src_loc.pod, src_loc.rack, src_agg, 1, -1}]);
        flow->path.push_back(agg_bottlenecks[ft_loc{src_loc.pod, src_agg, -1, -1, -1}]);
        flow->path.push_back(pod_core_bottlenecks[ft_loc{src_loc.pod, -1, -1, 1, core_num}]);
        flow->path.push_back(core_bottlenecks[ft_loc{-1, -1, -1, -1, core_num}]);
        flow->path.push_back(pod_core_bottlenecks[ft_loc{dst_loc.pod, -1, -1, 2, core_num}]);
        flow->path.push_back(agg_bottlenecks[ft_loc{dst_loc.pod, dst_agg, -1, -1, -1}]);
        flow->path.push_back(tor_agg_bottlenecks[ft_loc{dst_loc.pod, dst_loc.rack, dst_agg, 2, -1}]);
        flow->path.push_back(tor_bottlenecks[ft_loc{dst_loc.pod, dst_loc.rack, -1, -1, -1}]);
        flow->path.push_back(server_tor_bottlenecks[ft_loc{dst_loc.pod, dst_loc.rack, dst_loc.server, 2, -1}]);
    }

}

//==============================================================================
//==============================================================================
//==============================================================================

psim::Machine::Machine(int name) {
    this->name = name;
}

psim::Machine::~Machine() {
    
}

double psim::Machine::make_progress(double step_size, std::vector<PComp*> & step_finished_tasks) {
    double step_comp = 0; 

    if (this->task_queue.empty()) {
        return 0; 
    }
    
    PComp* compute_task = this->task_queue.front();
    
    compute_task->progress += step_size;
    step_comp += step_size;

    if (compute_task->progress >= compute_task->size) {
        step_comp -= (compute_task->progress - compute_task->size);
        compute_task->progress = compute_task->size;

        this->task_queue.pop();
        compute_task->status = PTaskStatus::FINISHED;
        step_finished_tasks.push_back(compute_task);
    }

    return step_comp; 
}

Bottleneck::Bottleneck(double bandwidth) {
    this->bandwidth = bandwidth;
    id = -1; 

    if (GConf::inst().priority_allocator == "priorityqueue"){
        pa = new PriorityQueuePriorityAllocator(this->bandwidth);
    } else if (GConf::inst().priority_allocator == "fixedlevels"){
        pa = new FixedLevelsPriorityAllocator(this->bandwidth);
    } else {
        spdlog::error("Invalid priority allocator");
        exit(1);
    }
}

Bottleneck::~Bottleneck() {
    delete pa;
}

void Bottleneck::reset_register(){
    pa->reset();
}

void Bottleneck::register_rate(int id, double rate, int priority){
    pa->register_rate(id, rate, priority);
}

void Bottleneck::compute_availability(){
    pa->compute_allocations(); 
}

double Bottleneck::get_allocated_rate(int id, double registered_rate, int priority){
    return pa->get_allocated_rate(id, registered_rate, priority);
}

bool Bottleneck::should_drop(double step_size){
    double excess = pa->total_registered - bandwidth;

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