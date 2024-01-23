
#include <iostream>
#include <cassert>
#include <algorithm>
#include <limits>
#include <set>

#include "spdlog/spdlog.h"

#include "network.h"
#include "gcontext.h"

using namespace psim;

Network::Network() {
    Bottleneck::bottleneck_counter = 0;
    core_load_balancer = nullptr;
}

Network::~Network() {
    for (auto bottleneck : this->bottlenecks) {
        delete bottleneck;
    }

    for (auto machine : this->machines) {
        delete machine;
    }
}

void Network::integrate_protocol_knowledge(std::vector<Protocol*>& protocols) {
    std::map<int, std::vector<Flow*>> src_flows;
    std::vector<Flow*> protocol_flows;
    int src;
    for (size_t p_idx = 0; p_idx < protocols.size(); p_idx++) {
        protocol_flows = protocols[p_idx]->get_flows();
        for (size_t f_idx = 0; f_idx < protocol_flows.size(); f_idx++) {
            src = get_source_for_flow(protocol_flows[f_idx]);
            if (src_flows.find(src) == src_flows.end()) {
                src_flows[src] = std::vector<Flow*>();
            }
            src_flows[src].push_back(protocol_flows[f_idx]);
        }
    }
    core_load_balancer->add_flow_info(src_flows);
}


int Network::make_progress_on_machines(int current_quantum, int step_quantums,
                                       std::vector<PComp*> & step_finished_tasks){
    
    int step_comp_quantums = 0;

    for (auto& machine : this->machines) {
        step_comp_quantums += machine->make_progress(current_quantum, step_quantums, step_finished_tasks);
    }

    if (GConf::inst().record_machine_history){
        for (auto& machine: this->machines) {
            machine->task_queue_length_history.push_back(machine->task_queue.size());
        }
    }

    return step_comp_quantums;
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


double Network::total_network_bw() {
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


double Network::total_core_bw() {
    return 0;
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



int Network::make_progress_on_flows(int current_quantum, int step_quantums,
                                       std::vector<Flow*> & step_finished_flows){

    int step_packets_sent = 0;

    reset_bottleneck_registers();

    for (auto& flow : flows) {
        flow->register_rate_on_path(step_quantums);
    }

    compute_bottleneck_allocations();

    for (auto& flow : flows) {
        step_packets_sent += flow->make_progress(current_quantum, step_quantums);

        if (flow->status == PTaskStatus::FINISHED) {
            step_finished_flows.push_back(flow);
        }
    }

    if (GConf::inst().record_bottleneck_history){
        for (auto& bn: bottlenecks){
            bn->total_register_history.push_back(bn->bwalloc->total_registered);
            bn->total_allocated_history.push_back(bn->bwalloc->total_allocated);
        }
    }

    if (core_load_balancer != nullptr) {
        core_load_balancer->update_state();
    }

    return step_packets_sent;
}


std::vector<int> Network::get_core_bottleneck_ids(){
   return std::vector<int>(); 
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

int BigSwitchNetwork::get_source_for_flow(Flow* flow) {
    return flow->src_dev_id;
}


//==============================================================================
//==============================================================================
//==============================================================================

psim::Machine::Machine(int name) {
    this->name = name;
}

psim::Machine::~Machine() {

}

int psim::Machine::make_progress(int current_quantum, int step_quantums,
                                    std::vector<PComp*> & step_finished_tasks) {

    if (this->task_queue.empty()) {
        return 0;
    }

    int step_comp_quantums = 0;
    int machine_remaining_quantums = step_quantums;

    while (not this->task_queue.empty() and machine_remaining_quantums > 0){
        PComp* compute_task = this->task_queue.front();

        if (compute_task->executed_quantum_count == 0){
            compute_task->start_quantum = current_quantum;
        }

        int task_remaining_quantums = compute_task->quantum_count - compute_task->executed_quantum_count;
        int quantums_to_execute = std::min(machine_remaining_quantums, task_remaining_quantums);

        compute_task->executed_quantum_count += quantums_to_execute;
        step_comp_quantums += quantums_to_execute;
        machine_remaining_quantums -= quantums_to_execute;
        task_remaining_quantums -= quantums_to_execute;

        if (task_remaining_quantums == 0) {
            this->task_queue.pop();
            compute_task->status = PTaskStatus::FINISHED;
            step_finished_tasks.push_back(compute_task);
        }
    }

    return step_comp_quantums;
}


int Bottleneck::bottleneck_counter = 0;


Bottleneck::Bottleneck(double bandwidth) {
    id = bottleneck_counter;
    bottleneck_counter += 1;

    // the bandwidth of the bottleneck given in Gbps. 
    // For 1342 bytes packets and 10us quantum, we are supposed to get 
    // 1 packet per quantum for every 1 Gbps of bandwidth.
    double bandwidth_gbps = bandwidth; 
    double quantums_per_second = 1000000 / TIME_Q_US; 
    double packets_per_second = (bandwidth_gbps * 1024 * 1024 * 1024) / (8 * PACKET_SIZE);
    this->packets_per_quantum = packets_per_second / quantums_per_second;

    // TODO: remove after testing 
    spdlog::critical("bandwidth_gbps: {}, quantums_per_second: {}, packets_per_second: {}, packets_per_quantum: {}", 
                     bandwidth_gbps, quantums_per_second, packets_per_second, this->packets_per_quantum); 
    
    this->current_flow_count = 0;
    this->current_flow_packet_count_sum = 0;
    this->load_metric = GConf::inst().load_metric;
    this->drop_chance_multiplier = GConf::inst().drop_chance_multiplier;

    setup_bwalloc();
}


double Bottleneck::get_load(LoadMetric load_metric_arg) {
    LoadMetric load_metric = this->load_metric;

    if (load_metric_arg != LoadMetric::DEFAULT){
        load_metric = load_metric_arg;
    }

    switch (load_metric) {
    case LoadMetric::REGISTERED:
        return bwalloc->total_registered;

    case LoadMetric::UTILIZATION:
        return bwalloc->utilized_bandwidth;

    case LoadMetric::ALLOCATED:
        return bwalloc->total_allocated;

    case LoadMetric::FLOWSIZE:
        return current_flow_packet_count_sum;

    case LoadMetric::FLOWCOUNT:
        return current_flow_count;

    default:
        spdlog::error("Invalid load metric");
        exit(1);
    }
}


Bottleneck::~Bottleneck() {
    delete bwalloc;
}


void Bottleneck::setup_bwalloc() {
    switch (GConf::inst().priority_allocator) {
        case PriorityAllocator::PRIORITY_QUEUE:
            bwalloc = new PriorityQueueBandwidthAllocator(packets_per_quantum);
            break;

        case PriorityAllocator::FIXED_LEVELS:
            bwalloc = new FixedLevelsBandwidthAllocator(packets_per_quantum);
            break;

        case PriorityAllocator::FAIR_SHARE:
            bwalloc = new FairShareBandwidthAllocator(packets_per_quantum);
            break;

        default:
            spdlog::error("Invalid priority allocator: {}", int(GConf::inst().priority_allocator));
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

        drop_prob *= drop_chance_multiplier;
        
        double rand_num = (double) rand() / RAND_MAX;
        if (rand_num < drop_prob) {
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
