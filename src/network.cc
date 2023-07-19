
#include "network.h"
#include <iostream>

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

    return step_comp; 
}

void 
Network::reset_bottleneck_registers(){
    for (auto bottleneck : this->bottlenecks) {
        bottleneck->reset_register();
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


BigSwitchNetwork::BigSwitchNetwork(double iface_bandwidth): Network() {


    for (int i = 0; i < this->server_count; i++) {
        Machine *machine = get_machine(i);
    }

    this->switch_bottleneck = create_bottleneck(iface_bandwidth);
    
    for (int i = 0; i < this->server_count; i++) {
        Bottleneck *ds_bn = create_bottleneck(iface_bandwidth);
        this->server_bottlenecks_downstream[i] = ds_bn;

        Bottleneck *us_bn = create_bottleneck(iface_bandwidth);
        this->server_bottlenecks_upstream[i] = us_bn;
    }
}

BigSwitchNetwork::~BigSwitchNetwork() {
    
}

void BigSwitchNetwork::set_path(Flow* flow) {
    flow->path.push_back(server_bottlenecks_upstream[flow->src_dev_id]);
    // flow->path.push_back(switch_bottleneck);
    flow->path.push_back(server_bottlenecks_downstream[flow->dst_dev_id]);
}



FatTreeNetwork::FatTreeNetwork() : Network() {

    Bottleneck *bottleneck = create_bottleneck(40);
    
    for (int i = 0; i < 16; i++) {
        Machine *machine = get_machine(i);
    }
}

FatTreeNetwork::~FatTreeNetwork() {
    
}

void FatTreeNetwork::set_path(Flow* flow) {
    flow->path.push_back(bottlenecks[0]);
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
    total_register = 0;
    id = -1; 
}

Bottleneck::~Bottleneck() {
    
}

void Bottleneck::reset_register(){
    total_register = 0;
    total_allocated = 0; 
}

void Bottleneck::register_rate(double rate){
    total_register += rate;
}

double Bottleneck::get_allocated_rate(double registered_rate){
    double allocated_rate;

    if (total_register > bandwidth) {
        allocated_rate = registered_rate * bandwidth / total_register;
    } else {
        allocated_rate = registered_rate;
    }

    total_allocated += allocated_rate;

    return allocated_rate;
}

bool Bottleneck::should_drop(double step_size){

    double excess = total_register - bandwidth;

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