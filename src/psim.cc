#include <iostream>

#include "psim.h"

using namespace havij;

int Network::bottleneck_counter = 0;

#define step_size_constant 1
#define rate_increase_constant 1.1
#define initial_rate_constant 10 

//==============================================================================
//==============================================================================
//==============================================================================

Protocol::Protocol() {
    finished_task_count = 0; 
    total_task_count = 0; 
}

Protocol::~Protocol() {
    for (auto task : this->tasks) {
        delete task;
    }
}

HavijTask* Protocol::create_task(HavijTaskType type, int id) {
    HavijTask *task;

    switch (type) {
        case HavijTaskType::FLOW:
            task = new Flow();
            break;
        case HavijTaskType::COMPUTE:
            task = new ComputeTask();
            break;
        case HavijTaskType::EMPTY:
            task = new EmptyTask();
            break;
        default:
            std::cout << "Unknown task type" << std::endl;
            return nullptr; 
            break;
    }

    task->id = id;
    this->tasks.push_back(task);
    this->task_map[id] = task;
    this->total_task_count += 1;

    return task;
}

void Protocol::build_dependency_graph() {
    for (auto task : this->tasks) {
        for (auto next_task_id : task->next_task_ids) {
            HavijTask *next_task = this->task_map[next_task_id];
            task->add_to_next(next_task);
            next_task->is_initiator = false;
        }
    }

    for (auto task : this->tasks) {
        if (task->is_initiator) {
            this->initiators.push_back(task);
        }
    }

    // std::cout << "total tasks in protocol " << this->tasks.size() << std::endl;
}

void Protocol::export_graph(std::ofstream protocol_log){
    for (auto& task : this->tasks) {
        task->print_task_info(protocol_log);
    }
}



HavijSimulator::HavijSimulator() {
    srand(time(NULL));
    this->timer = 0;  
    this->step_size = step_size_constant;
    this->network = new BigSwitchNetwork(10);
    this->protocol = new Protocol();
}


HavijSimulator::~HavijSimulator() {
    delete this->network;
    delete this->protocol;
}


void HavijSimulator::start_next_tasks(HavijTask *task){

    for (auto next_task : task->next_tasks) {
        next_task->dep_left -= 1;

        if (next_task->dep_left == 0) {
            start_task(next_task);
        } 
    }
}


void HavijSimulator::start_task(HavijTask *task) {
    // std::cout << "Timer:" << timer << " starting task: " << task->id << std::endl;

    HavijTaskType type = task->get_type();

    switch (type) {
        case HavijTaskType::FLOW: {
            task->status = HavijTaskStatus::RUNNING;
            task->start_time = timer;
            Flow* flow = (Flow *)task;
            this->flows.push_back(flow);

            flow->src = this->network->get_machine(flow->src_dev_id);
            flow->dst = this->network->get_machine(flow->dst_dev_id);
            this->network->set_path(flow);

            break;
        } case HavijTaskType::COMPUTE: {
            task->status = HavijTaskStatus::RUNNING;
            task->start_time = timer; 

            ComputeTask* compute_task = (ComputeTask *)task;
            
            compute_task->machine = this->network->get_machine(compute_task->dev_id);
            this->compute_tasks.push_back(compute_task);
            compute_task->machine->task_queue.push(compute_task);
            
            break;
        } case HavijTaskType::EMPTY: {
            task->status = HavijTaskStatus::FINISHED;
            task->start_time = timer;
            task->end_time = timer; 

            this->protocol->finished_task_count += 1;
            start_next_tasks(task);
            break;
        } default: {
            std::cout << "Unknown task type" << std::endl;
            break;
        }
    }

}
 

double HavijSimulator::make_progress_on_flows(std::vector<Flow*> & step_finished_flows){
    double step_comm = 0; 
    
    network->reset_bottleneck_registers();        

    for (auto& flow : flows) {
        flow->register_rate_on_path(step_size);
    }

    for (auto& flow : flows) {
        step_comm += flow->make_progress(step_size); 
        
        if (flow->status == HavijTaskStatus::FINISHED) {
            step_finished_flows.push_back(flow);
        }
    }

    return step_comm;
} 

static int simulation_counter = 0; 

double HavijSimulator::simulate() {
    simulation_counter += 1;
    std::string path = "logs/protocol_log_" + std::to_string(simulation_counter) + ".txt";
    std::ofstream simulation_log;
    simulation_log.open(path);

    for (auto task : this->protocol->initiators) {
        this->start_task(task);
    }

    double total_comm = 0; 
    double total_comp = 0;

    while (true) {

        std::vector<Flow *> step_finished_flows;
        std::vector<ComputeTask *> step_finished_tasks; 

        double step_comm = this->make_progress_on_flows(step_finished_flows);        
        double stop_comp = network->make_progress_on_machines(step_size, step_finished_tasks);

        total_comm += step_comm;
        total_comp += stop_comp;

        for (auto& flow : step_finished_flows) {
            finished_flows.push_back(flow);
            flow->end_time = timer;
            this->protocol->finished_task_count += 1;
            // TODO: this seems to be a very time consuming 
            // operation, as far as I can tell. Is there a better way?
            flows.erase(std::remove(flows.begin(), flows.end(), flow), flows.end());
            start_next_tasks(flow);
        }

        for (auto& task : step_finished_tasks) {
            this->finished_compute_tasks.push_back(task);
            task->end_time = timer;
            this->protocol->finished_task_count += 1;
            // TODO: this seems to be a very time consuming operation. 
            compute_tasks.erase(std::remove(compute_tasks.begin(), compute_tasks.end(), task), compute_tasks.end());
            start_next_tasks(task);
        }
        
        timer += step_size;

        if (true) {
            simulation_log << "Time: " << timer << std::endl; 
            simulation_log << "Task Completion: " << this->protocol->finished_task_count;
            simulation_log << "/" << this->protocol->total_task_count;
            simulation_log << std::endl;
            simulation_log << "Comp: " << stop_comp << "/" << total_comp << std::endl; 
            simulation_log << "Comm: " << step_comm << "/" << total_comm << std::endl;

            for (auto& bn: network->bottlenecks){
                simulation_log << "BN: " << std::setw(3) << bn->id << ":"; 
                simulation_log << " bw: " << std::setw(10) << bn->bandwidth << ", ";
                simulation_log << " reg: " << std::setw(10) << bn->total_register << ", ";
                simulation_log << " alloc: " << std::setw(10) << bn->total_allocated << ", ";
                simulation_log << " util: " << std::setw(10) << bn->total_allocated / bn->bandwidth << ", ";
                simulation_log << std::endl; 
            }
            simulation_log << "--------------------------------------";
            simulation_log << std::endl;
        }

        if (flows.size() == 0 && compute_tasks.size() == 0) {
            break;
        }
    }

    // std::cout << "Timer: " << timer << ", Task Completion: " << this->protocol->finished_task_count << "/" << this->protocol->total_task_count << ", Comm: " << total_comm << ", Comp: " << total_comp << std::endl;


    // this->protocol->export_graph(simulation_log);
    simulation_log.close();

    return timer;
}


//==============================================================================
//==============================================================================
//==============================================================================

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
                                        std::vector<ComputeTask*> & step_finished_tasks){
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


havij::Machine* Network::get_machine(int name){
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

    iface_bandwidth = iface_bandwidth / 1000.0 / 1000.0 * 8; 

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

//==============================================================================


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

havij::Machine::Machine(int name) {
    this->name = name;
}

havij::Machine::~Machine() {
    
}

double havij::Machine::make_progress(double step_size, std::vector<ComputeTask*> & step_finished_tasks) {
    double step_comp = 0; 

    if (this->task_queue.empty()) {
        return 0; 
    }
    
    ComputeTask* compute_task = this->task_queue.front();
    
    compute_task->progress += step_size;
    step_comp += step_size;

    if (compute_task->progress >= compute_task->size) {
        step_comp -= (compute_task->progress - compute_task->size);
        compute_task->progress = compute_task->size;

        this->task_queue.pop();
        compute_task->status = HavijTaskStatus::FINISHED;
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


//==============================================================================
//==============================================================================
//==============================================================================

HavijTask::HavijTask() {
    is_initiator = true; 
    status = HavijTaskStatus::BLOCKED;
    dep_left = 0;
    start_time = 0; 
    end_time = 0; 
}


HavijTask::~HavijTask() {
    
}

void HavijTask::add_next_task_id(int id) {
    this->next_task_ids.push_back(id);
}

void HavijTask::add_to_next(HavijTask *task) {
    this->next_tasks.push_back(task);
    task->dep_left += 1;
}


EmptyTask::EmptyTask() : HavijTask() {
    
}

EmptyTask::~EmptyTask() {
    
}

void 
EmptyTask::print_task_info(std::ostream& os){
    os << "AllR ";

    // fill the space with zeros
    os << "[" << std::setw(5) << std::setfill('0') 
                << this->id << "]";

    os << " next ";


    for (auto next_task : this->next_task_ids) {
        os << "[" << std::setw(5) << std::setfill('0') 
                    << next_task << "] ";
    }

    os << " size " << 0; 
    os << " dev "  << 0;

    os << std::endl;
}

Flow::Flow() : HavijTask() {
    progress = 0;
    current_rate = initial_rate_constant; 
}

Flow::~Flow() {
    
}

void Flow::register_rate_on_path(double step_size){
    double completion_rate = (size - progress) / step_size;
    registered_rate = std::min(completion_rate, current_rate);

    for (auto& bottleneck : this->path) {
        bottleneck->register_rate(registered_rate);
    }
}

void Flow::update_rate(double step_size) {
    bool should_drop = false;

    for (auto bottleneck : this->path) {
        if (bottleneck->should_drop(step_size)) {
            should_drop = true;
            break;
        }
    }

    if (should_drop) {
        current_rate /= 2;
    } else {
        double multipier = pow(rate_increase_constant, step_size);
        current_rate = current_rate * multipier;
    }

}

double Flow::make_progress(double step_size) {
    double allocated_rate = std::numeric_limits<double>::max();
    for (auto bottleneck : this->path) {
        double bn_rate = bottleneck->get_allocated_rate(registered_rate);
        allocated_rate = std::min(allocated_rate, bn_rate);
    }
    current_rate = allocated_rate;

    double step_progress = allocated_rate * step_size;
    progress += step_progress;
    
    if (progress >= size) {
        progress = size; 
        status = HavijTaskStatus::FINISHED;
    }

    update_rate(step_size);

    return step_progress;
}


void 
Flow::print_task_info(std::ostream& os){
    os << "Comm ";

    // fill the space with zeros
    os << "[" << std::setw(5) << std::setfill('0') 
                << this->id << "]";

    os << " next ";

    for (auto next_task : this->next_task_ids) {
        os << "[" << std::setw(5) << std::setfill('0') 
                    << next_task << "] ";
    }

    os << " size " << this->size;
    os << " from " << this->src_dev_id; 
    os << " to " << this->dst_dev_id;

    os << std::endl;
}

ComputeTask::ComputeTask() : HavijTask() {
    progress = 0; 
}


ComputeTask::~ComputeTask() {
    
}

void 
ComputeTask::print_task_info(std::ostream& os){
    std::string task_type_str = "Forw";
    os << task_type_str << " ";

    // fill the space with zeros
    os << "[" << std::setw(5) << std::setfill('0') 
                << this->id << "]";

    os << " next ";

    for (auto next_task : this->next_task_ids) {
        os << "[" << std::setw(5) << std::setfill('0') 
                    << next_task << "] ";
    }

    os << " size " << this->size; 
    os << " dev "  << this->dev_id;

    os << std::endl;
}


