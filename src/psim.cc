#include <iostream>
#include "psim.h"
#include "network.h"
#include "protocol.h"


using namespace psim;

int Network::bottleneck_counter = 0;
static int simulation_counter = 0; 



PSim::PSim() : 
    PSim(new Protocol()) {} 

PSim::PSim(Protocol* protocol) : protocol(protocol) {
    srand(time(NULL));
    this->timer = 0;  
    this->step_size = step_size_constant;
    this->network = new BigSwitchNetwork(10);
}


PSim::~PSim() {
    delete this->network;
    delete this->protocol;
}


void PSim::start_next_tasks(PTask *task){

    for (auto next_task : task->next_tasks) {
        next_task->dep_left -= 1;

        if (next_task->dep_left == 0) {
            start_task(next_task);
        } 
    }
}


void PSim::start_task(PTask *task) {
    // std::cout << "Timer:" << timer << " starting task: " << task->id << std::endl;

    PTaskType type = task->get_type();

    switch (type) {
        case PTaskType::FLOW: {
            task->status = PTaskStatus::RUNNING;
            task->start_time = timer;
            Flow* flow = (Flow *)task;
            this->flows.push_back(flow);

            flow->src = this->network->get_machine(flow->src_dev_id);
            flow->dst = this->network->get_machine(flow->dst_dev_id);
            this->network->set_path(flow);

            break;
        } case PTaskType::COMPUTE: {
            task->status = PTaskStatus::RUNNING;
            task->start_time = timer; 

            PComp* compute_task = (PComp *)task;
            
            compute_task->machine = this->network->get_machine(compute_task->dev_id);
            this->compute_tasks.push_back(compute_task);
            compute_task->machine->task_queue.push(compute_task);
            
            break;
        } case PTaskType::EMPTY: {
            task->status = PTaskStatus::FINISHED;
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
 

double PSim::make_progress_on_flows(std::vector<Flow*> & step_finished_flows){
    double step_comm = 0; 
    
    network->reset_bottleneck_registers();        

    for (auto& flow : flows) {
        flow->register_rate_on_path(step_size);
    }

    for (auto& flow : flows) {
        step_comm += flow->make_progress(step_size); 
        
        if (flow->status == PTaskStatus::FINISHED) {
            step_finished_flows.push_back(flow);
        }
    }

    return step_comm;
} 


double PSim::simulate() {
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
        std::vector<PComp *> step_finished_tasks; 

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

