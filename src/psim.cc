#include <iostream>
#include "psim.h"
#include "network.h"
#include "protocol.h"
#include "matplotlibcpp.h"
#include "spdlog/spdlog.h"

using namespace psim;

namespace plt = matplotlibcpp;

int Network::bottleneck_counter = 0;
static int simulation_counter = 0; 



PSim::PSim() {
    this->timer = 0;  
    this->total_task_count = 0;
    this->finished_task_count = 0;
    this->step_size = GConf::inst().step_size;
    
    this->traffic_gen = new TrafficGen(0.5);

    if (GConf::inst().network_type == "fattree"){
        this->network = new FatTreeNetwork();
    } else if (GConf::inst().network_type == "bigswitch"){
        this->network = new BigSwitchNetwork();
    } else {
        spdlog::error("Unknown network type: {}", GConf::inst().network_type);
        exit(1);
    }
} 

void PSim::add_protocol(Protocol *protocol){
    this->protocols.push_back(protocol);
    this->total_task_count += protocol->total_task_count;
}

PSim::~PSim() {
    delete this->network;
    for (auto protocol : this->protocols) {
        delete protocol;
    }
}

void PSim::handle_task_completion(PTask *task) {
    task->end_time = timer;
    task->protocol->finished_task_count += 1;

    if (task->protocol->type == ProtocolType::MAIN_PROTOCOL) {
        this->finished_task_count += 1; 
    }
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
            flow->initiate(); 

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
            handle_task_completion(task);
            start_next_tasks(task);
            break;
        } default: {
            std::cout << "Unknown task type" << std::endl;
            break;
        }
    }

}
 

double PSim::make_progress_on_flows(double current_time, std::vector<Flow*> & step_finished_flows){
    double step_comm = 0; 
    
    network->reset_bottleneck_registers();        

    for (auto& flow : flows) {
        flow->register_rate_on_path(step_size);
    }

    network->compute_bottleneck_availability();

    for (auto& flow : flows) {
        step_comm += flow->make_progress(current_time, step_size); 
        
        if (flow->status == PTaskStatus::FINISHED) {
            step_finished_flows.push_back(flow);
        }
    }

    if (GConf::inst().record_bottleneck_history){
        for (auto& bn: network->bottlenecks){
            bn->total_register_history.push_back(bn->pa->total_registered);
            bn->total_allocated_history.push_back(bn->pa->total_allocated);
        }
    }

    flow_count_history.push_back(flows.size()); 


    return step_comm;
} 



double PSim::simulate() {


    simulation_counter += 1;
    
    for (auto protocol : this->protocols) {
        for (auto task : protocol->initiators) {
            this->start_task(task);
        }
    }

    int last_summary_timer = -1; 

    while (true) {
        // auto new_flows = traffic_gen->get_flows(timer);
        // for (auto flow : new_flows) {
        //     this->start_task(flow);
        // }

        std::vector<Flow *> step_finished_flows;
        std::vector<PComp *> step_finished_tasks; 

        double step_comm = make_progress_on_flows(timer, step_finished_flows);        
        double stop_comp = network->make_progress_on_machines(timer, step_size, step_finished_tasks);

        total_comm += step_comm;
        total_comp += stop_comp;

        comm_log.push_back(step_comm / step_size / GConf::inst().link_bandwidth);
        comp_log.push_back(stop_comp / step_size);


        if (int(timer) % 1000 == 0 and int(timer) != last_summary_timer) {
            last_summary_timer = int(timer);
            spdlog::info("Time: {}, Flows: {}, Tasks: {}, Progress:{}/{}", 
                          int(timer), flows.size(), compute_tasks.size(), 
                          this->finished_task_count, this->total_task_count);

            network->print_core_link_status();

            // this->log_flow_states(); 
            // network->log_bottleneck_registers();

            // for (auto& flow : this->flows) {
            //     spdlog::debug("Flow: {}, rank:{}, priority:{}, progress: {}/{}, registered: {}, allocated: {}", flow->id, flow->rank, flow->selected_priority, flow->progress, flow->size, flow->registered_rate, flow->bn_allocated_rate);
            // }
            // for (auto& bn: network->bottlenecks){
            //     spdlog::debug("Bottleneck: {}, total_register: {}, total_allocated: {}", bn->id, bn->pa->total_registered, bn->pa->total_allocated);
            // }
            // for (auto& protocol : this->protocols) {
            //     spdlog::debug("Protocol, Task Completion: {}/{}", protocol->finished_task_count, protocol->total_task_count);
            // }
        }

        for (auto& flow : step_finished_flows) {
            finished_flows.push_back(flow);
            handle_task_completion(flow);
            flow->finished(); 
            // TODO: this seems to be a very time consuming 
            // operation, as far as I can tell. Is there a better way?
            flows.erase(std::remove(flows.begin(), flows.end(), flow), flows.end());
            start_next_tasks(flow);
        }

        for (auto& task : step_finished_tasks) {
            this->finished_compute_tasks.push_back(task);
            handle_task_completion(task);


            // TODO: this seems to be a very time consuming operation. 
            compute_tasks.erase(std::remove(compute_tasks.begin(), compute_tasks.end(), task), compute_tasks.end());
            start_next_tasks(task);
        }
        


        
        bool all_finished = true;
        for (auto protocol : this->protocols) {
            if (protocol->finished_task_count < protocol->total_task_count) {
                all_finished = false;
                break;
            }
        }
        if (all_finished) {
            break;
        }


        spdlog::debug("Time: {}, Flows: {}, Tasks: {}, Progress:{}/{}", 
                      int(timer), flows.size(), compute_tasks.size(), 
                      this->finished_task_count, this->total_task_count);

        timer += step_size;
    }
    
    save_run_results();

    return timer;
}


void PSim::save_run_results(){
    if (GConf::inst().plot_graphs) {
        // plot the data with matplotlibcpp: comm_log, comp_log
        plt::figure_size(1200, 780);

        plt::plot(comm_log, {{"label", "Comm"}});
        plt::plot(comp_log, {{"label", "Comp"}});
        plt::legend();
        plt::savefig("out/resources.png", {{"bbox_inches", "tight"}});
        plt::clf();

        plt::plot(flow_count_history, {{"label", "Flow Count"}});
        plt::legend();
        plt::savefig("out/flow_count.png", {{"bbox_inches", "tight"}});
        plt::clf();


        if (GConf::inst().record_machine_history) {
            std::string mkdir_command = "mkdir -p " + GConf::inst().output_dir + "/machines";
            int ret = system(mkdir_command.c_str());

            for (auto& machine: network->machines) {
                plt::plot(machine->task_queue_length_history, {{"label", "Comm"}});
                plt::legend();
                std::string plot_name = "out/machines/machine_" + std::to_string(machine->name) + ".png";
                plt::savefig(plot_name, {{"bbox_inches", "tight"}});
                plt::clf();
            }
        }

        if (GConf::inst().record_bottleneck_history){
            std::string mkdir_command = "mkdir -p " + GConf::inst().output_dir + "/bottlenecks";
            int ret = system(mkdir_command.c_str());

            for (auto& bn: network->bottlenecks){
                plt::plot(bn->total_register_history, {{"label", "Registered"}});
                plt::plot(bn->total_allocated_history, {{"label", "Allocated"}});
                plt::legend();
                std::string plot_name = "out/bottlenecks/bottleneck_" + std::to_string(bn->id) + ".png";
                plt::savefig(plot_name, {{"bbox_inches", "tight"}});
                plt::clf();
            }
        }
    }

    int total_task_count = 0;
    int total_finished_task_count = 0;
    
    for (auto protocol : this->protocols) {
        total_task_count += protocol->total_task_count;
        total_finished_task_count += protocol->finished_task_count;
    }

    spdlog::info("Simulation Finished at {}", timer);
    spdlog::info("Timer: {}, Task Completion: {}/{}", timer, total_finished_task_count, total_task_count);
    spdlog::info("Comm: {}, Comp: {}", total_comm, total_comp);

    // for (auto& task: protocols[0]->tasks){
    //     spdlog::info("task {}: start_time: {}, end_time: {}", task->id, task->start_time, task->end_time);
    // }
}

