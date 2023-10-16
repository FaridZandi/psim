#include <iostream>
#include "psim.h"
#include "network.h"
#include "protocol.h"
#include "matplotlibcpp.h"
#include "spdlog/spdlog.h"
#include "context.h"

using namespace psim;

namespace plt = matplotlibcpp;

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
    } else if (GConf::inst().network_type == "leafspine"){
        this->network = new LeafSpineNetwork();
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
            this->network->flows.push_back(flow);

            flow->src = this->network->get_machine(flow->src_dev_id);
            flow->dst = this->network->get_machine(flow->dst_dev_id);
            this->network->set_path(flow, timer);
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
 


double PSim::simulate() {
    
    simulation_counter += 1;

    int last_summary_timer = -1; 
    
    for (auto protocol : this->protocols) {
        for (auto task : protocol->initiators) {
            this->start_task(task);
        }
    }

    // main loop
    while (true) {

        // auto new_flows = traffic_gen->get_flows(timer);
        // for (auto flow : new_flows) {
        //     this->start_task(flow);
        // }

        std::vector<Flow *> step_finished_flows;
        std::vector<PComp *> step_finished_tasks; 

        double step_comm = network->make_progress_on_flows(timer, step_finished_flows);        
        double stop_comp = network->make_progress_on_machines(timer, step_size, step_finished_tasks);
        

        int timer_interval = GConf::inst().core_status_profiling_interval; 

        if (int(timer) % timer_interval == 0 and int(timer) != last_summary_timer) {
            last_summary_timer = int(timer);
            network->record_core_link_status(timer);
        }

        for (auto& flow : step_finished_flows) {
            finished_flows.push_back(flow);
            handle_task_completion(flow);
            flow->finished();

            auto& flows = network->flows;  
            flows.erase(std::remove(flows.begin(), flows.end(), flow), flows.end());
            
            start_next_tasks(flow);
        }

        for (auto& task : step_finished_tasks) {
            this->finished_compute_tasks.push_back(task);
            handle_task_completion(task);
            compute_tasks.erase(std::remove(compute_tasks.begin(), compute_tasks.end(), task), compute_tasks.end());
            start_next_tasks(task);
        }
        


        history_entry h; 
        h.time = timer;
        h.flow_count = network->flows.size();
        h.step_finished_flows = step_finished_flows.size();
        h.comp_task_count = compute_tasks.size();
        h.step_finished_comp_tasks = step_finished_tasks.size();
        h.step_comm = step_comm;
        h.step_comp = stop_comp;
        h.total_bw_utilization = network->total_bw_utilization(); 
        h.total_core_bw_utilization = network->total_core_bw_utilization();
        h.min_core_link_bw_utilization = network->min_core_link_bw_utilization();
        h.max_core_link_bw_utilization = network->max_core_link_bw_utilization();
        h.total_link_bandwidth = network->total_link_bandwidth();
        h.total_accelerator_capacity = GConf::inst().machine_count * step_size;
        history.push_back(h);
        log_history_entry(h);


        spdlog::debug("Time: {}, Flows: {}, Tasks: {}, Progress:{}/{}", 
                      int(timer), network->flows.size(), compute_tasks.size(), 
                      this->finished_task_count, this->total_task_count);

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

        timer += step_size;
    }
    
    save_run_results();

    return timer;
}

void PSim::log_history_entry(history_entry& h){
    spdlog::info("Time: {}", h.time); 
    spdlog::info("Flows: {}", h.flow_count);
    spdlog::info("Step Finished Flows: {}", h.step_finished_flows);
    spdlog::info("Comp Tasks: {}", h.comp_task_count);
    spdlog::info("Step Finished Comp Tasks: {}", h.step_finished_comp_tasks);
    spdlog::info("Step Comm: {}", h.step_comm);
    spdlog::info("Step Comp: {}", h.step_comp);
    spdlog::info("Total BW Utilization: {}", h.total_bw_utilization);
    spdlog::info("Total Core BW Utilization: {}", h.total_core_bw_utilization);
    spdlog::info("Min Core Link BW Utilization: {}", h.min_core_link_bw_utilization);
    spdlog::info("Max Core Link BW Utilization: {}", h.max_core_link_bw_utilization);
    spdlog::info("Total Link Bandwidth: {}", h.total_link_bandwidth);
    spdlog::info("Total Accelerator Capacity: {}", h.total_accelerator_capacity);
    spdlog::info("------------------------------------------------------------");
}

// a function that receives a names and fields, plots them and saves them to a file. 
// any number of such inputs can be given to the function. 
void PSim::draw_plots(std::initializer_list<std::pair<std::string, std::function<double(history_entry)>>> plots){
    plt::figure_size(1200, 780);

    std::string plot_name; 

    for (auto& plot: plots){
        auto field = plot.second;
        auto name = plot.first;

        std::vector <double> data;
        for (auto& h: history){
            data.push_back(field(h));
        }
        plt::plot(data, {{"label", name}});
        
        plot_name += name + "|";
    }

    plt::legend();    

    // drop the last underscore
    plot_name.pop_back();
    plot_name = GConf::inst().output_dir + "/" + plot_name + ".png";
    plt::savefig(plot_name, {{"bbox_inches", "tight"}});
    plt::clf();
    plt::close(); 
    
}

void PSim::save_run_results(){
    if (GConf::inst().plot_graphs) {
        // plot the data with matplotlibcpp: comm_log, comp_log

        draw_plots({
            {"network-util", [](history_entry h){return h.total_bw_utilization / h.total_link_bandwidth;}},
            {"accel-util", [](history_entry h){return h.step_comp / h.total_accelerator_capacity;}}
        });

        draw_plots({
            {"network-util", [](history_entry h){return h.total_bw_utilization;}},
            {"core-util", [](history_entry h){return h.total_core_bw_utilization;}},
        });

        draw_plots({
            {"min-core-util", [](history_entry h){return h.min_core_link_bw_utilization;}},
            {"max-core-util", [](history_entry h){return h.max_core_link_bw_utilization;}},
        });


        if (GConf::inst().record_machine_history) {
            std::string mkdir_command = "mkdir -p " + GConf::inst().output_dir + "/machines";
            int ret = system(mkdir_command.c_str());

            for (auto& machine: network->machines) {
                plt::plot(machine->task_queue_length_history, {{"label", "Comm"}});
                plt::legend();
                std::string plot_name = "out/machines/machine_" + std::to_string(machine->name) + ".png";
                plt::savefig(plot_name, {{"bbox_inches", "tight"}});
                plt::clf();
                plt::close(); 
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
                plt::close(); 
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

    // for (auto& task: protocols[0]->tasks){
    //     spdlog::info("task {}: start_time: {}, end_time: {}", task->id, task->start_time, task->end_time);
    // }
}

