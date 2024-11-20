#include <iostream>
#include "psim.h"
#include "network.h"
#include "protocol.h"
#include "protocol_builder.h"
#include "matplotlibcpp.h"
#include "spdlog/spdlog.h"
#include "spdlog/fmt/ranges.h"
#include <boost/algorithm/string.hpp>
#include "gcontext.h"

using namespace psim;

namespace plt = matplotlibcpp;

static int simulation_counter = 0;

PSim::PSim() {
    this->timer = 0;
    this->total_task_count = 0;
    this->finished_task_count = 0;
    this->step_size = GConf::inst().step_size;
    this->traffic_gen = new TrafficGen(0.5);

    switch(GConf::inst().network_type){
        case NetworkType::FAT_TREE:
            this->network = new FatTreeNetwork();
            break;

        case NetworkType::BIG_SWITCH:
            this->network = new BigSwitchNetwork();
            break;

        case NetworkType::LEAF_SPINE:
            this->network = new LeafSpineNetwork();
            break;

        default:
            spdlog::error("Unknown network type: {}", int(GConf::inst().network_type));
            exit(1);
    }
}

void PSim::add_protocol(Protocol *protocol){
    this->protocols.push_back(protocol);
    this->total_task_count += protocol->total_task_count;
}

void PSim::inform_network_of_protocols() {
    network->integrate_protocol_knowledge(protocols);
}

void PSim::add_protocols_from_input(){
    
    std::vector<std::string> protocol_file_names;
    boost::split(protocol_file_names, GConf::inst().protocol_file_name, boost::is_any_of(","));

    for (auto protocol_file_name : protocol_file_names) {
        Protocol *proto = nullptr; 

        if (protocol_file_name == "build-ring") {
            proto = ring_allreduce(GConf::inst().machine_count,
                                   GConf::inst().link_bandwidth * 10, 
                                   2);  
        } else if (protocol_file_name == "build-all-to-all") {
            proto = build_all_to_all(GConf::inst().machine_count,
                                     GConf::inst().link_bandwidth * 10, 
                                     2);
        } else if (protocol_file_name == "periodic-test") {
            proto = build_periodic_data_parallelism(); 
        } else if (protocol_file_name == "periodic-test-simple") {
            proto = build_periodic_simple(); 
        } else if (protocol_file_name == "nethint-test"){
            proto = build_nethint_test();
        } else {
            std::string path = GConf::inst().protocol_file_dir + "/" + protocol_file_name;
            proto = load_protocol_from_file(path);
        }


        proto->build_dependency_graph();

        for (int i = 0; i < 1000; i ++) {
            proto->build_dependency_graph(); 
        }
        
        if (GConf::inst().export_dot){
            proto->export_dot(protocol_file_name);
        }

        this->add_protocol(proto);
    }
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

void PSim::start_next_tasks(PTask *task, bool start_in_next_timestep){

    for (auto next_task : task->next_tasks) {
        next_task->dep_left -= 1;

        if (next_task->dep_left == 0) {
            start_task(next_task, start_in_next_timestep);
        }
    }
}


void PSim::start_task(PTask *task, bool start_in_next_timestep) {
    // std::cout << "Timer:" << timer << " starting task: " << task->id << std::endl;

    PTaskType type = task->get_type();

    switch (type) {
        case PTaskType::FLOW: {
            task->status = PTaskStatus::RUNNING;
            task->start_time = timer;
            if (start_in_next_timestep) {
                task->start_time += step_size;
            }

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
            if (start_in_next_timestep) {
                task->start_time += step_size;
            }

            PComp* compute_task = (PComp *)task;

            compute_task->machine = this->network->get_machine(compute_task->dev_id);
            this->compute_tasks.push_back(compute_task);
            compute_task->machine->task_queue.push(compute_task);
            break;

        } case PTaskType::EMPTY: {
            task->status = PTaskStatus::FINISHED;
            task->start_time = timer;

            EmptyTask* empty_task = (EmptyTask *)task;
            if (empty_task->print_on_exec){
                double time_to_print = 0; 

                if (timer < step_size){
                    time_to_print = 0;
                } else {
                    time_to_print = timer + step_size;
                }

                spdlog::critical("[{}]: {}", time_to_print, empty_task->print_message);
            }

            handle_task_completion(task);

            if (start_in_next_timestep) {
                start_next_tasks(task, true);
            } else {
                start_next_tasks(task, false);
            }

            break;

        } default: {
            std::cout << "Unknown task type" << std::endl;
            break;
        }
    }

}



double PSim::simulate() {
    // 


    simulation_counter += 1;

    // last time we've logged a summary.
    int last_summary_timer = -1;

    for (auto protocol : this->protocols) {
        for (auto task : protocol->initiators) {
            this->start_task(task, false);
        }
    }


    // THE main simulator loop. everything happens in here. 
    while (true) {
        
        // auto new_flows = traffic_gen->get_flows(timer);
        // for (auto flow : new_flows) {
        //     this->start_task(flow);
        // }

        std::vector<Flow *> step_finished_flows;
        std::vector<PComp *> step_finished_tasks;
        
        double this_step_step_size = GConf::inst().step_size;

        if (GConf::inst().adaptive_step_size){
            this_step_step_size = GConf::inst().adaptive_step_size_max;

            for (auto& flow: network->flows){
                double remaining_time_estimate = flow->crude_remaining_time_estimate(); 
                // spdlog::critical("flow {} remaining time estimate: {}", flow->id, remaining_time_estimate);
                if (remaining_time_estimate < this_step_step_size){
                    this_step_step_size = remaining_time_estimate;
                }    
            }
            for (auto& task: compute_tasks){
                double remaining_time_estimate = task->crude_remaining_time_estimate(); 
                // spdlog::critical("task {} remaining time estimate: {}", task->id, remaining_time_estimate);
                if (remaining_time_estimate < this_step_step_size){
                    this_step_step_size = remaining_time_estimate;
                }    
            }

            double min_step_size = GConf::inst().adaptive_step_size_min;

            if (this_step_step_size < min_step_size){
                this_step_step_size = min_step_size;
            }
        }

        spdlog::debug("step size: {}", this_step_step_size);

        double job_progress[10]; 
        for (int i = 0; i < 10; i++){
            job_progress[i] = 0;
        }
        double job_progress_through_core[10]; 
        for (int i = 0; i < 10; i++){
            job_progress_through_core[i] = 0;
        }

        double step_comm = network->make_progress_on_flows(timer, this_step_step_size, step_finished_flows, 
                                                           job_progress, job_progress_through_core);

        double stop_comp = network->make_progress_on_machines(timer, this_step_step_size, step_finished_tasks);

        int timer_interval = GConf::inst().core_status_profiling_interval;

        if (int(timer) % timer_interval == 0 and int(timer) != last_summary_timer) {
            last_summary_timer = int(timer);
            network->record_link_status(timer);
        }

        // Remove the finished flows and start up the tasks that follow.
        for (auto& flow : step_finished_flows) {
            finished_flows.push_back(flow);
            handle_task_completion(flow);
            flow->finished();

            auto& flows = network->flows;
            flows.erase(std::remove(flows.begin(), flows.end(), flow), flows.end());

            start_next_tasks(flow, true);
        }

        // Remove the finished computation tasks and start up the tasks that follow.
        for (auto& task : step_finished_tasks) {
            this->finished_compute_tasks.push_back(task);
            handle_task_completion(task);
            compute_tasks.erase(std::remove(compute_tasks.begin(), compute_tasks.end(), task), compute_tasks.end());
            start_next_tasks(task, true);
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
        h.total_network_bw = network->total_network_bw();
        h.total_core_bw = network->total_core_bw();
        h.total_accelerator_capacity = GConf::inst().machine_count * this_step_step_size;

        for (int i = 0; i < 10; i++){
            h.job_progress[i] = job_progress[i];
            h.job_progress_through_core[i] = job_progress_through_core[i];
        }

        history.push_back(h);

        if ((int)timer % 1000 == 0){
            log_history_entry(h);
        }


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
        
        timer += this_step_step_size;
        
        if (all_finished) {
            break;
        }
    }

    mark_critical_path(); 
    save_run_results();

    return timer;
}

void PSim::log_flow_info(){
    if (GConf::inst().print_flow_progress_history) {
        for (Flow* flow: finished_flows){
            if (flow->lb_decision == -1) {
                continue; 
            }

            // bool outgoing = false; 
            // if (flow->src_dev_id > 3 and flow->src_dev_id < 8){
            //     outgoing = true; 
            // }

            int src_rack = ((CoreConnectedNetwork*)network)->server_loc_map[flow->src_dev_id].rack;
            int dst_rack = ((CoreConnectedNetwork*)network)->server_loc_map[flow->dst_dev_id].rack;

            std::string progress_history = "";

            for (double ph: flow->progress_history){
                // 2 digits after the decimal point.
                std::string item = fmt::format("{:.2f}", ph);
                progress_history += item + " ";
            }

            spdlog::warn("flow: {} jobid: {} iter: {} subflow: {} srcrack: {} dstrack: {} start: {} end: {} fct: {} core: {} stepsize: {} label: {} progress_history: {}", 
                flow->per_job_task_id, flow->jobid,
                flow->protocol_defined_iteration, 
                flow->protocol_defined_subflow_id,
                src_rack, dst_rack, 
                flow->start_time, flow->end_time, 
                flow->end_time - flow->start_time + step_size, flow->lb_decision, 
                step_size, flow->label_for_progress_graph, progress_history);
        }
    }
}

void PSim::mark_critical_path(){
    for (Protocol* protocol: protocols){
        // find the task or the tasks that finished last. 
        double max_finish_time = 0; 
        for (PTask* task: protocol->finishers){
            max_finish_time = std::max(max_finish_time, task->end_time);
        }

        for (PTask* task: protocol->finishers){
            if (task->end_time == max_finish_time) {
                traverse_critical_path(task); 
            }
        }


        auto& this_run_cp = GContext::this_run().is_on_critical_path; 

        for (PTask* task: protocol->tasks){
            if (task->is_on_critical_path){
                this_run_cp[task->id] = true; 
            } else {
                this_run_cp[task->id] = false; 
            } 
        }

        // currently not supporting multiple protocols, since the ids 
        // of the tasks will collide. sorry for the stupid design.
        break;
    }




}

void PSim::traverse_critical_path(PTask* task) {
    if (task->is_on_critical_path) {
        return;
    }

    task->is_on_critical_path = true;

    double max_finish_time = 0; 
    for (PTask* next_task: task->prev_tasks){
        max_finish_time = std::max(max_finish_time, next_task->end_time);
    }

    for (PTask* prev_task: task->prev_tasks){
        if (prev_task->end_time == max_finish_time) {
            traverse_critical_path(prev_task); 
        }
    }
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
    spdlog::info("Total Link Bandwidth: {}", h.total_network_bw);
    spdlog::info("Total Core Bandwidth: {}", h.total_core_bw);
    spdlog::info("Total Accelerator Capacity: {}", h.total_accelerator_capacity);
    spdlog::info("------------------------------------------------------------");
}



// a function that receives a names and fields, plots them and saves them to a file.
// any number of such inputs can be given to the function.
void PSim::draw_plots(std::initializer_list<std::pair<std::string, std::function<double(history_entry)>>> plots, 
                      int smoothing) {


    if (smoothing == auto_smooth) {
        smoothing = history.size() / 500;
    }

    plt::figure_size(1200, 780);

    std::string plot_name;

    for (auto& plot: plots){
        auto field = plot.second;
        auto name = plot.first;

        std::vector <double> data;
        for (auto& h: history){
            data.push_back(field(h));
        }

        if (smoothing > 1) {
            if (data.size() < smoothing) {
                smoothing = data.size();
            }
            
            std::vector<double> smoothed_data;
            for (int i = 0; i < data.size(); i++) {
                double sum = 0;
                for (int j = 0; j < smoothing and i + j < data.size(); j++) {
                    sum += data[i + j];
                }
                smoothed_data.push_back(sum / smoothing);
            }
            data = smoothed_data;
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


void PSim::log_results() {

    spdlog::critical("run number: {}", GContext::this_run().run_number);

    spdlog::critical("psim time: {}", timer);


    if (this->protocols.size() != 1) {
        spdlog::error("the rest of log_results is only supported for single protocol runs");
        return;
    }

    double average_fct = 0;
    double average_flow_size = 0;
    double average_flow_bw = 0;
    double average_flow_path_length = 0;

    for (auto flow : finished_flows) {
        average_fct += flow->end_time - flow->start_time;
        average_flow_size += flow->size;
        average_flow_bw += (flow->size) / (flow->end_time - flow->start_time + step_size);
        average_flow_path_length += flow->path.size();
    }

    average_fct /= finished_flows.size();
    average_flow_size /= finished_flows.size();
    average_flow_bw /= finished_flows.size();
    average_flow_path_length /= finished_flows.size();

    spdlog::critical("average_fct: {}", average_fct);
    spdlog::critical("average_flow_size: {}", average_flow_size);
    spdlog::critical("average_flow_bw: {:03.2f}", average_flow_bw);
    spdlog::critical("average_flow_path_length: {}", average_flow_path_length);

    std::vector<double> average_rates;
    auto& this_run = GContext::this_run();
    for (auto& kv : this_run.average_rate) {
        average_rates.push_back(kv.second);
    }
    std::sort(average_rates.begin(), average_rates.end());
    std::vector<double> percentiles; 
    for (int i = 0; i < 100; i+=10) {
        int index = int(average_rates.size() * i / 100); 
        percentiles.push_back(average_rates[index]);
    }
    // spdlog::critical("average rates \%ile: {}", percentiles);




    int critical_path_count = 0;
    int non_critical_path_count = 0;
    for(Protocol* protocol: protocols){
        for (PTask* task: protocol->tasks){
            if (task->get_type() != PTaskType::FLOW){
                continue;
            }

            if (task->is_on_critical_path){
                critical_path_count += 1;
            } else {
                non_critical_path_count += 1;
            }
        }
        break; 
    }

    spdlog::critical("critical flows: {}, non-critical flows: {}", 
                     critical_path_count, non_critical_path_count);



    int bottlenecked_by_intermediate_count = 0; 
    int bottlenecked_by_srcdst_count = 0;
    int bottlenecked_by_both = 0; 

    for (Flow* flow: finished_flows){
        int intermediate_count = flow->bottlenecked_by_intermediate_count;
        int srcdst_count = flow->bottlenecked_by_srcdst_count;
        int total = intermediate_count + srcdst_count;

        if (intermediate_count > total * 0.9){
            bottlenecked_by_intermediate_count += 1;
        } else if (srcdst_count > total * 0.9){
            bottlenecked_by_srcdst_count += 1;
        } else {
            bottlenecked_by_both += 1;
        }
    }

    spdlog::critical("flows limited by core: {}, by src/dst: {}, both: {}", 
        bottlenecked_by_intermediate_count, bottlenecked_by_srcdst_count, bottlenecked_by_both);


    std::vector<int> core_choices(network->core_load_balancer->item_count, 0);
    std::vector<int> cp_core_choices(network->core_load_balancer->item_count, 0);
    std::vector<int> core_choices_size(network->core_load_balancer->item_count, 0);

    for (auto& kv: this_run.core_decision){
        core_choices[kv.second] += 1;
        double flow_size = ((Flow*) (this->protocols[0]->task_map[kv.first]))->size;
        core_choices_size[kv.second] += flow_size;

        if (this_run.is_on_critical_path[kv.first]){
            cp_core_choices[kv.second] += 1;
        }
    }
    spdlog::critical("core choices: {}", core_choices);
    spdlog::critical("core choices size: {}", core_choices_size);
    spdlog::critical("critical path core choices: {}", cp_core_choices);



    spdlog::critical("-------------------------------------------------------");
}


struct regret_entry {
    int flow_id;
    int old_decision; 
    int core_decision;
    double regret_score;
};


void PSim::measure_regret() {
    int prof_interval = GConf::inst().core_status_profiling_interval; 
    double step_size = GConf::inst().step_size;
    LBScheme lb_scheme = GConf::inst().lb_scheme;
    NetworkType network_type = GConf::inst().network_type;
    RegretMode regret_mode = GConf::inst().regret_mode;

    if (regret_mode == RegretMode::NONE) {
        return; 
    }

    if (not GConf::inst().profile_core_status){
        spdlog::critical("core status profiling must be enabled for regret measurement");
        return;
    }

    if (prof_interval != (int)step_size) {
        spdlog::critical("prof_interval and step_size must be the same");
        return;
    }
    if (network_type != NetworkType::LEAF_SPINE){
        spdlog::critical("regret measurement is only supported for leaf-spine networks");
        return;
    }

    auto& this_run = GContext::this_run();
    LeafSpineNetwork* lsnetwork = (LeafSpineNetwork*) this->network;
    LoadBalancer* lb = network->core_load_balancer;

    auto& link_up_map = lb->link_up_map;
    auto& link_down_map = lb->link_down_map; 
    int upper_level_items = lb->item_count; 
    int lower_level_items = lb->lower_item_count; 

    double max_score = -1;
    double max_score_new_core = -1; 
    Flow* max_score_flow = nullptr; 
    int repath_count = 0; 

    std::vector<regret_entry> regret_entries;

    for (Flow* flow: finished_flows) {

        if (regret_mode == RegretMode::CRITICAL){
            // we only want to do this for critical path flows.
            if (not flow->is_on_critical_path) {
                continue; 
            }   
        } 

        if (flow->size < 1) {
            continue;
        }

        int core_decision = this_run.core_decision[flow->id]; 
        double average_rate = this_run.average_rate[flow->id];
        double actual_flow_fct = flow->end_time - flow->start_time;
        auto prof_limits = get_prof_limits(flow->start_time, flow->end_time);

        int src_lower_item = lsnetwork->server_loc_map[flow->src_dev_id].rack;
        int dst_lower_item = lsnetwork->server_loc_map[flow->dst_dev_id].rack;

        // only consider flows that go through the core.
        if (src_lower_item == dst_lower_item) {
            continue;
        }

        double max_regret = -1; 
        double max_regret_core = -1;
        double max_regret_fct = -1;  

        for (int c = 0; c < upper_level_items; c++) {
            
            if (c == core_decision) {
                continue;
            }

            std::vector<Bottleneck*> path = flow->path;

            path[1] = link_up_map[std::make_pair(src_lower_item, c)];
            path[2] = link_down_map[std::make_pair(dst_lower_item, c)];

            double flow_remaining_size = flow->size;  
            int t = prof_limits.first;
            
            for (;flow_remaining_size > 0; t += prof_interval) {
                
                double min_link_availability = std::numeric_limits<double>::max();

                for (auto& link: path) {
                    double util = this_run.network_status[t].link_loads[link->id];
                    double capacity = link->bandwidth;

                    double link_availability = (capacity - util); //+ (util / (flow_count + 1));

                    min_link_availability = std::min(min_link_availability, link_availability);

                    // spdlog::critical("link: {}, util: {}, flow_count: {}, capacity: {}, availability: {}", 
                                    //  link->id, util, flow_count, capacity, link_availability);
                }

                flow_remaining_size -= (min_link_availability * prof_interval);
                if (flow_remaining_size < 0) {
                    t -= int(flow_remaining_size / min_link_availability);
                }
            }

            double new_fct = t - flow->start_time;
            double regret = actual_flow_fct / new_fct;
            
            // spdlog::critical("critflow {} with size: {}, core {}->{}, fct: {}->{}, regret: {:.2f}", 
            //                  flow->id, flow->size,
            //                  core_decision, c, 
            //                  actual_flow_fct, new_fct, 
            //                  regret);

            if (regret > max_regret) {
                max_regret_core = c;
                max_regret_fct = new_fct;
                max_regret = regret;
            }
        }

        if (max_regret == -1) {
            continue;
        }
        
        double chance = 1; 
        // double chance = rand() / double(RAND_MAX);
        // double regret_score = max_regret * flow->size;
        // double regret_score = max_regret; 
        double regret_score = max_regret * chance;

        // spdlog::critical("normal score: {}, chance: {}, after chance: {}", 
        //                     max_regret * flow->size, chance, regret_score);


        // if (max_regret > 1.5) {
        //     GContext::save_decision(flow->id, max_regret_core);
        //     repath_count += 1; 
        // }

        regret_entry entry;
        entry.flow_id = flow->id;
        entry.old_decision = core_decision;
        entry.core_decision = max_regret_core;
        entry.regret_score = regret_score;
        regret_entries.push_back(entry);


        if (regret_score > max_score) {
            max_score = regret_score;
            max_score_flow = flow;
            max_score_new_core = max_regret_core;
        }
        // spdlog::critical("critflow {} with size: {}, core {}->{}, fct: {}->{}, regret: {:.2f}", 
        //                  flow->id, flow->size,
        //                  core_decision, max_regret_core, 
        //                  actual_flow_fct, max_regret_fct, 
        //                  max_regret);
    }

    // spdlog::critical("max regret score: {:.2f}, flow: {}, core: {} -> {}, transfer: {} to {}", 
    //                  max_score, 
    //                  max_score_flow->id, 
    //                  GContext::this_run().core_decision[max_score_flow->id],
    //                  max_score_new_core, 
    //                  max_score_flow->start_time, 
    //                  max_score_flow->end_time);

    // spdlog::critical("flow {} core {} to {}", max_score_flow->id, GContext::this_run().core_decision[max_score_flow->id], max_score_new_core); 

    int reported_regrets = std::min(10, (int) regret_entries.size());

    if (reported_regrets > 0) {
        // sort the flows based on their regret scores.
        std::sort(regret_entries.begin(), regret_entries.end(), 
            [](regret_entry a, regret_entry b) {
                return a.regret_score > b.regret_score;
            });


        // log the top 10 flows with the highest regret scores.
        for (int i = 0; i < reported_regrets; i++) {
            regret_entry entry = regret_entries[i];
            spdlog::critical("flow {} old core {} new core {} regret score {}", 
                            entry.flow_id, 
                            entry.old_decision, 
                            entry.core_decision, 
                            (int) entry.regret_score);
        }
    }
    


    // if (max_score_flow != nullptr){
    //     GContext::save_decision(max_score_flow->id, max_score_new_core);
    //     repath_count += 1; 
    // }

    // spdlog::critical("repath count: {}", repath_count);
    // spdlog::critical("-------------------------------------------------------");

}

void PSim::log_lb_decisions(){
    auto& this_run = GContext::this_run();
    auto& core_decisions = this_run.core_decision;

    for (auto& kv: core_decisions){
        int flow_id = kv.first;
        int core_id = kv.second;
        bool crit = this_run.is_on_critical_path[flow_id];
        spdlog::warn("flow {} core {} crit {}", flow_id, core_id, crit);
    }

    spdlog::warn("-------------------------------------------------------");
}


void PSim::save_run_results(){
    if (GConf::inst().plot_graphs) {
        // plot the data with matplotlibcpp: comm_log, comp_log

        // draw_plots({
        //     {"network-util", [](history_entry h){return h.total_bw_utilization / h.total_network_bw;}},
        //     {"accel-util", [](history_entry h){return h.step_comp / h.total_accelerator_capacity;}}
        // });

        // draw_plots({
        //     {"network-util", [](history_entry h){return h.total_bw_utilization / h.total_network_bw;}},
        //     {"core-util", [](history_entry h){return h.total_core_bw_utilization / h.total_core_bw;}},
        // });

        draw_plots({
            {"core-util", [](history_entry h){return h.total_core_bw_utilization / h.total_core_bw;}},
        });
        
        draw_plots({
            {"min-core-util", [](history_entry h){return h.min_core_link_bw_utilization;}},
            {"max-core-util", [](history_entry h){return h.max_core_link_bw_utilization;}},
        });

        // draw_plots({
        //     {"step_comp", [](history_entry h){return h.step_comm;}},
        // });

        // draw_plots({
        //     {"job1_progress", [](history_entry h){return h.job_progress[1];}},
        // });

        // draw_plots({
        //     {"job2_progress", [](history_entry h){return h.job_progress[2];}},
        // });

        // draw all together now 
        draw_plots({
            // {"step_comp", [](history_entry h){return h.step_comm;}},
            {"job1_progress", [](history_entry h){return h.job_progress_through_core[1];}},
            {"job2_progress", [](history_entry h){return h.job_progress_through_core[2];}},
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
