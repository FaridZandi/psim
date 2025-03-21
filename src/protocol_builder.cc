#include <iostream>
#include <iomanip>
#include <vector>
#include <algorithm>
#include <limits>
#include "protocol.h"
#include "protocol_builder.h"
#include <cmath>
#include <fstream>
#include <sstream>
#include "gcontext.h"
#include "spdlog/spdlog.h"
#include "nlohmann/json.hpp"
#include <filesystem>
#include <cstdint>
#include <filesystem>

using namespace psim;

Protocol* 
psim::pipelinize_protocol(Protocol *proto, int num_replicas, bool tight_connections){
    Protocol *combined_protocol = new Protocol();
    
    std::map<int, std::vector<int>> task_id_to_replica_ids;

    for (PTask* ptask: proto->tasks) {
        for (int i = 0; i < num_replicas; i++){
            PTask *new_task = ptask->make_shallow_copy();
            combined_protocol->add_to_tasks(new_task);
            task_id_to_replica_ids[ptask->id].push_back(new_task->id);
        }
    }
    
    // add connections inside the replicas
    for (int i = 0; i < num_replicas; i++){
        for (PTask* ptask: proto->tasks) {
            int new_task_id = task_id_to_replica_ids[ptask->id][i];
            PTask *new_task = combined_protocol->task_map[new_task_id];

            for (PTask* next_task: ptask->next_tasks) {
                int next_new_task_id = task_id_to_replica_ids[next_task->id][i];
                new_task->add_next_task_id(next_new_task_id);
            }
        }
    }

    // add connections between replicas
    for (int i = 1; i < num_replicas; i++){

        if (tight_connections){
            for (PTask* ptask: proto->tasks) {
                int prev_task_id = task_id_to_replica_ids[ptask->id][i - 1];
                PTask *prev_task = combined_protocol->task_map[prev_task_id];
                int curr_task_id  = task_id_to_replica_ids[ptask->id][i];
                prev_task->add_next_task_id(curr_task_id);
            }
        } else {
            for (PTask* ptask: proto->initiators) {
                int prev_task_id = task_id_to_replica_ids[ptask->id][i - 1];
                PTask *prev_task = combined_protocol->task_map[prev_task_id];
                int curr_task_id  = task_id_to_replica_ids[ptask->id][i];
                prev_task->add_next_task_id(curr_task_id);
            }
        }
    }

    combined_protocol->build_dependency_graph();
    
    return combined_protocol;
}

Protocol* 
psim::super_simple_protocol(){
    Protocol *protocol = new Protocol();
    
    PComp* ptask1 = (PComp*)protocol->create_task(PTaskType::COMPUTE);
    ptask1->size = 100; 
    ptask1->dev_id = 0;

    Flow* ptask2 = (Flow*)protocol->create_task(PTaskType::FLOW);
    Flow* ptask3 = (Flow*)protocol->create_task(PTaskType::FLOW); 
    
    ptask2->size = 10000; 
    ptask2->src_dev_id = 0;
    ptask2->dst_dev_id = 1;

    ptask3->size = 10000;
    ptask3->src_dev_id = 0;
    ptask3->dst_dev_id = 2;

    PComp* ptask4 = (PComp*)protocol->create_task(PTaskType::COMPUTE);
    ptask4->size = 100;
    ptask4->dev_id = 1;

    PComp* ptask5 = (PComp*)protocol->create_task(PTaskType::COMPUTE);
    ptask5->size = 100;
    ptask5->dev_id = 2;

    ptask1->add_next_task_id(ptask2->id);
    ptask1->add_next_task_id(ptask3->id);

    ptask2->add_next_task_id(ptask4->id);
    ptask3->add_next_task_id(ptask5->id);

    return protocol;
}

Protocol* 
psim::simple_pipeline_protocol(int length){
    Protocol *protocol = new Protocol();

    int task_counter = 0;

    for (int i = 0; i < length; i++) {
        PComp *pc = (PComp*)protocol->create_task(PTaskType::COMPUTE, task_counter);
        pc->size = 100;
        pc->dev_id = i;
        task_counter += 1;
    }

    for (int i = 0; i < length - 1; i++) {
        Flow *flow = (Flow*)protocol->create_task(PTaskType::FLOW, task_counter);
        flow->src_dev_id = i;
        flow->dst_dev_id = i + 1;
        
        flow->add_next_task_id(i + 1);
        protocol->task_map[i]->add_next_task_id(flow->id);

        flow->size = 100;
        task_counter += 1;
    }

    return protocol;
}


Protocol* 
psim::build_random_protocol(int num_comp, int machine_count){
    Protocol *protocol = new Protocol();

    // create compute tasks 
    std::map<int, PComp *> task_map;
    
    int task_counter = 0;
    for (int i = 0; i < num_comp; i++) {
        PComp* pc = (PComp*)protocol->create_task(PTaskType::COMPUTE, task_counter);
        task_map[i] = pc;
        pc->size = rand() % 50;
        pc->dev_id = rand() % machine_count;
        task_counter += 1;
    }

    // reachable tasks 
    std::vector<int> reachable_tasks;
    reachable_tasks.push_back(0);

    // create enough connection such that all tasks are reachable
    for (int i = 1; i < num_comp; i++) {
        int connection_count = rand() % 3 + 1;
        connection_count = std::min(connection_count, (int)reachable_tasks.size());

        // get connection_count different random samples from reachable_tasks
        std::vector<int> samples;
        for (int j = 0; j < connection_count; j++) {
            int sample = rand() % reachable_tasks.size();
            while (std::find(samples.begin(), samples.end(), sample) != samples.end()) {
                sample = rand() % reachable_tasks.size();
            }
            samples.push_back(sample);
        }

        for (int j = 0; j < connection_count; j++) {
            int prev = samples[j];

            if (task_map[prev]->dev_id == task_map[i]->dev_id) {
                task_map[prev]->add_next_task_id(i);
            } else {
                Flow *flow = (Flow*)protocol->create_task(PTaskType::FLOW, task_counter);
                task_counter += 1;

                flow->src_dev_id = task_map[prev]->dev_id;
                flow->dst_dev_id = task_map[i]->dev_id;
                flow->size = rand() % 10000;

                task_map[prev]->add_next_task_id(flow->id);
                flow->add_next_task_id(task_map[i]->id);
            }
        }

        reachable_tasks.push_back(i);
    }

    return protocol;
}




Protocol* 
psim::load_protocol_from_file(std::string file_path){

    std::ifstream myfile (file_path);

    // check if file exists
    if(not myfile.good()){
        spdlog::error("protocol file not found. exiting.");
        spdlog::error("path: {}", file_path);
        exit(0);
    }

    Protocol *protocol = new Protocol();
    std::string line;
    int task_counter = 0;

    if (myfile.is_open()) {
        while (getline(myfile, line)) {
            std::vector <std::string> tokens;
            std::stringstream check1(line);
            std::string intermediate;
            
            while(getline(check1, intermediate, ' ')){
                if (intermediate == "") {
                    continue;
                }
                tokens.push_back(intermediate);
            }

            // lines in the format: 
            // AllR [02662] next [08710] [08711] [08712] [08713] [08714] [08715] [08716] [08717] [08718] [08719] [08720] [08721] [08722] [08723] [08724] [08725]  size 0 dev 0

            std::string task_type_str = tokens[0];
            PTaskType task_type;
            if (task_type_str == "AllR") {
                task_type = PTaskType::EMPTY;
            } else if (task_type_str == "Comm") {
                task_type = PTaskType::FLOW;
            } else if (task_type_str == "Forw" or task_type_str == "Back") {
                task_type = PTaskType::COMPUTE;
            } else {
                continue;
            }

            int task_id = std::stoi(tokens[1].substr(1, tokens[1].size() - 2));
            PTask *task = protocol->create_task(task_type, task_id);
            
            int i = 3;
            while (tokens[i][0] == '[') {
                int next_task_id = std::stoi(tokens[i].substr(1, tokens[i].size() - 2));
                task->add_next_task_id(next_task_id);
                i += 1;
            }

            if (task_type == PTaskType::COMPUTE) {

                PComp *compute_task = (PComp *)task;
                compute_task->size = std::stod(tokens[i + 1]);
                compute_task->dev_id = std::stoi(tokens[i + 3]);
                if (GConf::inst().shuffle_device_map){
                    compute_task->dev_id = GContext::get_device_shuffle_map(compute_task->dev_id);
                }

            } else if (task_type == PTaskType::FLOW) {

                Flow *flow = (Flow *)task;
                flow->size = std::stod(tokens[i + 1]);
                flow->src_dev_id = std::stoi(tokens[i + 3]);
                flow->dst_dev_id = std::stoi(tokens[i + 5]);
                if (GConf::inst().shuffle_device_map){
                    flow->src_dev_id = GContext::get_device_shuffle_map(flow->src_dev_id);
                    flow->dst_dev_id = GContext::get_device_shuffle_map(flow->dst_dev_id);
                }
            } else {
                // empty task
            }
        }
        myfile.close();
    }

    return protocol;
} 


Protocol* 
psim::ring_allreduce(int num_replicas, double comm_size, double aggregate_time) {
    Protocol *protocol = new Protocol();

    int task_counter = 0;

    int num_chunks = num_replicas; 

    // each chuck would start rotating at its corresponding machine, 
    // going through all the machines, and then back to the original machine. 

    for (int i = 0; i < num_chunks; i++) {
        // the chunk starts at machine i, goes through all the other machine, 
        // so the prev machine in the ring would know the aggregate result.
        // then the chunk will go through all the other machines again, such that
        // all the machines will have the aggregate result.

        // there would be a total of 2 * (num_replicas - 1) communication steps. 
        // there would a total of (num_replicas - 1) aggregation steps.


        PTask* prev_task = nullptr; 

        for (int j = 0; j < num_replicas - 1; j++) {

            Flow* flow = (Flow*)protocol->create_task(PTaskType::FLOW, task_counter);
            flow->size = comm_size; 
            flow->src_dev_id = (i + j) % num_replicas;
            flow->dst_dev_id = (i + j + 1) % num_replicas;
            task_counter += 1;

            PComp* agg = (PComp*)protocol->create_task(PTaskType::COMPUTE, task_counter);
            agg->size = aggregate_time; 
            agg->dev_id = (i + j + 1) % num_replicas;
            task_counter += 1;

            flow->add_next_task_id(agg->id);

            if (prev_task != nullptr) {
                prev_task->add_next_task_id(flow->id);
            }

            prev_task = agg;
        } 

        // now the aggregate result is at machine i + num_replicas - 1 round the ring. 
        // now turn the aggregated result round the ring again, so that all the machines
        // will have the aggregated result.

        int starting = i + num_replicas - 1;
        for (int j = 0; j < num_replicas - 1; j++) {

            Flow* flow = (Flow*)protocol->create_task(PTaskType::FLOW, task_counter);
            flow->size = comm_size; 
            flow->src_dev_id = (starting + j) % num_replicas;
            flow->dst_dev_id = (starting + j + 1) % num_replicas;
            task_counter += 1;

            if (prev_task != nullptr) {
                prev_task->add_next_task_id(flow->id);
            }

            prev_task = flow;
        }
    }



    return protocol;
} 

Protocol* 
psim::build_all_to_all(int num_replicas, double comm_size, int chunk_count) {
    Protocol *protocol = new Protocol();

    int task_counter = 0;

    double chunk_size = comm_size / chunk_count;
    
    for (int k = 0; k < chunk_count; k++) {
        for (int i = 0; i < num_replicas; i++) {
            for (int j = 0; j < num_replicas; j++) {
                if (i == j) {
                    continue;
                }

                Flow* flow = (Flow*)protocol->create_task(PTaskType::FLOW, task_counter);
                flow->size = comm_size; 
                flow->src_dev_id = i;
                flow->dst_dev_id = j;
                task_counter += 1;
            }
        }
    }

    return protocol; 
}


//////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////

RoutingSpec get_flow_spine_rates(int jobid, int per_job_task_id, int iteration, 
                                 int sub_flow_count, 
                                 std::map<ProtocolFlowSpec, RoutingSpec>& spine_rates) {

    ProtocolFlowSpec flow_spec = {jobid, per_job_task_id, iteration};
    RoutingSpec flow_spine_rates;   

    if(spine_rates.find(flow_spec) != spine_rates.end()) {
        flow_spine_rates = spine_rates[flow_spec];    
    }

    if (flow_spine_rates.spine_count == 0) {
        // if nothing could be found, we will just leave it unassigned.
        flow_spine_rates.spine_count = sub_flow_count; 

        for (int k = 0; k < sub_flow_count; k++) {
            flow_spine_rates.spine_rates.push_back({-1, 1.0 / sub_flow_count});
        }   
    }
    
    return flow_spine_rates;
}



EmptyTask* insert_all_reduce_into_protocol(Protocol* protocol, std::vector<PComp*> last_layer_pcs, 
                                           std::vector<int>& node_ids, int comm_size, 
                                           EmptyTask* last_all_reduce_finisher, bool add_stage_barriers,
                                           bool reverse_ring, int jobid, int iter_num, int layer_num, 
                                           std::map<ProtocolFlowSpec, RoutingSpec>& spine_rates, 
                                           double iteration_throttle_rate) {


    // in the end, everything should lead to this. 
    EmptyTask* all_reduce_finisher = (EmptyTask*)protocol->create_task(PTaskType::EMPTY);
    all_reduce_finisher->name = "AllR";
    all_reduce_finisher->print_on_exec = true;
    all_reduce_finisher->print_message = "job " + std::to_string(jobid) +
                                         " iter " + std::to_string(iter_num) +
                                         " layer " + std::to_string(layer_num) +
                                         " all-reduce finished";

    int num_replicas = node_ids.size();
    int node_count = last_layer_pcs.size();
    int num_chunks = num_replicas;

    int sub_flow_count = 1; 
    if (GConf::inst().subflows != 0) {
        sub_flow_count = GConf::inst().subflows;
    }

    if(reverse_ring) {
        std::reverse(last_layer_pcs.begin(), last_layer_pcs.end());
    }

    // shuffle the last_layer_pcs to creat a random chain 
    // std::random_shuffle(last_layer_pcs.begin(), last_layer_pcs.end());

    // each chuck would start rotating at its corresponding machine, 
    // going through all the machines, and then back to the original machine. 
    std::vector<std::vector<std::vector<Flow*>>> all_flows(num_chunks);
    for (int i = 0; i < num_chunks; i++) {
        all_flows[i].resize(2 * num_replicas - 2);
    }

    EmptyTask* initial_barrier = (EmptyTask*)protocol->create_task(PTaskType::EMPTY);
    initial_barrier->name = "init";

    // the initial barrier will make sure all the layers have prepared their data before 
    // starting the all-reduce operation. 
    for (int j = 0; j < num_replicas; j++) {
        last_layer_pcs[j]->add_next_task_id(initial_barrier->id);
    }

    // There's the option to serialize the all-reduce operations. One all-reduce wouldn't
    // start until the earlier one has finished. if the pointer is given, the dependency 
    // will be added. 
    if (last_all_reduce_finisher != nullptr) {
        last_all_reduce_finisher->add_next_task_id(initial_barrier->id);
    }

    for (int i = 0; i < num_chunks; i++) {
        // the chunk starts at machine i, goes through all the other machine, 
        // so the prev machine in the ring would know the aggregate result.
        // then the chunk will go through all the other machines again, such that
        // all the machines will have the aggregate result.

        // there would be a total of 2 * (num_replicas - 1) communication steps. 
        // there would a total of (num_replicas - 1) aggregation steps.

        PTask* prev_task = nullptr; 
        int starting = i;

        for (int j = 0; j < num_replicas - 1; j++) {
            RoutingSpec flow_spine_rates = get_flow_spine_rates(jobid, protocol->per_job_task_counter, 
                                                                iter_num, sub_flow_count, spine_rates);

            for (int k = 0; k < flow_spine_rates.spine_count; k++) {
                int spine_decision = flow_spine_rates.spine_rates[k].first; 
                double spine_ratio = flow_spine_rates.spine_rates[k].second; 
                double flow_max_rate = GConf::inst().link_bandwidth * spine_ratio * iteration_throttle_rate;  
                double flow_size = comm_size * spine_ratio;  
                int flow_src_index = (starting + j) % num_replicas; 
                int flow_dst_index = (starting + j + 1) % num_replicas;

                Flow* flow = (Flow*)protocol->create_task(PTaskType::FLOW);
                all_flows[i][j].push_back(flow);

                flow->jobid = jobid;
                flow->size = flow_size;
                flow->src_dev_id = last_layer_pcs[flow_src_index]->dev_id;
                flow->dst_dev_id = last_layer_pcs[flow_dst_index]->dev_id;
                flow->protocol_defined_lb_decision = spine_decision;
                flow->protocol_defined_max_rate = flow_max_rate;
                flow->protocol_defined_subflow_id = k;  
                flow->protocol_defined_iteration = iter_num; 

                flow->label_for_progress_graph = "chain_" + std::to_string(i + 1) + 
                                                 "_hop_" + std::to_string(j + 1) + 
                                                 "_subflow_" + std::to_string(k + 1);

                spdlog::info("flow {}-{}-{}-{} assigned to core {} with rate {} Gbps, size {} Mbytes", 
                                 jobid, flow->per_job_task_id, flow->id, k,
                                 flow->protocol_defined_lb_decision, 
                                 flow->protocol_defined_max_rate, flow_size);
            }

            protocol->increment_per_job_task_counter();
            // An empty to show the aggregation. 
            EmptyTask* agg = (EmptyTask*)protocol->create_task(PTaskType::EMPTY);
            agg->name = "Agg"; 
            
            // Linking all the dependencies 
            // for (int k = 0; k < sub_flow_count; k++) {
            //     Flow* flow = all_flows[i][j][k];
            for (Flow* flow: all_flows[i][j]) { 
                flow->add_next_task_id(agg->id);
                if (j == 0) {
                    initial_barrier->add_next_task_id(flow->id);
                } else {
                    prev_task->add_next_task_id(flow->id);
                }
            }
            prev_task = agg;
        } 

        // now the aggregate result is at machine i + num_replicas - 1 round the ring. 
        // now turn the aggregated result round the ring again, so that all the machines
        // will have the aggregated result.
        starting = i + num_replicas - 1;

        for (int j = 0; j < num_replicas - 1; j++) {
            // we will trust that the same per task id will be used for this flow. 
            RoutingSpec flow_spine_rates = get_flow_spine_rates(jobid, protocol->per_job_task_counter, 
                                                                iter_num, sub_flow_count, spine_rates);

            for (int k = 0; k < flow_spine_rates.spine_count; k++) {
                int spine_decision = flow_spine_rates.spine_rates[k].first; 
                double spine_ratio = flow_spine_rates.spine_rates[k].second; 
                double flow_max_rate = GConf::inst().link_bandwidth * spine_ratio * iteration_throttle_rate;

                double flow_size = comm_size * spine_ratio;  
                int flow_src_index = (starting + j) % num_replicas;
                int flow_dst_index = (starting + j + 1) % num_replicas;

                Flow* flow = (Flow*)protocol->create_task(PTaskType::FLOW);
                all_flows[i][num_replicas - 1 + j].push_back(flow);

                flow->jobid = jobid;
                flow->size = flow_size; 
                flow->src_dev_id = last_layer_pcs[flow_src_index]->dev_id;
                flow->dst_dev_id = last_layer_pcs[flow_dst_index]->dev_id;
                flow->protocol_defined_lb_decision = spine_decision;
                flow->protocol_defined_max_rate = flow_max_rate;
                flow->protocol_defined_subflow_id = k;  
                flow->protocol_defined_iteration = iter_num; 

                flow->label_for_progress_graph = "chain_" + std::to_string(i + 1) + 
                                                 "_hop_" + std::to_string(j + num_replicas - 1 + 1) +
                                                 "_subflow_" + std::to_string(k + 1);

                spdlog::info("flow {}-{}-{}-{} assigned to core {} with rate {} Gbps, size {} Mbytes", 
                                 jobid, flow->per_job_task_id, flow->id, k, 
                                 flow->protocol_defined_lb_decision, 
                                 flow->protocol_defined_max_rate, flow_size);   
            }
            
            protocol->increment_per_job_task_counter();

            EmptyTask* agg = nullptr;
            if (j < num_replicas - 2) {
                agg = (EmptyTask*)protocol->create_task(PTaskType::EMPTY);
                agg->name = "Agg";  
            }

            for (Flow* flow: all_flows[i][num_replicas - 1 + j]) {  
                prev_task->add_next_task_id(flow->id);

                if (j == num_replicas - 2) { // the last one
                    flow->add_next_task_id(all_reduce_finisher->id);
                } else {
                    flow->add_next_task_id(agg->id);
                }
            }

            prev_task = agg;
        }
    }

    if (add_stage_barriers) {
        for (int j = 0; j < 2 * num_replicas - 3; j++) {
            EmptyTask* barrier = (EmptyTask*)protocol->create_task(PTaskType::EMPTY);
            barrier->name = "barrier";

            for (int i = 0; i < num_chunks; i++) {
                for (int k = 0; k < all_flows[i][j].size(); k++) {  
                    Flow* this_stage_flow = all_flows[i][j][k];    
                    this_stage_flow->add_next_task_id(barrier->id);
                }

                for (int k = 0; k < all_flows[i][j + 1].size(); k++) {  
                    Flow* next_stage_flow = all_flows[i][j + 1][k];    
                    barrier->add_next_task_id(next_stage_flow->id);
                }
            }
        }
    }

    return all_reduce_finisher;
} 


void 
insert_simple_data_parallelism(Protocol* protocol, int jobid,
                               std::vector<int>& node_ids, 
                               int layer_count, int iter_count, 
                               int comp_size, int comm_size, 
                               std::vector<int>& iteration_deltas, 
                               std::vector<double>& iter_throttle_rates,
                               bool reverse_ring, 
                               std::map<ProtocolFlowSpec, RoutingSpec>& spine_rates) {

    int forward_size = comp_size;
    int backward_size = comp_size; 
    int job_node_count = node_ids.size();
    
    EmptyTask* last_iter_finisher = (EmptyTask*)protocol->create_task(PTaskType::EMPTY);
    last_iter_finisher->name = "protocol start";
    last_iter_finisher->print_on_exec = true;
    last_iter_finisher->print_message = "job " + std::to_string(jobid) + " started";

    PTask* prev_dep = last_iter_finisher; 
    // if(initial_wait != 0) {
    //     PComp* wait = (PComp*)protocol->create_task(PTaskType::COMPUTE);
    //     wait->size = initial_wait;
    //     wait->dev_id = node_ids[0];
    //     wait->add_next_task_id(last_iter_finisher->id);
    // }

    for (int i = 0; i < iter_count; i ++) {
        double iteration_throttle_rate = iter_throttle_rates[i];
        
        protocol->reset_per_job_task_counter();

        int this_iter_delta = iteration_deltas[i];  
        if (this_iter_delta != 0) {
            PComp* wait = (PComp*)protocol->create_task(PTaskType::COMPUTE);
            wait->size = this_iter_delta;
            wait->dev_id = node_ids[0];
            last_iter_finisher->add_next_task_id(wait->id); 
            prev_dep = wait;
        }

        std::vector<PComp*> last_layer_pcs; 

        // build the forward pass for the current iteration. the tasks for each machine are connected in a chain.
        for (int node_index = 0; node_index < job_node_count; node_index++) {
            PComp* last_pc = nullptr; 
            for (int k = 0; k < layer_count; k++) {
                PComp* pc = (PComp*)protocol->create_task(PTaskType::COMPUTE);
                pc->size = forward_size;
                pc->dev_id = node_ids[node_index];

                // if it's not the first iteration, I have to connect it somehow to the previous iteration. 
                if (k == 0 and prev_dep != nullptr) {
                    prev_dep->add_next_task_id(pc->id);
                }

                if (last_pc != nullptr) {
                    last_pc->add_next_task_id(pc->id);
                }

                last_pc = pc;

                if (k == layer_count - 1) {
                    last_layer_pcs.push_back(pc);
                }
            }
        }

        last_iter_finisher = (EmptyTask*)protocol->create_task(PTaskType::EMPTY);
        last_iter_finisher->name = "ITER" + std::to_string(i);
        last_iter_finisher->print_on_exec = true;
        last_iter_finisher->print_message = "job " + std::to_string(jobid) + " iter " + std::to_string(i + 1) + " finished";
        prev_dep = last_iter_finisher;
        
        // build the backward pass for the current iteration. the tasks for each machine are connected in a chain.
        EmptyTask* last_all_reduce_finisher = nullptr;

        for (int j = layer_count - 1; j >= 0; j--) {
            
            for (int node_index = 0; node_index < job_node_count; node_index++) {
                PComp* pc = (PComp*)protocol->create_task(PTaskType::COMPUTE);
                pc->size = backward_size;
                pc->dev_id = node_ids[node_index];

                // connect the last layer pc to this layer 
                last_layer_pcs[node_index]->add_next_task_id(pc->id);

                // subsitute the last layer pc with the current pc
                last_layer_pcs[node_index] = pc;

                if (j == 0) {
                    pc->add_next_task_id(last_iter_finisher->id);
                }
            }

            // at this point last_layer_pcs contains the last layer of the backward passes.
            // we can do the all_reduce to the protocol. 

            bool add_stage_barriers = true; 
            // bool add_stage_barriers = false; 

            int iter_num = i; 
            int layer_num = j;  

            // the all_reduce starter will fire when all the backward pass processing is done.
            EmptyTask* all_reduce_starter = (EmptyTask*)protocol->create_task(PTaskType::EMPTY);
            all_reduce_starter->name = "AllR";
            all_reduce_starter->print_on_exec = true;
            all_reduce_starter->print_message = "job " + std::to_string(jobid) +
                                                " iter " + std::to_string(iter_num) +
                                                " layer " + std::to_string(layer_num) +
                                                " all-reduce started";
        
            for (int node_index = 0; node_index < job_node_count; node_index++) {
                last_layer_pcs[node_index]->add_next_task_id(all_reduce_starter->id);
            }

            EmptyTask* all_reduce_finisher = insert_all_reduce_into_protocol(protocol, last_layer_pcs, node_ids, 
                                                                             comm_size, last_all_reduce_finisher, 
                                                                             add_stage_barriers, reverse_ring, 
                                                                             jobid, iter_num, layer_num, spine_rates, 
                                                                             iteration_throttle_rate);

            all_reduce_finisher->add_next_task_id(last_iter_finisher->id);

            // if you want a barrier between this all_reduce and the next all_reduce, 
            // uncomment the following line. This would be equivalent to have NCCL or 
            // something like that avoiding a new collective operation before the 
            // previous one is finished. 

            last_all_reduce_finisher = all_reduce_finisher;
        }
    }
}


int LCM(int a, int b){
    return a * b / std::__gcd(a, b);
}

Protocol* 
psim::build_periodic_data_parallelism() { 

    spdlog::critical("building periodic data parallelism protocol is probably not working correctly. exiting.");    
    exit(0);    

    int node_count = 4; // n: number of machines
        
    int layer_count = GConf::inst().general_param_6;  // l: number of teeth in the graph
    if (layer_count == 0) {
        layer_count = 12;
    }

    int reps_multiplier = GConf::inst().general_param_5; // i: 0 .. reps_multiplier * hyper_period
    if (reps_multiplier == 0) {
        reps_multiplier = 1;
    }

    int job1_length_base = GConf::inst().general_param_2; 
    if (job1_length_base == 0) {
        job1_length_base = 1;
    }

    std::vector<int> job1_initial_wait(0, 0); 
    int job1_starting_node = 2; 
    int job1_jobid = 1; 

    int job2_length_base = GConf::inst().general_param_3;
    if (job2_length_base == 0) {
        job2_length_base = 1;
    }

    int job2_initial_wait_input = GConf::inst().general_param_1;
    std::vector<int> job2_initial_wait = {job2_initial_wait_input};  
    int job2_starting_node = job1_starting_node + node_count;
    int job2_jobid = 2; 
    
    std::vector<double> iter_throttle_rates = {1.0}; 
    
    int comp_length_amplification = 100;
    
    // double comm_duty_cycle = GConf::inst().general_param_4; 
    // if (comm_duty_cycle == 0) {
    //     comm_duty_cycle = 50; // 10 units of time for the 400G links 
    // }

    // int link_rate = 400; 
    // int comm_length_amplification = link_rate * comp_length_amplification * (comm_duty_cycle / 100) / (2 * (node_count - 1));
    
    int comm_size = GConf::inst().general_param_4;
    if (comm_size == 0) {
        comm_size = 4000; 
    }

    int hyper_period = LCM(job1_length_base, job2_length_base);
    int job1_reps_per_hyper_period = hyper_period / job1_length_base;
    int job2_reps_per_hyper_period = hyper_period / job2_length_base;

    std::vector<int> job1_node_ids;      
    for (int i = 0; i < node_count; i++) {
        job1_node_ids.push_back(i + job1_starting_node);
    }
    std::vector<int> job2_node_ids;
    for (int i = 0; i < node_count; i++) {
        job2_node_ids.push_back(i + job2_starting_node);
    }


    Protocol *protocol = new Protocol();

    std::map<ProtocolFlowSpec, RoutingSpec> spine_rates;

    insert_simple_data_parallelism(protocol, job1_jobid, 
                                   job1_node_ids, layer_count, 
                                   job1_reps_per_hyper_period * reps_multiplier, 
                                   job1_length_base * comp_length_amplification, 
                                   job1_length_base * comm_size, 
                                   job1_initial_wait, iter_throttle_rates, 
                                   false, spine_rates);

    insert_simple_data_parallelism(protocol, job2_jobid, 
                                   job2_node_ids, layer_count, 
                                   job2_reps_per_hyper_period * reps_multiplier, 
                                   job2_length_base * comp_length_amplification, 
                                   job2_length_base * comm_size, 
                                   job2_initial_wait, iter_throttle_rates, 
                                   true, spine_rates);                             

    return protocol;
}



//////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////

void insert_simple_periodic(Protocol* protocol, int src_machine, 
                            int dst_machine, int bump_count, int comp_length, 
                            int comm_length, int jobid, int reps_multiplier, 
                            int initial_wait, int long_pc_length, 
                            bool add_flow_dependencies) {
    

    // build a chain of computation tasks.
    PTask* last_pc = nullptr;
    PTask* last_flow = nullptr; 

    if (initial_wait != 0) {
        PComp* wait = (PComp*)protocol->create_task(PTaskType::COMPUTE);
        wait->size = initial_wait;
        wait->dev_id = src_machine;
        last_pc = wait;
    }

    EmptyTask* last_iter_finisher = (EmptyTask*)protocol->create_task(PTaskType::EMPTY);
    last_iter_finisher->name = "protocol start";
    last_iter_finisher->print_on_exec = true;
    last_iter_finisher->print_message = "job " + std::to_string(jobid) + " started";
    
    if (last_pc != nullptr) {
        last_pc->add_next_task_id(last_iter_finisher->id);
        last_pc = last_iter_finisher; 
    } else {
        last_pc = last_iter_finisher;
    }

    for (int j = 0; j < reps_multiplier; j++) {
        for (int i = 0; i < bump_count; i++) {
            PComp* pc = (PComp*)protocol->create_task(PTaskType::COMPUTE);
            pc->size = comp_length;
            pc->dev_id = src_machine;

            Flow* flow = (Flow*)protocol->create_task(PTaskType::FLOW);
            flow->size = comm_length;
            flow->label_for_progress_graph = "chain_" + std::to_string(j + 1) + "_hop_" + std::to_string(i + 1);
            flow->src_dev_id = src_machine;
            flow->dst_dev_id = dst_machine;
            flow->jobid = jobid; 
            
            if (jobid == 1) {
                flow->protocol_defined_lb_decision = 0; 
            } else if (jobid == 2) {
                if (j > 2 and j < 9) {
                    flow->protocol_defined_lb_decision = 1;
                } else {
                    flow->protocol_defined_lb_decision = 0;
                }
            }

            last_pc->add_next_task_id(pc->id);
            pc->add_next_task_id(flow->id);

            last_pc = pc;
            
            if (add_flow_dependencies) {
                if (last_flow != nullptr) {
                    last_flow->add_next_task_id(flow->id);
                }
            }
            last_flow = flow; 
        }
        
        PComp* long_pc = (PComp*)protocol->create_task(PTaskType::COMPUTE);
        long_pc->size = long_pc_length;
        long_pc->dev_id = src_machine;
        last_flow->add_next_task_id(long_pc->id);
        last_pc = long_pc;

        last_iter_finisher = (EmptyTask*)protocol->create_task(PTaskType::EMPTY);
        last_iter_finisher->name = "ITER" + std::to_string(j);
        last_iter_finisher->print_on_exec = true;
        last_iter_finisher->print_message = "job " + std::to_string(jobid) + " iter " + std::to_string(j + 1) + " finished";
        
        last_pc->add_next_task_id(last_iter_finisher->id);
    }
}

Protocol* 
psim::build_periodic_simple() { 

    int job1_bump_count = GConf::inst().general_param_4;
    if (job1_bump_count == 0) {
        job1_bump_count = 6;
    } 

    int job2_bump_count = GConf::inst().general_param_8;
    if (job2_bump_count == 0) {
        job2_bump_count = 6;
    }

    int job1_src_machine = 3; 
    int job1_dst_machine = 4;

    int job2_src_machine = 8;
    int job2_dst_machine = 7;

    int job1_offset = 0; 
    int job2_offset = GConf::inst().general_param_1; 

    int comp_length = 100;



    int job1_comm_length = 400 * GConf::inst().general_param_2; 
    if (job1_comm_length == 0) {
        job1_comm_length = 400 * 50; 
    }

    int job2_comm_length = 400 * GConf::inst().general_param_7;
    if (job2_comm_length == 0) {
        job2_comm_length = 400 * 50; 
    }

    int job1_job_id = 1; 
    int job2_job_id = 2;

    int job1_reps_multiplier = GConf::inst().general_param_3;
    int job2_reps_multiplier = GConf::inst().general_param_3;
    if (job1_reps_multiplier == 0) {
        job1_reps_multiplier = 1; 
        job2_reps_multiplier = 1; 
    }
    
    int job1_long_pc_length = GConf::inst().general_param_6; 
    int job2_long_pc_length = GConf::inst().general_param_6; 
    
    if(job1_long_pc_length == 0) {
        job1_long_pc_length = 1500;
        job2_long_pc_length = 1500; 
    }


    ////////////////////////////////////////
    comp_length = 40; 

    job1_bump_count = 2; 
    job1_comm_length = 400 * 20;
    job1_long_pc_length = 200; 
    job1_reps_multiplier = 8; 

    job2_bump_count = 3; 
    job2_comm_length = 400 * 20; 
    job2_long_pc_length = 260;
    job2_reps_multiplier = 6; 
    ////////////////////////////////////////



    bool add_flow_dependencies = false; 
    if (GConf::inst().general_param_5 == 1) {
        add_flow_dependencies = true;
    }

    Protocol *protocol = new Protocol(); 

    insert_simple_periodic(protocol, job1_src_machine, job1_dst_machine, 
                           job1_bump_count, comp_length, job1_comm_length, 
                           job1_job_id, job1_reps_multiplier, 
                           job1_offset, 
                           job1_long_pc_length, add_flow_dependencies);

    insert_simple_periodic(protocol, job2_src_machine, job2_dst_machine, 
                           job2_bump_count, comp_length, job2_comm_length, 
                           job2_job_id, job2_reps_multiplier, 
                           job2_offset, 
                           job2_long_pc_length, add_flow_dependencies);

    return protocol;
}


int get_config_or_default(int value, int default_value) {
    if (value == 0) {
        return default_value;
    }
    return value;
}

Protocol* 
psim::build_nethint_test() {
    Protocol *protocol = new Protocol();
    std::map<ProtocolFlowSpec, RoutingSpec> flow_spines; 
    std::map<int, std::vector<int>> job_timings_data; 
    std::map<int, std::vector<double>> rate_throttling_data; 

    int iso = GConf::inst().isolate_job_id;
    bool isolated_execution = false;  
    if (iso != -1) {
        isolated_execution = true;
    }

    // read the placement file and populate the jobs map.
    nlohmann::json jobs;
    std::string placement_file = GConf::inst().placement_file;   
    spdlog::critical("placement file: {}", placement_file);
    std::ifstream placement_stream(placement_file);
    placement_stream >> jobs;

    // read the timing file and populate the timings map.
    nlohmann::json timings;
    std::string timing_file = GConf::inst().timing_file; 

    bool timing_file_exists = true; 
    if (not std::filesystem::exists(timing_file)) {
        timing_file_exists = false; 
        spdlog::critical("timing file not found. all timings will be set to 0.");  
    } else { 
        timing_file_exists = true;
        spdlog::critical("timing file: {}", timing_file);
        std::ifstream timing_stream(timing_file);
        timing_stream >> timings;

        for (const auto& item : timings) {
            int job_id = item["job_id"];
            std::vector<int> deltas = item["deltas"];
            std::vector<double> throttle_rates = item["throttle_rates"];
            
            job_timings_data[job_id] = deltas;
            rate_throttling_data[job_id] = throttle_rates;
        }
    }

    // std::cout << "press any key to continue" << std::endl;  
    // int a; 
    // std::cin >> a; 

    // read the routing file and populate the flow_spines map.
    nlohmann::json routings; 
    std::string routing_file = GConf::inst().routing_file;    
    bool routing_file_exists = true;    
    if (not std::filesystem::exists(routing_file)) {
        spdlog::critical("routing file not found. exiting.");
        routing_file_exists = false;
    
    } else {
        spdlog::critical("routing file: {}", routing_file);
        std::ifstream routing_stream(routing_file);
        routing_stream >> routings;

        for (auto& routing : routings) {
            int job_id = routing["job_id"];  
            int per_job_task_id = routing["flow_id"];   
            int iteration = routing["iteration"];     
            ProtocolFlowSpec spec = {job_id, per_job_task_id, iteration};   

            RoutingSpec routing_spec; 
            int spine_count = routing["spine_count"];
            routing_spec.spine_count = spine_count; 
            std::vector<std::pair<int, double>> spine_rates;
            spine_rates = routing["spine_rates"];   
            routing_spec.spine_rates = spine_rates; 

            flow_spines[spec] = routing_spec;   
            
            // print all this info in one line 
            spdlog::critical("job_id: {}, per_job_task_id: {}, iteration: {}, spine_count: {}", job_id, per_job_task_id, iteration, spine_count);
            for (auto& spine_rate : spine_rates) {
                spdlog::critical("spine_id: {}, rate: {}", spine_rate.first, spine_rate.second);
            } 
        }
    }

    int job_index = 0; 
    for (auto& job : jobs) {
        int job_id = job["job_id"];
        int machine_count = job["machine_count"];
        int job_comm_size = job["comm_size"];
        int job_comp_size = job["comp_size"];
        int job_layer_count = job["layer_count"];
        int job_iter_count = job["iter_count"]; 

        if (isolated_execution) {
            // if this an isolated execution, only one iteration would be enough.
            job_iter_count = 1; 
        }

        std::vector<int> job_machines = job["machines"];
        std::vector<int> iteration_deltas(job_iter_count, 0);
        std::vector<double> iter_throttle_rates(job_iter_count, 1.0);   

        if (timing_file_exists) {
            auto& job_timing = timings[job_index];
            for (int i = 0; i < job_iter_count; i++) {
                iteration_deltas[i] = job_timing["deltas"][i];
                iter_throttle_rates[i] = job_timing["throttle_rates"][i];   
            }
        }

        if (isolated_execution) {
            // if this an isolated execution, only one iteration
            iter_throttle_rates[0] = GConf::inst().throttle_factor; 
        }

        // print the information    
        spdlog::critical("job_id: {}", job_id);
        spdlog::critical("machine_count: {}", machine_count); 
        spdlog::critical("job_comm_size: {}", job_comm_size);
        spdlog::critical("job_comp_size: {}", job_comp_size);
        spdlog::critical("job_layer_count: {}", job_layer_count);

        std::string deltas_str = "";    
        for (auto& delta : iteration_deltas) {
            deltas_str += std::to_string(delta) + " ";
        }
        spdlog::critical("iteration_deltas: {}", deltas_str);   

        std::string machines_str = "";
        for (auto& machine : job_machines) {
            machines_str += std::to_string(machine) + " ";
        }
        spdlog::critical("machines: {}", machines_str);

        std::string rates_str = ""; 
        for (auto& rate : iter_throttle_rates) {
            rates_str += std::to_string(rate) + " ";
        }
        spdlog::critical("throttle rates: {}", rates_str);

        int this_job_long_pc_length = 0;
        bool reverse_ring = false;
        job_comm_size = (int)(job_comm_size / (job_machines.size())); 

        if (iso == -1 or iso == job_id) {
            insert_simple_data_parallelism(protocol, 
                                           job_id, 
                                           job_machines, 
                                           job_layer_count, 
                                           job_iter_count, 
                                           job_comp_size, 
                                           job_comm_size, 
                                           iteration_deltas,
                                           iter_throttle_rates, 
                                           reverse_ring, 
                                           flow_spines); 
        }

        job_index += 1;
    }
    return protocol; 
}