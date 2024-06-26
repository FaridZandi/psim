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


Protocol* 
psim::build_periodic_test() { 

    int node_count = 3; 
    int layer_count = 3; 
    int iter_count = 3;

    Protocol *protocol = new Protocol();
    

    PComp* last_iter_dummy_pc = nullptr;

    for (int i = 0; i < iter_count; i ++) {
        // if it's not the first iteration, I have to connect it somehow to the previous iteration. 

        // TODO 


        std::vector<PComp*> last_layer_pcs; 

        // build the forward pass for the current iteration. the tasks for each machine are connected in a chain.
        for (int j = 0; j < node_count; j++) {
            
            PComp* last_pc = nullptr; 
            
            for (int k = 0; k < layer_count; k++) {
                PComp* pc = (PComp*)protocol->create_task(PTaskType::COMPUTE);
                pc->size = 100;
                pc->dev_id = j;

                if (k == 0 and i != 0) {
                    last_iter_dummy_pc->add_next_task_id(pc->id);
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

        last_iter_dummy_pc = (PComp*)protocol->create_task(PTaskType::COMPUTE);
        last_iter_dummy_pc->size = 0;
        last_iter_dummy_pc->dev_id = 0;

        // build the backward pass for the current iteration. the tasks for each machine are connected in a chain.
        for (int j = layer_count - 1; j >= 0; j--) {

            for (int k = 0; k < node_count; k++) {
                PComp* pc = (PComp*)protocol->create_task(PTaskType::COMPUTE);
                pc->size = 100;
                pc->dev_id = k;

                // connect the last layer pc to this layer 
                last_layer_pcs[k]->add_next_task_id(pc->id);

                // subsitute the last layer pc with the current pc
                last_layer_pcs[k] = pc;


                if (j == 0) {
                    pc->add_next_task_id(last_iter_dummy_pc->id);
                }
            }
        }
    }

    return protocol;
}