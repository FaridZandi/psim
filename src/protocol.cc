#include <iostream>
#include <iomanip>
#include <vector>
#include <algorithm>
#include <limits>
#include "protocol.h"
#include <cmath>
#include <fstream>

using namespace psim;

Protocol::Protocol() {
    finished_task_count = 0; 
    total_task_count = 0; 
}

Protocol::~Protocol() {
    for (auto task : this->tasks) {
        delete task;
    }
}


PTask* Protocol::create_task(PTaskType type, int id) {
    PTask *task;

    switch (type) {
        case PTaskType::FLOW:
            task = new Flow();
            break;
        case PTaskType::COMPUTE:
            task = new PComp();
            break;
        case PTaskType::EMPTY:
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
            PTask *next_task = this->task_map[next_task_id];
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

void Protocol::export_graph(std::ofstream& protocol_log){
    for (auto& task : this->tasks) {
        task->print_task_info(protocol_log);
    }
}

void Protocol::export_dot(std::string filename){
    std::ofstream protocol_log;
    protocol_log.open(filename);

    protocol_log << "digraph G {" << std::endl;
    protocol_log << "node [shape=record];" << std::endl;

    for (auto& task : this->tasks) {
        // print task info in dot format
        protocol_log << task->id << " [label=\"" << task->id << "\"";
        protocol_log << " shape="; 
        if (task->get_type() == PTaskType::COMPUTE) {
            protocol_log << "box";
        } else if (task->get_type() == PTaskType::FLOW) {
            protocol_log << "ellipse";
        } else {
            protocol_log << "diamond";
        }
        protocol_log << "];" << std::endl;
    }

    for (auto& task : this->tasks) {
        for (auto& next_task : task->next_task_ids) {
            protocol_log << task->id << " -> " << next_task << ";" << std::endl;
        }
    }

    protocol_log << "}" << std::endl;
    protocol_log.close();

    // run dot command to generate png
    std::string dot_command = "dot -Tpng " + filename + " -o " + filename + ".png";
    int ret = system(dot_command.c_str());

}

Protocol* Protocol::build_random_protocol(int num_comp, int machine_count){
    Protocol *protocol = new Protocol();

    // create compute tasks 
    std::map<int, PComp *> task_map;
    
    int task_counter = 0;
    for (int i = 0; i < num_comp; i++) {
        PComp* pc = (PComp*)protocol->create_task(PTaskType::COMPUTE, task_counter);
        task_map[i] = pc;
        pc->size = 100;
        pc->dev_id = rand() % 16;
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
                flow->size = 100;

                task_map[prev]->add_next_task_id(flow->id);
                flow->add_next_task_id(task_map[i]->id);
            }
        }

        reachable_tasks.push_back(i);
    }

    return protocol;
}

PTask::PTask() {
    is_initiator = true; 
    status = PTaskStatus::BLOCKED;
    dep_left = 0;
    start_time = 0; 
    end_time = 0; 
}


PTask::~PTask() {
    
}

void PTask::add_next_task_id(int id) {
    this->next_task_ids.push_back(id);
}

void PTask::add_to_next(PTask *task) {
    this->next_tasks.push_back(task);
    task->dep_left += 1;
}


EmptyTask::EmptyTask() : PTask() {
    
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

Flow::Flow() : PTask() {
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
        status = PTaskStatus::FINISHED;
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

PComp::PComp() : PTask() {
    progress = 0; 
}


PComp::~PComp() {
    
}

void 
PComp::print_task_info(std::ostream& os){
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
