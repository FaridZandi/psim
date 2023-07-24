#include <iostream>
#include <iomanip>
#include <vector>
#include <algorithm>
#include <limits>
#include "protocol.h"
#include <cmath>
#include <fstream>
#include <sstream>

using namespace psim;

Protocol::Protocol() {
    finished_task_count = 0; 
    total_task_count = 0; 
    max_allocated_id = 0; 
}

Protocol::~Protocol() {
    for (auto task : this->tasks) {
        delete task;
    }
}

void Protocol::add_to_tasks(PTask *task){
    if(task->id == -1) {
        task->id = max_allocated_id + 1;
    }

    max_allocated_id = std::max(max_allocated_id, task->id);

    this->tasks.push_back(task);
    this->task_map[task->id] = task;

    this->total_task_count += 1;
}

PTask* Protocol::create_task(PTaskType type) {
    return create_task(type, -1);
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
    add_to_tasks(task);

    return task;
}

void Protocol::build_dependency_graph() {
    for (auto task : this->tasks) {
        for (auto next_task_id : task->next_task_ids) {
            PTask *next_task = this->task_map[next_task_id];
            task->add_to_next(next_task);
            next_task->is_initiator = false;
        }
        task->next_task_ids.clear();
    }

    for (auto task : this->tasks) {
        if (task->is_initiator) {
            task->is_initiator = false; 
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
    
    std::string dot_path = "out/" + filename + ".dot";
    std::string png_path = "out/" + filename + ".png";

    protocol_log.open(dot_path);

    protocol_log << "digraph G {" << std::endl;
    protocol_log << "node [shape=record];" << std::endl;

    for (auto& task : this->tasks) {
        // print task info in dot format
        // label is in the format:
        // for compute tasks: Work <task_size> \n on M<task_dev_id>
        // for flow tasks: Comm <task_size> \n from M<task_src_dev_id> to M<task_dst_dev_id>        
        std::string label = "";
        if (task->get_type() == PTaskType::COMPUTE) {
            PComp *compute_task = (PComp *)task;
            // round the size to 2 decimal places
            std::stringstream stream;
            stream << std::fixed << std::setprecision(2) << compute_task->size;
            std::string size_str = stream.str();
            label = "Work\n" + size_str + "\non M" + std::to_string(compute_task->dev_id);

        } else if (task->get_type() == PTaskType::FLOW) {
            Flow *flow = (Flow *)task;
            
            std::stringstream stream;
            stream << std::fixed << std::setprecision(2) << flow->size;
            std::string size_str = stream.str();
            
            label = "Comm\n" + size_str + "\nM" + std::to_string(flow->src_dev_id) + "->M" + std::to_string(flow->dst_dev_id);
        } else {
            label = "AllR";
        }

        protocol_log << task->id << " [label=\"" << task->id << "|" << label << "\"";

        protocol_log << " shape="; 
        if (task->get_type() == PTaskType::COMPUTE) {
            protocol_log << "box";
        } else if (task->get_type() == PTaskType::FLOW) {
            protocol_log << "ellipse";
        } else {
            protocol_log << "diamond";
        }

        protocol_log << " color=";
        if (task->get_type() == PTaskType::COMPUTE) {
            protocol_log << "blue";
        } else if (task->get_type() == PTaskType::FLOW) {
            protocol_log << "green";
        } else {
            protocol_log << "red";
        }

        protocol_log << "];" << std::endl;
    }

    for (auto& task : this->tasks) {
        for (auto& next_task : task->next_tasks) {
            protocol_log << task->id << " -> " << next_task->id << ";" << std::endl;
        }
    }

    protocol_log << "}" << std::endl;
    protocol_log.close();

    // run dot command to generate png
    std::string dot_command = "dot -Tpng " + dot_path + " -o " + png_path;
    int ret = system(dot_command.c_str());

}


Protocol* Protocol::make_copy(bool build_dependency_graph){
    Protocol *replica = new Protocol();

    for (auto& task : this->tasks) {
        PTask *new_task = replica->create_task(task->get_type(), task->id);

        if (task->get_type() == PTaskType::COMPUTE) {
            PComp *compute_task = (PComp *)task;
            PComp *new_compute_task = (PComp *)new_task;

            new_compute_task->size = compute_task->size;
            new_compute_task->dev_id = compute_task->dev_id;
        } else if (task->get_type() == PTaskType::FLOW) {
            Flow *flow = (Flow *)task;
            Flow *new_flow = (Flow *)new_task;

            new_flow->size = flow->size;
            new_flow->src_dev_id = flow->src_dev_id;
            new_flow->dst_dev_id = flow->dst_dev_id;
        } else {
            // empty task
        }

        for (auto& next_task_id : task->next_task_ids) {
            new_task->add_next_task_id(next_task_id);
        }
    }
    
    if (build_dependency_graph){
        replica->build_dependency_graph();
    }

    return replica;
} 


PTask::PTask() {
    reset();
}


PTask::~PTask() {
    
}

void PTask::reset(){
    is_initiator = true;
    status = PTaskStatus::BLOCKED;
    dep_left = 0; 
    start_time = 0;
    end_time = 0;
    next_tasks.clear();
    next_task_ids.clear();
    id = -1;  
} 

void PTask::add_next_task_id(int id) {
    this->next_task_ids.push_back(id);
}

void PTask::add_to_next(PTask *task) {
    this->next_tasks.push_back(task);
    task->dep_left += 1;
}


EmptyTask::EmptyTask() : PTask() {
    reset();
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



void EmptyTask::reset(){
    
} 

PTask* EmptyTask::make_shallow_copy(){
    EmptyTask *new_task = new EmptyTask();
    return new_task;
} 


////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////

Flow::Flow() : PTask() {
    reset();
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
        double multipier = pow(rate_increase, step_size);
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

void Flow::reset(){
    size = 0;
    progress = 0;
    current_rate = GConf::inst().initial_rate; 
    rate_increase = GConf::inst().rate_increase;
    registered_rate = 0; 
    src_dev_id = -1; 
    dst_dev_id = -1;
    path.clear();
    src = nullptr;
    dst = nullptr;
} 

PTask* Flow::make_shallow_copy(){
    Flow *new_task = new Flow();
    new_task->size = this->size;
    new_task->src_dev_id = this->src_dev_id;
    new_task->dst_dev_id = this->dst_dev_id;
    return new_task;

} 


////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////

PComp::PComp() : PTask() {
    reset();    
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

void PComp::reset(){
    progress = 0; 
    dev_id = 0;
    size = 0;
    machine = nullptr;
}


PTask* PComp::make_shallow_copy(){
    PComp *new_task = new PComp();
    new_task->size = this->size;
    new_task->dev_id = this->dev_id;

    return new_task;
} 
