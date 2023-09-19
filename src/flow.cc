#include <iostream>
#include <iomanip>
#include <vector>
#include <algorithm>
#include <limits>
#include "protocol.h"
#include <cmath>
#include <fstream>
#include <sstream>
#include "spdlog/spdlog.h"
#include <queue>

using namespace psim;

Flow::Flow() : PTask() {
    reset();
}

Flow::~Flow() {
    
}

void Flow::initiate(){
    compute_priority(); 
    for (Bottleneck* bottleneck : this->path) {
        bottleneck->current_flow_count += 1;
        bottleneck->current_flow_size_sum += this->size; 
    }
}

void Flow::finished() {
    for (Bottleneck* bottleneck : this->path) {
        bottleneck->current_flow_count -= 1;
        bottleneck->current_flow_size_sum -= this->size; 
    }
}

void Flow::compute_priority(){

    if (id == -1){
        selected_priority = 1e6; 
    } else {
        selected_priority = rank * protocol->tasks.size() + id; 
        // selected_priority = id; 
        // selected_priority = rank; 
        // selected_priority = (int) start_time;
    }
}



void Flow::register_rate_on_path(double step_size){
    double completion_rate = (size - progress) / step_size;
    registered_rate = std::min(completion_rate, current_rate);

    for (auto& bottleneck : this->path) {
        bottleneck->register_rate(id, registered_rate, selected_priority);
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

    current_rate = std::max(current_rate, min_rate);
}

double Flow::make_progress(double current_time, double step_size) {
    double allocated_rate = std::numeric_limits<double>::max();
    
    for (auto bottleneck : this->path) {
        double bn_rate = bottleneck->get_allocated_rate(id, registered_rate, selected_priority);
        allocated_rate = std::min(allocated_rate, bn_rate);
    }

    bn_allocated_rate = allocated_rate;
    current_rate = allocated_rate;
    double step_progress = allocated_rate * step_size;

    if (progress == 0 and step_progress > 0) {
        start_time = current_time;
    }

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
    initial_rate = current_rate; 
    rate_increase = GConf::inst().rate_increase;
    min_rate = GConf::inst().min_rate;
    registered_rate = 0; 
    src_dev_id = -1; 
    dst_dev_id = -1;
    path.clear();
    src = nullptr;
    dst = nullptr;
    bn_priority_levels = GConf::inst().bn_priority_levels;
    selected_priority = -1; 
} 

PTask* Flow::make_shallow_copy(){
    Flow *new_task = new Flow();
    new_task->size = this->size;
    new_task->src_dev_id = this->src_dev_id;
    new_task->dst_dev_id = this->dst_dev_id;
    return new_task;
} 