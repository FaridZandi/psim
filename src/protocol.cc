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

Protocol::Protocol() {
    finished_task_count = 0;
    total_task_count = 0;
    max_allocated_id = 0;
    max_rank = 0;
    type = ProtocolType::MAIN_PROTOCOL;
    per_job_task_counter = 0;
}

Protocol::~Protocol() {
    for (auto task : this->tasks) {
        delete task;
    }
}

void Protocol::reset_per_job_task_counter(){
    per_job_task_counter = 0;
}   


void Protocol::add_to_tasks(PTask *task, int id){
    if(id == -1) {
        // spdlog::warn("Task id not specified, allocating id {}", max_allocated_id + 1);
        id = max_allocated_id + 1;
    }

    if (task_map.find(id) != task_map.end()) {
        spdlog::error("Task id {} already exists", id);
        exit(1);
    }

    task->id = id;
    max_allocated_id = std::max(max_allocated_id, id);
    task->protocol = this;
    this->tasks.push_back(task);
    this->task_map[task->id] = task;
    this->total_task_count += 1;

    task->per_job_task_id = per_job_task_counter;
    per_job_task_counter += 1;
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

    add_to_tasks(task, id);

    return task;
}

void Protocol::build_dependency_graph() {
    // For each task we initially have the ids identifying its next tasks. This
    // loop populates the next_tasks member for each task with the actual task
    // objects rather than the task IDs.

    // this loop also clears the next_task_ids vector, since it's no longer needed.
    // The purpose is that the build_dependency_graph function can be called multiple 
    // times with no problems, and we don't want to keep adding the same next tasks to 
    // the next_tasks vector. If any new ids are added to the next_task_ids vector, 
    // they will be added to the next_tasks vector in the next iteration.

    // the is_initiator and is_finisher flags are set to true by default, and
    // are set to false if the task has a predecessor or successor respectively.
    // the loop below this one identifies the initiator and finisher tasks.
    // TODO: set the flags to true here, don't rely on the default value. 

    for (auto task : this->tasks) {
        for (auto next_task_id : task->next_task_ids) {
            PTask *next_task = this->task_map[next_task_id];
            task->add_to_next(next_task);
            task->is_finisher = false;
            next_task->is_initiator = false;
        }
        task->next_task_ids.clear();
    }

    // Identifies the set of initiator tasks (in-degree 0).
    for (auto task : this->tasks) {
        if (task->is_initiator) {
            // todo: why did I set it back to false? it's harmless, but why? 
            task->is_initiator = false;
            this->initiators.push_back(task);
        }
        if (task->is_finisher) {
            task->is_finisher = false;
            this->finishers.push_back(task);
        }
    }

    max_rank = 0;
    int ranked_tasks = 0;

    // Queue of tasks to be processed in BFS order. Initialize it with all
    // tasks that have no predecessors.
    std::queue<PTask *> queue;
    for (auto task : this->initiators) {
        task->rank_bfs_queued = true;
        task->rank = 0;
        queue.push(task);
        ranked_tasks += 1;
    }

    // Computes ranks in BFS order.
    while (!queue.empty()) {
        PTask *task = queue.front();
        queue.pop();

        for (auto next_task : task->next_tasks) {
            next_task->rank = std::max(next_task->rank, task->rank + 1);
            max_rank = std::max(max_rank, next_task->rank);

            if (not next_task->rank_bfs_queued){
                queue.push(next_task);
                ranked_tasks += 1;
                next_task->rank_bfs_queued = true;
            }
        }
    }
    spdlog::debug("total tasks in protocol {}, ranked tasks: {}, max rank: {}", this->tasks.size(), ranked_tasks, max_rank);
}

std::vector<Flow*> Protocol::get_flows() {
    std::vector<Flow*> flows;
    for (auto task : tasks) {
        if (task->get_type() == PTaskType::FLOW) {
            flows.push_back((Flow*)task);
        }
    }
    return flows;
}

void Protocol::export_graph(std::ofstream& protocol_log){
    for (auto& task : this->tasks) {
        task->print_task_info(protocol_log);
    }
}

void Protocol::export_dot(std::string filename){
    std::ofstream protocol_log;

    std::string dot_path = GConf::inst().output_dir + "/" + filename + ".dot";
    std::string png_path = GConf::inst().output_dir + "/" + filename + ".png";

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
            EmptyTask *empty_task = (EmptyTask *)task;
            label = empty_task->name; 
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
    per_job_task_id = -1;
    is_initiator = true; // default is true, unless it has a predecessor
    is_finisher = true;  // default is true, unless it has a successor
    is_on_critical_path = false; 
    status = PTaskStatus::BLOCKED;
    dep_left = 0;
    start_time = 0;
    end_time = 0;
    next_tasks.clear();
    next_task_ids.clear();
    id = -1;
    protocol = nullptr;
    rank = -1;
    rank_bfs_queued = false;
}

void PTask::add_next_task_id(int id) {
    this->next_task_ids.push_back(id);
}

void PTask::add_to_next(PTask *task) {
    this->next_tasks.push_back(task);    
    task->prev_tasks.push_back(this);
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
    name = "Empty";
    print_message = ""; 
    print_on_exec = false; 
}

PTask* EmptyTask::make_shallow_copy(){
    EmptyTask *new_task = new EmptyTask();
    return new_task;
}


////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////



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

double PComp::crude_remaining_time_estimate(){
    return (this->size - this->progress); 
}