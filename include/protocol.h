// Execution protocol for an ML model.
#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <vector>
#include <map>
#include <ostream>
#include "network.h"
#include "gconfig.h"


namespace psim {

class Protocol;
class PSim;
class Network;
class FatTreeNetwork;
class Machine;
class Bottleneck;
class Flow;
class PTask;
class PComp;
class EmptyTask;

enum PTaskType {
    FLOW,
    COMPUTE,
    EMPTY,
};

enum PTaskStatus {
    BLOCKED,
    RUNNING,
    FINISHED,
};

enum ProtocolType {
    MAIN_PROTOCOL,
    BACKGROUND_PROTOCOL,
};



// Abstract representation of task (a node in the execution graph).
class PTask {
public:
    PTask();
    virtual ~PTask();

    void add_next_task_id(int id);

    void add_to_next(PTask *task);

    Protocol *protocol;

    std::vector<PTask *> next_tasks;
    std::vector<PTask *> prev_tasks;

    // In the input files, some tasks might point to other tasks 
    // that are not seen yet. We store the ids of those tasks here.
    // later in the "build_dependency_graph" function, we will
    // replace these ids with pointers to the actual tasks, which will 
    // be available by then and will be stored in the "next_tasks" vector.
    std::vector<int> next_task_ids;

    int dep_left;
    int id;
    int per_job_task_id; 
    int rank;
    bool rank_bfs_queued;

    double start_time;
    double end_time;

    PTaskStatus status;
    bool is_initiator;
    bool is_finisher; 
    bool is_on_critical_path; 
    
    virtual PTaskType get_type() = 0;
    virtual void print_task_info(std::ostream& os) = 0;
    virtual void reset();

    virtual PTask* make_shallow_copy() = 0;

    virtual double crude_remaining_time_estimate() {return 0.0;}
private:

};

class EmptyTask : public PTask {
public:
    EmptyTask();
    virtual ~EmptyTask();

    PTaskType get_type() {return PTaskType::EMPTY;}

    void print_task_info(std::ostream& os);

    void reset();

    PTask* make_shallow_copy();

    // let's give them names so they are happy. 
    std::string name; 
    std::string print_message; 
    bool print_on_exec; 
private:

};

class Flow : public PTask {
public:
    Flow();
    virtual ~Flow();

    double current_rate;
    double last_rate;
    double initial_rate;
    double min_rate;
    double registered_rate;
    double rate_increase;
    double min_bottleneck_rate;
    double rate_decrease_factor;
    
    std::string label_for_progress_graph; 

    int src_dev_id;
    int dst_dev_id;
    double size;
    double progress;
    int jobid; 

    int selected_priority;
    int bn_priority_levels;

    int bottlenecked_by_intermediate_count;
    int bottlenecked_by_srcdst_count;
    
    int lb_decision; 
    
    int protocol_defined_lb_decision; 
    double protocol_defined_max_rate; 
    int protocol_defined_iteration; 
    int protocol_defined_subflow_id; 
    
    std::vector<double> progress_history; 

    std::vector<Bottleneck *> path;
    Machine *src;
    Machine *dst;

    void initiate();
    void finished();
    void compute_priority();
    void register_rate_on_path(double step_size);
    double make_progress(double current_time, double step_size);
    void update_rate(double step_size);

    double get_load(LoadMetric load_metric_arg = LoadMetric::DEFAULT);

    PTaskType get_type() {return PTaskType::FLOW;}

    void print_task_info(std::ostream& os);

    void reset();

    PTask* make_shallow_copy();

    double crude_remaining_time_estimate(); 


private:
    LoadMetric load_metric;
};

// Computation task.
class PComp : public PTask {
public:
    PComp();
    virtual ~PComp();

    int dev_id;
    double size;
    double progress;

    Machine *machine;

    PTaskType get_type() {return PTaskType::COMPUTE;}

    void print_task_info(std::ostream& os);

    void reset();
    PTask* make_shallow_copy();

    double crude_remaining_time_estimate(); 

private:

};




class Protocol {
public:
    Protocol();
    virtual ~Protocol();

    PTask* create_task(PTaskType type, int id = -1);
    void add_to_tasks(PTask *task, int id = -1);
    
    void build_dependency_graph();
    std::vector<Flow*> get_flows();

    void export_graph(std::ofstream& protocol_log);
    void export_dot(std::string filename);
    Protocol *make_copy(bool build_dependency_graph = true);
    void reset_per_job_task_counter();    

    ProtocolType type;
    std::vector<PTask *> tasks;
    std::map<int, PTask *> task_map;
    std::vector<PTask *> initiators;
    std::vector<PTask *> finishers;
    int max_rank;
    int max_allocated_id;

    int per_job_task_counter;
    void increment_per_job_task_counter(); 

    int total_task_count;
    int finished_task_count;
private:

};



} // namespace havij
#endif /* PROTOCOL_H */
