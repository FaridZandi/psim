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
    std::vector<int> next_task_ids;
    int dep_left;
    int id;
    int rank;
    bool rank_bfs_queued;

    bool about_to_finish; // to deal with double precision errors
    double start_time;
    double end_time;

    PTaskStatus status;
    bool is_initiator;

    virtual PTaskType get_type() = 0;
    virtual void print_task_info(std::ostream& os) = 0;
    virtual void reset();

    virtual PTask* make_shallow_copy() = 0;
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

    int src_dev_id;
    int dst_dev_id;
    double size;
    double progress;

    int selected_priority;
    int bn_priority_levels;

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

private:

};




class Protocol {
public:
    Protocol();
    virtual ~Protocol();

    PTask* create_task(PTaskType type, int id = -1);
    void add_to_tasks(PTask *task, int id = -1);
    void build_dependency_graph();

    void export_graph(std::ofstream& protocol_log);
    void export_dot(std::string filename);
    Protocol *make_copy(bool build_dependency_graph = true);

    ProtocolType type;
    std::vector<PTask *> tasks;
    std::map<int, PTask *> task_map;
    std::vector<PTask *> initiators;
    int max_rank;
    int max_allocated_id;

    int total_task_count;
    int finished_task_count;

private:

};



} // namespace havij
#endif /* PROTOCOL_H */
