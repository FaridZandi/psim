#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <vector>
#include <map>
#include <ostream>
#include "network.h"
#include "config.h"


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
    double initial_rate; 
    double min_rate; 
    double registered_rate; 
    double bn_allocated_rate;
    double rate_increase; 

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
    void compute_priority(); 
    void register_rate_on_path(double step_size);
    double make_progress(double current_time, double step_size);
    void update_rate(double step_size);

    PTaskType get_type() {return PTaskType::FLOW;}

    void print_task_info(std::ostream& os);

    void reset(); 

    PTask* make_shallow_copy(); 
private:

};

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

    static Protocol* build_random_protocol(int num_comp, int machine_count); 
    static Protocol* load_protocol_from_file(std::string file_path); 
    static Protocol* pipelinize_protocol(Protocol *proto, int num_replicas, bool tight_connections = false);
    static Protocol* super_simple_protocol();
    static Protocol* simple_pipeline_protocol(int length);


    std::vector<PTask *> tasks;
    std::map<int, PTask *> task_map;
    std::vector<PTask *> initiators;
    int max_rank; 

    int total_task_count;
    int finished_task_count;

    int max_allocated_id; 

private:

};



} // namespace havij
#endif /* PROTOCOL_H */