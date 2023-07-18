#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <vector>
#include <map>
#include <ostream>
#include "network.h"
#include "constants.h"


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

    std::vector<PTask *> next_tasks;
    std::vector<int> next_task_ids;
    int dep_left; 
    int id;

    double start_time; 
    double end_time;

    PTaskStatus status;
    bool is_initiator;

    virtual PTaskType get_type() = 0;
    virtual void print_task_info(std::ostream& os) = 0;
private:    

};

class EmptyTask : public PTask {
public:
    EmptyTask();
    virtual ~EmptyTask();
    
    PTaskType get_type() {return PTaskType::EMPTY;}

    void print_task_info(std::ostream& os);

private:

};

class Flow : public PTask {
public:
    Flow(); 
    virtual ~Flow();
    
    double current_rate; 
    double registered_rate; 

    int src_dev_id; 
    int dst_dev_id; 
    double size;
    double progress;

    std::vector<Bottleneck *> path;
    Machine *src;
    Machine *dst;

    void register_rate_on_path(double step_size);
    double make_progress(double step_size);
    void update_rate(double step_size);

    PTaskType get_type() {return PTaskType::FLOW;}

    void print_task_info(std::ostream& os);

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

private:

};


class Protocol {
public:
    Protocol();
    virtual ~Protocol();
    PTask* create_task(PTaskType type, int id);

    void build_dependency_graph();

    void export_graph(std::ofstream protocol_log);

    std::vector<PTask *> tasks;
    std::map<int, PTask *> task_map;
    std::vector<PTask *> initiators;

    int total_task_count;
    int finished_task_count;
private:

};



} // namespace havij
#endif /* PROTOCOL_H */