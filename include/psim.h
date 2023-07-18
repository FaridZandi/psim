#ifndef HavijSimulator_H_
#define HavijSimulator_H_

#include <vector>
#include <map>
#include <cmath>
#include <queue>
#include <limits>
#include <algorithm>
#include <cstdlib>
#include <iomanip>
#include <fstream>

namespace havij {

class Protocol;
class HavijSimulator;
class Network;
class FatTreeNetwork;
class Machine;
class Bottleneck;
class Flow;
class HavijTask;
class ComputeTask;
class EmptyTask;

enum HavijTaskType {
    FLOW,
    COMPUTE,
    EMPTY,
};

enum HavijTaskStatus {
    BLOCKED,
    RUNNING,
    FINISHED,
};

class Protocol {
public:
    Protocol();
    virtual ~Protocol();
    HavijTask* create_task(HavijTaskType type, int id);

    void build_dependency_graph();

    void export_graph(std::ofstream protocol_log);

    std::vector<HavijTask *> tasks;
    std::map<int, HavijTask *> task_map;
    std::vector<HavijTask *> initiators;

    int total_task_count;
    int finished_task_count;
private:

};

class HavijSimulator {
public:
    HavijSimulator();
    
    virtual ~HavijSimulator();

    virtual double simulate();

    Protocol *protocol;

private: 
    double timer;
    double step_size;

    Network *network;

    std::vector<Flow *> flows; 
    std::vector<ComputeTask *> compute_tasks;

    std::vector<Flow *> finished_flows;
    std::vector<ComputeTask *> finished_compute_tasks;
    
    void start_next_tasks(HavijTask *task);
    void start_task(HavijTask *task);
    double make_progress_on_flows(std::vector<Flow*> & step_finished_flows); 

}; 



class Network {
public:
    static int bottleneck_counter;

    Network();
    virtual ~Network();

    std::vector<Machine *> machines;
    std::vector<Bottleneck *> bottlenecks;

    std::map<int, Machine *> machine_map;
    std::map<int, Bottleneck *> bottleneck_map;

    Machine* get_machine(int name);
    
    void reset_bottleneck_registers();

    double make_progress_on_machines(double step_size, 
                                   std::vector<ComputeTask*> & step_finished_tasks);

    virtual void set_path(Flow* flow) = 0;

    Bottleneck* create_bottleneck(double bandwidth);
    
private: 

};


class BigSwitchNetwork : public Network {
public:
    BigSwitchNetwork(double iface_bandwidth);
    virtual ~BigSwitchNetwork();

    void set_path(Flow* flow);

private: 
    Bottleneck* switch_bottleneck;
    std::map<int, Bottleneck *> server_bottlenecks_downstream;
    std::map<int, Bottleneck *> server_bottlenecks_upstream;

    int server_count = 16;
    int switch_capacity = 40;
    int server_switch_link_capacity = 40;
};

class FatTreeNetwork : public Network {
public:
    FatTreeNetwork();
    virtual ~FatTreeNetwork();

    void set_path(Flow* flow);

private: 
    int server_per_rack = 4; 
    int rack_per_pod = 2;
    int agg_per_pod = 2;
    int pod_count = 2;
    int core_count = 2;
    int core_capacity = 20;
    int agg_capacity = 20;
    int tor_capacity = 20;
    int server_tor_link_capacity = 10;
    int tor_agg_link_capacity = 10;
    int agg_core_link_capacity = 20;  
};

class Machine {
public:
    Machine(int name);
    virtual ~Machine();
    int name;
    
    double make_progress(double step_size, std::vector<ComputeTask*> & step_finished_tasks);
    std::queue<ComputeTask*, std::deque<ComputeTask*> > task_queue;

private:
};

class Bottleneck {
public:
    Bottleneck(double bandwidth);
    virtual ~Bottleneck();

    void register_rate(double rate);
    void reset_register(); 
    double get_allocated_rate(double registered_rate);
    bool should_drop(double step_size);

    double bandwidth;
    double total_register;
    double total_allocated; 
    int id;  
private: 
};


class HavijTask {
public:
    HavijTask();
    virtual ~HavijTask();
    
    void add_next_task_id(int id);
    
    void add_to_next(HavijTask *task);

    std::vector<HavijTask *> next_tasks;
    std::vector<int> next_task_ids;
    int dep_left; 
    int id;

    double start_time; 
    double end_time;

    HavijTaskStatus status;
    bool is_initiator;

    virtual HavijTaskType get_type() = 0;
    virtual void print_task_info(std::ostream& os) = 0;
private:    

};

class EmptyTask : public HavijTask {
public:
    EmptyTask();
    virtual ~EmptyTask();
    
    HavijTaskType get_type() {return HavijTaskType::EMPTY;}

    void print_task_info(std::ostream& os);

private:

};

class Flow : public HavijTask {
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

    HavijTaskType get_type() {return HavijTaskType::FLOW;}

    void print_task_info(std::ostream& os);

private:

};

class ComputeTask : public HavijTask {
public:
    ComputeTask();
    virtual ~ComputeTask();

    int dev_id;
    double size;
    double progress;
     
    Machine *machine;

    HavijTaskType get_type() {return HavijTaskType::COMPUTE;}

    void print_task_info(std::ostream& os);

private:

};

} // namespace havij
#endif // HavijSimulator_H_