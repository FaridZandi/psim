
#ifndef NETWORK_H
#define NETWORK_H

#include <vector>
#include <map>
#include <queue>
#include <deque>
#include "protocol.h"
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
                                   std::vector<PComp*> & step_finished_tasks);

    virtual void set_path(Flow* flow) = 0;

    Bottleneck* create_bottleneck(double bandwidth);
    
private: 

};


class BigSwitchNetwork : public Network {
public:
    BigSwitchNetwork();
    virtual ~BigSwitchNetwork();

    void set_path(Flow* flow);

private: 
    Bottleneck* switch_bottleneck;
    std::map<int, Bottleneck *> server_bottlenecks_downstream;
    std::map<int, Bottleneck *> server_bottlenecks_upstream;

    int server_count = 128;
    int switch_capacity;
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
    
    double make_progress(double step_size, std::vector<PComp*> & step_finished_tasks);
    std::queue<PComp*, std::deque<PComp*> > task_queue;

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

} // namespace psim
#endif // NETWORK_H