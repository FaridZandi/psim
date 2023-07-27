
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


struct ft_loc{
    int pod;
    int rack;
    int server;
    int dir; 
    int core;

    bool operator<(const ft_loc& rhs) const {
        if (pod < rhs.pod) return true;
        if (pod > rhs.pod) return false;
        if (rack < rhs.rack) return true;
        if (rack > rhs.rack) return false;
        if (server < rhs.server) return true;
        if (server > rhs.server) return false;
        if (dir < rhs.dir) return true;
        if (dir > rhs.dir) return false;
        if (core < rhs.core) return true;
        if (core > rhs.core) return false; 
        
        return false;
    }
};




class FatTreeNetwork : public Network {
public:
    FatTreeNetwork();
    virtual ~FatTreeNetwork();

    void set_path(Flow* flow);
private: 
    int server_count;
    int server_per_rack; 
    int rack_per_pod;
    int agg_per_pod;
    int pod_count;
    int core_count;

    int core_capacity;
    int agg_capacity;
    int tor_capacity;
    int server_tor_link_capacity; 
    int tor_agg_link_capacity; 
    int agg_core_link_capacity;

    int core_link_per_agg;

    std::map<ft_loc, Bottleneck *> tor_bottlenecks;
    std::map<ft_loc, Bottleneck *> agg_bottlenecks;
    std::map<ft_loc, Bottleneck *> core_bottlenecks;
    std::map<ft_loc, Bottleneck *> server_tor_bottlenecks;
    std::map<ft_loc, Bottleneck *> tor_agg_bottlenecks;
    std::map<ft_loc, Bottleneck *> pod_core_bottlenecks;
    
    std::map<int, ft_loc> server_loc_map;
    std::map<ft_loc, int> pod_core_agg_map;
};

class Machine {
public:
    Machine(int name);
    virtual ~Machine();
    int name;
    
    double make_progress(double step_size, std::vector<PComp*> & step_finished_tasks);
    std::queue<PComp*, std::deque<PComp*> > task_queue;

    std::vector<int> task_queue_length_history;
private:
};

class Bottleneck {
public:
    Bottleneck(double bandwidth);
    virtual ~Bottleneck();

    void register_rate(double rate, int priority = 0);
    void reset_register(); 
    double get_allocated_rate(double registered_rate, int priority = 0);
    bool should_drop(double step_size);

    double bandwidth;
    double total_register;
    std::map<int, double> register_map;
    
    double total_allocated; 
    int id;

    std::vector<double> total_register_history;
    std::vector<double> total_allocated_history; 

    static const int priority_levels = 3; 

private: 
};

} // namespace psim
#endif // NETWORK_H