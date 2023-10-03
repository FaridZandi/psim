
#ifndef NETWORK_H
#define NETWORK_H

#include <vector>
#include <map>
#include <queue>
#include <deque>
#include "protocol.h"
#include "config.h"
#include "bwalloc.h"

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

enum core_selection{
    RANDOM,
    ROUND_ROBIN,
    LEAST_LOADED,
    FUTURE_LOAD,
};

class Network {
public:
    Network();
    virtual ~Network();

    std::vector<Machine *> machines;
    std::vector<Bottleneck *> bottlenecks;

    std::map<int, Machine *> machine_map;
    std::map<int, Bottleneck *> bottleneck_map;

    Machine* get_machine(int name);
    
    void reset_bottleneck_registers();
    void compute_bottleneck_allocations();
    double make_progress_on_machines(double current_time, double step_size, 
                                     std::vector<PComp*> & step_finished_tasks);

    virtual void set_path(Flow* flow, double timer) = 0;

    Bottleneck* create_bottleneck(double bandwidth);

    //temp
    virtual void print_core_link_status(double timer) {}; 

    virtual double total_link_bandwidth(); 
    virtual double total_bw_utilization(); 
    virtual double total_core_bw_utilization();
private: 

};


class BigSwitchNetwork : public Network {
public:
    BigSwitchNetwork();
    virtual ~BigSwitchNetwork();

    void set_path(Flow* flow, double timer);

private: 
    std::map<int, Bottleneck *> server_bottlenecks_downstream;
    std::map<int, Bottleneck *> server_bottlenecks_upstream;

    int server_count = 128;
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

    void set_path(Flow* flow, double timer);
private: 
    int server_count;
    int server_per_rack; 
    int rack_per_pod;
    int agg_per_pod;
    int pod_count;

    int core_count;
    double server_tor_link_capacity; 
    double tor_agg_link_capacity; 
    double agg_core_link_capacity;

    int core_link_per_agg;

    int * core_usage_count;
    double * core_usage_sum;

    std::map<ft_loc, Bottleneck *> server_tor_bottlenecks;
    std::map<ft_loc, Bottleneck *> tor_agg_bottlenecks;
    std::map<ft_loc, Bottleneck *> pod_core_bottlenecks;
    
    std::map<int, ft_loc> server_loc_map;
    std::map<ft_loc, int> pod_core_agg_map;

    void print_core_link_status(double timer);

    int select_core(Flow* flow, 
                    double timer, 
                    core_selection mechanism = core_selection::ROUND_ROBIN);

    int* last_agg_in_pod;

    int select_agg(Flow* flow, int pod_number);

    double total_core_bw_utilization();

};



class Machine {
public:
    Machine(int name);
    virtual ~Machine();
    int name;
    
    double make_progress(double current_time, double step_size, std::vector<PComp*> & step_finished_tasks);
    std::queue<PComp*, std::deque<PComp*> > task_queue;

    std::vector<int> task_queue_length_history;
private:
};



class Bottleneck {
public:
    Bottleneck(double bandwidth);
    virtual ~Bottleneck();

    bool should_drop(double step_size);

    // priority allocation wrapper functions 
    BandwidthAllocator* bwalloc; 
    void register_rate(int id, double rate, int priority = 0);
    void reset_register(); 
    void allocate_bandwidths(); 
    double get_allocated_rate(int id, double registered_rate, int priority = 0);

    // basic info
    int id;
    double bandwidth;
    int current_flow_count; 
    double current_flow_size_sum; 
    std::vector<Flow*> flows;

    // history
    std::vector<double> total_register_history;
    std::vector<double> total_allocated_history; 

private: 
    static int bottleneck_counter;
    void setup_bwalloc(); 
};

} // namespace psim
#endif // NETWORK_H