
#ifndef NETWORK_H
#define NETWORK_H

#include <vector>
#include <map>
#include <queue>
#include <deque>
#include "protocol.h"
#include "gconfig.h"
#include "bwalloc.h"
#include "loadbalancer.h"
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
class LoadBalancer;


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
    virtual int get_source_for_flow(Flow* flow) = 0;
    void integrate_protocol_knowledge(std::vector<Protocol*>& protocols);

    Bottleneck* create_bottleneck(double bandwidth);

    //temp
    virtual void record_link_status(double timer) {};

    virtual double total_link_bandwidth();
    virtual double total_bw_utilization();
    virtual double total_core_bw_utilization();
    virtual double min_core_link_bw_utilization();
    virtual double max_core_link_bw_utilization();

    std::vector<Flow *> flows;
    double make_progress_on_flows(double current_time, std::vector<Flow*> & step_finished_flows);

    LoadBalancer* core_load_balancer;

private:

};


class BigSwitchNetwork : public Network {
public:
    BigSwitchNetwork();
    virtual ~BigSwitchNetwork();

    void set_path(Flow* flow, double timer);
    int get_source_for_flow(Flow* flow);
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

    bool operator<(const ft_loc& rhs) const;
};

std::pair<int, int> get_prof_limits(double start_time, double end_time);


class CoreConnectedNetwork : public Network {
public:
    CoreConnectedNetwork();
    virtual ~CoreConnectedNetwork();

    int core_count;
    int server_count;
    int core_link_capacity;

    std::map<int, ft_loc> server_loc_map;
    std::map<ft_loc, Bottleneck *> core_bottlenecks;


    LBScheme lb_scheme;

    void record_link_status(double timer);

    double total_core_bw_utilization();
    double min_core_link_bw_utilization();
    double max_core_link_bw_utilization();
};

class FatTreeNetwork : public CoreConnectedNetwork {
public:
    FatTreeNetwork();
    virtual ~FatTreeNetwork();

    void set_path(Flow* flow, double timer);
    int get_source_for_flow(Flow* flow);
private:
    int server_per_rack;
    int rack_per_pod;
    int agg_per_pod;
    int pod_count;
    int core_link_per_agg;

    double server_tor_link_capacity;
    double tor_agg_link_capacity;

    std::map<ft_loc, Bottleneck *> server_tor_bottlenecks;
    std::map<ft_loc, Bottleneck *> tor_agg_bottlenecks;
    std::map<ft_loc, int> pod_core_agg_map;

    int* last_agg_in_pod;
    int select_agg(Flow* flow, int pod_number, LBScheme mechanism);
};



class LeafSpineNetwork : public CoreConnectedNetwork {
public:
    LeafSpineNetwork();
    virtual ~LeafSpineNetwork();

    void set_path(Flow* flow, double timer);
    int get_source_for_flow(Flow* flow);
private:
    int server_per_rack;
    int tor_count;
    double server_tor_link_capacity;

    std::map<ft_loc, Bottleneck *> server_tor_bottlenecks;
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
    static int bottleneck_counter;

    bool should_drop(double step_size);

    // priority allocation wrapper functions
    BandwidthAllocator* bwalloc;
    void register_rate(int id, double rate, int priority = 0);
    void reset_register();
    void allocate_bandwidths();
    double get_allocated_rate(int id, double registered_rate, int priority = 0);

    double get_load(LoadMetric load_metric_arg = LoadMetric::DEFAULT);

    // basic info
    int id;
    double bandwidth;
    int current_flow_count;
    double current_flow_size_sum;
    std::vector<Flow*> flows;
    LoadMetric load_metric;

    // history
    std::vector<double> total_register_history;
    std::vector<double> total_allocated_history;

private:
    void setup_bwalloc();
};

} // namespace psim
#endif // NETWORK_H
