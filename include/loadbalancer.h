#ifndef LOADBALANCER_H
#define LOADBALANCER_H

#include <map>
#include <vector>
#include "protocol.h"
#include "network.h"
#include "gconfig.h"

namespace psim {

class Bottleneck;
class Flow;
class Protocol;

// Network Loadbalancer.
// The structure is like this: There a bunch of items at the lower level, and
// the there are a bunch of items in the upper level. The lower level items
// are connected to the upper level in a all-to-all fashion. The load balancer
// is invoket with two items from lower level as input. The load balancer then
// decides which upper level item should be used to connect the two lower level
// items. the connections are bidirectional, so there will be actually two
// connections between every lower level and upper level item.

class LoadBalancer {
public:
    LoadBalancer(int item_count);
    virtual ~LoadBalancer() {}

    void register_link(int lower_item, int upper_item, int dir, Bottleneck* link);
    void add_flow_sizes(std::map<int, std::vector<double>>& src_flow_sizes);
    void update_state(Flow* arriving_flow = nullptr);
    virtual int get_upper_item(int src, int dst, Flow* flow, int timer) = 0;

    static LoadBalancer* create_load_balancer(int item_count, LBScheme lb_scheme);

protected:
    int item_count;
    std::map<std::pair<int, int>, Bottleneck*> link_up_map;
    std::map<std::pair<int, int>, Bottleneck*> link_down_map;

    Bottleneck* uplink(int lower_item, int upper_item);
    Bottleneck* downlink(int lower_item, int upper_item);
};

std::pair<int, int> get_prof_limits(double start_time, double end_time);


/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////

class RandomLoadBalancer : public LoadBalancer {
public:
    RandomLoadBalancer(int item_count);
    virtual ~RandomLoadBalancer() {}
    int get_upper_item(int src, int dst, Flow* flow, int timer) override;
};

/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////

class RoundRobinLoadBalancer : public LoadBalancer {
public:
    RoundRobinLoadBalancer(int item_count);
    virtual ~RoundRobinLoadBalancer() {}
    int get_upper_item(int src, int dst, Flow* flow, int timer) override;

private:
    int current_upper_item = 0;
};

/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////

class PowerOfKLoadBalancer : public LoadBalancer {
public:
    PowerOfKLoadBalancer(int item_count, int samples = 2);
    virtual ~PowerOfKLoadBalancer() {}

    int get_upper_item(int src, int dst, Flow* flow, int timer) override;

private:
    const int num_samples;
    int prev_best_item;
};

/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////

class LeastLoadedLoadBalancer : public LoadBalancer {
public:
    LeastLoadedLoadBalancer(int item_count);
    virtual ~LeastLoadedLoadBalancer() {}

    int get_upper_item(int src, int dst, Flow* flow, int timer) override;
};


/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////

// The RobinHood Algorithm works as follows: it maintains a lower bound on the
// optimal balance, `lb`, and deems links having loads at least `sqrt(N) * lb`
// as hard-working, where `N` is the number of links. Otherwise, the link is
// non hard-working. It tries to give traffic to the non hard working if there
// is any or the link that most recently became hard-working otherwise.
//
// As we are balancing on paths and the load balancer connects two lower items
// to an upper item we say a path is non hard-working if both the uplink and
// downlink are non hard-working, otherwise, the path is hard-working.
class RobinHoodLoadBalancer : public LoadBalancer {
public:
    RobinHoodLoadBalancer(int item_count);
    virtual ~RobinHoodLoadBalancer() {}
    void register_link(int lower_item, int upper_item, int dir, Bottleneck* link);
    void update_state(Flow* arriving_flow = nullptr);
    int get_upper_item(int src, int dst, Flow* flow, int timer) override;


private:
    // An array of number of consecutive iterations a given link has been
    // hard-working.
    std::map<Bottleneck*, int> iterations_hard_working;

    // A coarse-grained estimate of the current load in the network that is
    // only updated each time `update_state()` is called.
    double load;

    // Multiplier for the robin hood algorithm (sqrt of the link count but we
    // store it so that we don't have to compute it each time).
    double multiplier;

    // Lower bound on the optimal.
    double lb;

    double get_multiplier();
};


/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////

class FutureLoadLoadBalancer : public LoadBalancer {
public:
    FutureLoadLoadBalancer(int item_count);
    virtual ~FutureLoadLoadBalancer() {}
    int get_upper_item(int src, int dst, Flow* flow, int timer) override;

private:
    double get_flow_load_estimate(Flow* flow);

    void remove_flow_from_last_run_data(Flow* flow, int src, int dst,
                                        std::pair<int, int> last_run_prof);

    std::vector<double> get_core_loads_estimate(Flow* flow, int src, int dst, 
                                                std::pair<int, int> this_run_prof);

    void add_flow_load_to_last_run_data(Flow* flow, int src, int dst, 
                                        std::pair<int, int> this_run_prof, 
                                        double flow_load_estimate, 
                                        int best_core);
                                    
    int my_round_robin();
    int current_upper_item;
};


/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////

class SitaELoadBalancer : public LoadBalancer {
public:
    SitaELoadBalancer(int item_count);
    virtual ~SitaELoadBalancer() {}
    void add_flow_sizes(std::map<int, std::vector<double>>& src_flow_sizes);
    int get_upper_item(int src, int dst, Flow* flow, int timer) override;

private:
    int current_upper_item;
    std::map<int, std::vector<double>> src_size_thresholds;
};

} // namespace psim
#endif
