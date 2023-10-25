#ifndef LOADBALANCER_H
#define LOADBALANCER_H

#include <map>
#include "network.h"
#include "gconfig.h"

namespace psim {

class Bottleneck;
class Flow;

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
    virtual int get_upper_item(int src, int dst, Flow* flow, int timer) = 0;

    static LoadBalancer* create_load_balancer(int item_count, LBScheme lb_scheme);

protected:
    int item_count;
    std::map<std::pair<int, int>, Bottleneck*> link_up_map;
    std::map<std::pair<int, int>, Bottleneck*> link_down_map;

    Bottleneck* uplink(int lower_item, int upper_item);
    Bottleneck* downlink(int lower_item, int upper_item);
};

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

class PowerOf2LoadBalancer : public LoadBalancer {
public:
    PowerOf2LoadBalancer(int item_count);
    virtual ~PowerOf2LoadBalancer() {}

    int get_upper_item(int src, int dst, Flow* flow, int timer) override;

private:
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


class RobinHoodLoadBalancer : public LoadBalancer {
public:
    RobinHoodLoadBalancer(int item_count);
    virtual ~RobinHoodLoadBalancer() {}

    int get_upper_item(int src, int dst, Flow* flow, int timer) override;


private:
    // An array of number of iterations a given core has been hard working.
    // Used for the robin-hood load balancing algorithm that selects cores that
    // are not hard working or the core that most recently became hard working
    // if all cores are hard working.
    std::vector<int> iterations_hard_working;

    // Multiplier for the robin hood algorithm (sqrt of the core count but we
    // store it so that we don't have to compute it each time).
    const double multiplier;

    // Lower bound on the optimal.
    double lb;
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
    int my_round_robin();
    int current_upper_item;
};

} // namespace psim
#endif
