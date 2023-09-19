#ifndef TRAFFIC_GEN_H
#define TRAFFIC_GEN_H

#include "protocol.h"
#include <cstdlib>
#include <vector>

namespace psim {


class TrafficGen {
public:
    TrafficGen(double load);
    virtual ~TrafficGen();

    double get_flow_size();
    double get_flow_size_mean(); 

    std::vector<Flow*> get_flows(double timestep);

    Protocol *protocol;
    int machine_count;
    double load; 
    double poisson_lambda;


private: 
};


} // namespace psim
#endif // TRAFFIC_GEN_H