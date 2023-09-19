#include "traffic_gen.h"
#include "config.h"

using namespace psim; 

TrafficGen::TrafficGen(double load) {
    this->protocol = new Protocol();
    this->protocol->type = ProtocolType::BACKGROUND_PROTOCOL;
    
    machine_count = GConf::inst().machine_count;
    this->load = load;

    double link_bandwidth = GConf::inst().link_bandwidth;
    double all_to_all = (double)machine_count * (double)(machine_count - 1); 
    all_to_all = load * link_bandwidth * all_to_all; 
    double mean_flow_size = get_flow_size_mean();

    
    
}

TrafficGen::~TrafficGen() {
}

std::vector<Flow*> TrafficGen::get_flows(double timestep){


    double prob = timestep * poisson_lambda;
    double r = (double)rand() / (double)RAND_MAX;

    Flow* flow = (Flow*)protocol->create_task(PTaskType::FLOW);
    flow->size = get_flow_size();
    flow->src_dev_id = 1;
    flow->dst_dev_id = 2;

    std::vector<Flow*> flows;
    flows.push_back(flow);
    
    return flows;
}

double TrafficGen::get_flow_size() {
    // get a random number between 0 and 1
    double r = (double)rand() / (double)RAND_MAX;

    if (r < 0.5) {
        return 1.0;
    } else if (r < 0.75) {
        return 10.0;
    } else if (r < 0.875) {
        return 100.0;
    } else if (r < 0.9375) {
        return 1000.0;
    } else if (r < 0.96875) {
        return 10000.0;
    } else if (r < 0.984375) {
        return 100000.0;
    } else if (r < 0.9921875) {
        return 1000000.0;
    } else if (r < 0.99609375) {
        return 10000000.0;
    } else if (r < 0.998046875) {
        return 100000000.0;
    } else if (r < 0.9990234375) {
        return 1000000000.0;
    } else {
        return 10000000000.0;
    }
}

    

double TrafficGen::get_flow_size_mean(){
    double mean = 0.5 * 1 + 0.25 * 10 + 0.125 * 100 + 0.0625 * 1000 + 0.03125 * 10000 + 0.015625 * 100000 + 0.0078125 * 1000000 + 0.00390625 * 10000000 + 0.001953125 * 100000000 + 0.0009765625 * 1000000000 + 0.0009765625 * 10000000000;
    
    return mean;
}
                