#ifndef PSIM_H
#define PSIM_H

#include <vector>
#include <map>
#include <cmath>
#include <queue>
#include <limits>
#include <algorithm>
#include <cstdlib>
#include <iomanip>
#include <fstream>
#include "config.h"
#include "traffic_gen.h"

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

class PSim {
public:
    
    PSim();
    virtual ~PSim();
    virtual double simulate();
    void add_protocol(Protocol *protocol);

private: 
    void handle_task_completion(PTask *task);
    void start_next_tasks(PTask *task);
    void start_task(PTask *task);
    double make_progress_on_flows(double current_time, std::vector<Flow*> & step_finished_flows); 
    void save_run_results();

    TrafficGen *traffic_gen;
    
    std::vector<Protocol *> protocols;
    std::vector<Flow *> flows; 
    std::vector<PComp *> compute_tasks;
    Network *network;
    double timer;
    double step_size;
    int total_task_count; 
    int finished_task_count;

    std::vector<Flow *> finished_flows;
    std::vector<PComp *> finished_compute_tasks;
    std::vector<double> comm_log;
    std::vector<double> comp_log;
    std::vector<int> flow_count_history; 
    double total_comm = 0; 
    double total_comp = 0;


}; 

} // namespace psim
#endif // PSIM_H