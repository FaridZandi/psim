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
#include "gconfig.h"
#include "traffic_gen.h"
#include <functional>

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

struct history_entry {
    int time;
    int flow_count;
    int step_finished_flows;
    int comp_task_count;
    int step_finished_comp_tasks;
    double step_comm;
    double step_comp;
    double total_bw_utilization;
    double total_core_bw_utilization;
    double min_core_link_bw_utilization;
    double max_core_link_bw_utilization;
    double total_link_bandwidth;
    double total_accelerator_capacity;
};


class PSim {
public:

    PSim();
    virtual ~PSim();
    virtual double simulate();
    void add_protocol(Protocol *protocol);
    
    void inform_network_of_protocols();

    void add_protocols_from_input();

    void log_results(); 
    
    Network *network;

private:
    void handle_task_completion(PTask *task);
    void start_next_tasks(PTask *task);
    void start_task(PTask *task);
    void save_run_results();
    void mark_critical_path(); 
    void traverse_critical_path(PTask* task); 
    
    TrafficGen *traffic_gen;
    std::vector<Protocol *> protocols;
    std::vector<Flow *> finished_flows;
    std::vector<PComp *> finished_compute_tasks;

    // maybe move this to the network later as well.
    std::vector<PComp *> compute_tasks;


    double timer;
    double step_size;

    int total_task_count;
    int finished_task_count;

    std::vector<history_entry> history;
    void log_history_entry(history_entry& h);

    void draw_plots(std::initializer_list<std::pair<std::string, std::function<double(history_entry)>>> plots);

};

} // namespace psim
#endif // PSIM_H
