#ifndef CONTEXT_H
#define CONTEXT_H

#include <map>
#include <vector>
#include <set>

#include "config.h"

namespace psim {
class Flow; 

struct core_link_status {
    std::map<std::pair<int, int>, double> core_link_registered_rate_map_up;
    std::map<std::pair<int, int>, double> core_link_registered_rate_map_down;

    std::map<int, double> link_loads; 
    
    std::set<Flow*> flows;

    double current_flow_rate_sum = 0;
    double last_flow_rate_sum = 0; 
};

struct run_info{
    std::map<int, core_link_status> core_link_status_map;
    std::map<int, int> core_selection_decision_map; 
    std::map<int, double> flow_completion_time_map;
    int max_time_step = 0;
};


class GContext {
public:

    static GContext& inst();
    static run_info& this_run();
    static run_info& last_run() ;
    static bool first_run() ;
    static void save_decision(int flow_id, int decision);
    static const int last_decision(int flow_id);
    static void start_new_run();
    static void initiate_device_shuffle_map();
    static int get_device_shuffle_map(int device_id);


    GContext(GContext const&) = delete;
    void operator=(GContext const&) = delete;
    
    std::map<int, int> core_selection; 
    std::map<int, double> flow_avg_transfer_rate;
    int cut_off_time = 0; 
    int cut_off_decrease_step = 0; 
    std::vector<run_info> run_info_list;
    std::map<int, int> device_shuffle_map; 

private:
    GContext();

};
    
} // namespace psim
#endif // CONFIG_H