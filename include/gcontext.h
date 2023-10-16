#ifndef CONTEXT_H
#define CONTEXT_H

#include <map>
#include <vector>
#include <set>

#include "gconfig.h"

namespace psim {
class Flow; 

struct core_link_status {
    int time; 
    std::map<int, double> link_loads; 
    std::map<int, double> flow_loads;
};

struct run_info{
    int run_number = 0; 
    int max_time_step = 0;

    std::map<int, core_link_status> network_status;
    std::map<int, int> core_decision; 
    std::map<int, double> flow_fct;
    std::map<int, double> flow_start; 
    std::map<int, double> flow_end; 
    std::map<int, double> least_load; 

};


class GContext {
public:

    static GContext& inst();
    static run_info& this_run();
    static run_info& last_run() ;
    static bool is_first_run() ;
    static void save_decision(int flow_id, int decision);
    static const int last_decision(int flow_id);
    static void start_new_run();
    static void initiate_device_shuffle_map();
    static int get_device_shuffle_map(int device_id);


    GContext(GContext const&) = delete;
    void operator=(GContext const&) = delete;
    
    std::map<int, int> core_selection; 
    int cut_off_time = 0; 
    int cut_off_decrease_step = 0; 
    std::vector<run_info> run_info_list;
    std::map<int, int> device_shuffle_map; 
    int run_counter = 0; 

private:
    GContext();

};
    
} // namespace psim
#endif // CONFIG_H