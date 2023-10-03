#ifndef CONTEXT_H
#define CONTEXT_H
#include <map>
#include <vector>
#include <set>


namespace psim {
class Flow; 

struct core_link_status{
    std::map<std::pair<int, int>, double> core_link_registered_rate_map_up;
    std::map<std::pair<int, int>, double> core_link_registered_rate_map_down;

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
    static GContext& inst() {
        static GContext instance;
        return instance;
    }

    static run_info& this_run() {
        return inst().run_info_list.back();
    }

    static run_info& last_run() {
        return inst().run_info_list[inst().run_info_list.size() - 2];
    }

    static bool first_run() {
        return inst().run_info_list.size() == 1;
    }   

    static void save_decision(int flow_id, int decision) {
        this_run().core_selection_decision_map[flow_id] = decision;
    }

    static const int last_decision(int flow_id) {
        return last_run().core_selection_decision_map[flow_id];
    }

    static void start_new_run() {
        inst().run_info_list.push_back(run_info());
    }

    GContext(GContext const&) = delete;
    void operator=(GContext const&) = delete;

    std::map<int, int> core_selection; 
    std::map<int, double> flow_avg_transfer_rate;
    int cut_off_time = 10000000; 

    std::vector<run_info> run_info_list;

private:
    GContext() {}
};
    
} // namespace psim
#endif // CONFIG_H