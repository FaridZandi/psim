#ifndef CONTEXT_H
#define CONTEXT_H
#include <map>
#include <vector>
#include <set>
#include <sstream>
#include <fstream>

namespace psim {
class Flow; 

struct core_link_status{
    std::map<int, double> link_loads; 

    // old stuff. should be eventually phased out as we move to the new model.
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

    static void initiate_device_shuffle_map(){
        // read the shuffle map from the file.
        // there's a permutation of the numbers 0 to 127 in the file, separated by commas.

        std::string path = GConf::inst().shuffle_map_file;
        std::ifstream file(path);
        std::string line;
        std::getline(file, line);
        std::stringstream ss(line);
        std::string token;
        int i = 0;
        while(std::getline(ss, token, ',')){
            inst().device_shuffle_map[i] = std::stoi(token);
            i++;
        }

        

        // // shuffle nambers between 0 and 127, store them in the map. 
        // std::vector<int> shuffle_list;
        // for(int i = 0; i < 128; i++){
        //     shuffle_list.push_back(i);
        // }
        // std::random_shuffle(shuffle_list.begin(), shuffle_list.end());
        // for(int i = 0; i < 128; i++){
        //     inst().device_shuffle_map[i] = shuffle_list[i];
        // }
    }

    static int get_device_shuffle_map(int device_id){
        return inst().device_shuffle_map[device_id];
    }

    GContext(GContext const&) = delete;
    void operator=(GContext const&) = delete;

    std::map<int, int> core_selection; 
    std::map<int, double> flow_avg_transfer_rate;
    int cut_off_time = 0; 
    int next_cut_off_time = 1e9;
    int cut_off_decrease_step = 0; 

    std::vector<run_info> run_info_list;

    std::map<int, int> device_shuffle_map; 

    int flow_cutoff = 0; 
    int next_flow_cutoff = 0;
    int flow_cutoff_decrease_step = 0;

private:
    GContext() {}
};
    
} // namespace psim
#endif // CONFIG_H