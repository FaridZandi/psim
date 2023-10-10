#include <sstream>
#include <fstream>
#include "context.h"
#include <cassert>

using namespace psim;

GContext::GContext() {

}

GContext& GContext::inst() {
    static GContext instance;
    return instance;
}

run_info& GContext::this_run() {
    return inst().run_info_list.back();
}

run_info& GContext::last_run() {
    return inst().run_info_list[inst().run_info_list.size() - 2];
}

bool GContext::first_run() {
    return inst().run_info_list.size() == 1;
}   

void GContext::save_decision(int flow_id, int decision) {
    this_run().core_decision[flow_id] = decision;
}

const int GContext::last_decision(int flow_id) {
    return last_run().core_decision[flow_id];
}

void GContext::start_new_run() {
    inst().run_counter ++;
    inst().run_info_list.push_back(run_info());
    inst().run_info_list.back().run_number = inst().run_counter;

    // keep at most 2 items in the list 
    if (inst().run_info_list.size() > 2) {
        // remove the first item
        inst().run_info_list.erase(inst().run_info_list.begin());
    }

    assert(inst().run_info_list.size() <= 2);
}

void GContext::initiate_device_shuffle_map(){
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

int GContext::get_device_shuffle_map(int device_id){
    return inst().device_shuffle_map[device_id];
}