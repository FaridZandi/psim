#include <sstream>
#include <fstream>
#include "gcontext.h"
#include <cassert>
#include <iostream> 
#include "spdlog/spdlog.h"

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

bool GContext::is_first_run() {
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
    
    // we can either always choose the better of the last two runs to keep (a 
    // greedy approach), or we can just keep the last one. 
    bool keep_the_better_one = true; 

    // keep at most 2 items in the list 
    if (inst().run_info_list.size() > 2) {
        if (keep_the_better_one){
            // if 0 is better than 1, remove 1. otherwise remove 0.
            if (inst().run_info_list[0].psim_time < inst().run_info_list[1].psim_time) {
                inst().run_info_list.erase(inst().run_info_list.begin() + 1);
            } else {
                inst().run_info_list.erase(inst().run_info_list.begin());
            }
        } else {
            // remove the first one
            inst().run_info_list.erase(inst().run_info_list.begin());
        }
    }

    // making sure we have at most 2 items in the list
    assert(inst().run_info_list.size() <= 2);
}

void GContext::initiate_device_shuffle_map(){
    // read the shuffle map from the file.
    // there's a permutation of the numbers 0 to 127 in the file, separated by commas.

    std::string path = GConf::inst().shuffle_map_file;
    std::ifstream file(path);
    if(!file.good()){
        std::cout << "shuffle map file not found. exiting." << std::endl;
        std::cout << "path: " << path << std::endl;
        exit(0);
    }
    std::string line;
    std::getline(file, line);

    int i = 0;
    while(std::getline(file, line, '\n')){
        inst().device_shuffle_map[i] = std::stoi(line);
        i++;
    }
}

int GContext::get_device_shuffle_map(int device_id){
    return inst().device_shuffle_map[device_id];
}