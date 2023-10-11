
#include <iostream>

#include "psim.h"
#include "protocol.h"
#include "network.h"
#include "options.h"
#include "protocol_builder.h"
#include <boost/program_options.hpp>
#include <boost/algorithm/string.hpp>
#include "spdlog/spdlog.h"
#include "context.h"

namespace po = boost::program_options;
using namespace psim;


// function declarations
void init(int argc, char** argv);


// main function
int main(int argc, char** argv) {
    init(argc, argv); 
    std::vector<double> psim_time_list;     
    
    GContext::initiate_device_shuffle_map();
    

    for (int rep = 1; rep <= GConf::inst().rep_count; rep ++) {
        std::string worker_dir = "workers/worker-" + std::to_string(GConf::inst().worker_id) + "/";
        change_log_path(worker_dir + "run-" + std::to_string(rep), "runtime.txt", true);
        GContext::start_new_run();

        PSim* psim = new PSim();

        std::vector<std::string> protocol_file_names;
        boost::split(protocol_file_names, GConf::inst().protocol_file_name, boost::is_any_of(","));

        for (auto protocol_file_name : protocol_file_names) {
            std::string path = GConf::inst().protocol_file_dir + "/" + protocol_file_name;
            Protocol* proto = load_protocol_from_file(path);
            proto->build_dependency_graph();
            if (GConf::inst().export_dot){
                proto->export_dot(protocol_file_name);
            }  
            psim->add_protocol(proto);
        }

        double psim_time = psim->simulate();
        psim_time_list.push_back(psim_time);
        delete psim;

        change_log_path(worker_dir + "run-" + std::to_string(rep), "results.txt", false);
        spdlog::critical("psim time: {}", psim_time);

        if (rep == 1){
            // auto& ctx = GContext::inst();
            // auto& this_run = GContext::this_run(); 
            // for (auto& entry: this_run.network_status) {
            //     spdlog::critical("time: {}", entry.second.time);
            //     for (auto& flow_load: entry.second.flow_loads) {
            //         spdlog::critical("flow: {}, load: {}", flow_load.first, flow_load.second);
            //     }
            // }
            // exit(0); 

            GContext::inst().cut_off_time = psim_time;
            GContext::inst().cut_off_decrease_step = psim_time / GConf::inst().rep_count;
        } else {
            GContext::inst().cut_off_time -= GContext::inst().cut_off_decrease_step; 
        }
        
        spdlog::critical("cut off time: {}.", GContext::inst().cut_off_time);
    }


    // int run_number = 0; 
    // for (auto psim_time : psim_time_list) {
    //     spdlog::critical("run {}: psim time: {}.", run_number, psim_time);
    //     run_number ++;
    // }
    return 0;
}



void init(int argc, char** argv){
    srand(time(NULL));

    po::variables_map vm = parse_arguments(argc, argv);
    process_arguments(vm);

    // output + worker id
    GConf::inst().output_dir = "workers/worker-" + std::to_string(GConf::inst().worker_id) + "/";

    setup_logger(true);
    log_config(); 
}