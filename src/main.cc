
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
        std::string worker_dir = "worker-" + std::to_string(GConf::inst().worker_id) + "/";
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

        change_log_path(worker_dir + "run-" + std::to_string(rep), "results.txt", false);
        spdlog::critical("psim time: {}", psim_time);

        if (rep == 1){
            int time_decrease_step = psim_time / GConf::inst().rep_count;

            GContext::inst().cut_off_decrease_step = time_decrease_step;
            GContext::inst().cut_off_time = psim_time - time_decrease_step;
            GContext::inst().next_cut_off_time = psim_time - 2 * time_decrease_step;

            // largest flow number in the protocol 
            int largest_flow_number = 0; 
            for (auto flow : psim->finished_flows){
                if (flow->id > largest_flow_number){
                    largest_flow_number = flow->id;
                }
            }
            
            int flow_decrease_step = largest_flow_number / GConf::inst().rep_count;
            GContext::inst().flow_cutoff_decrease_step = flow_decrease_step;
            GContext::inst().flow_cutoff = largest_flow_number - flow_decrease_step;
            GContext::inst().next_flow_cutoff = largest_flow_number - 2 * flow_decrease_step;
        } else {
            int time_decrease_step = GContext::inst().cut_off_decrease_step; 
            GContext::inst().cut_off_time -= time_decrease_step; 
            GContext::inst().next_cut_off_time -= time_decrease_step;

            int flow_decrease_step = GContext::inst().flow_cutoff_decrease_step;
            GContext::inst().flow_cutoff -= flow_decrease_step;
            GContext::inst().next_flow_cutoff -= flow_decrease_step; 
        }

        // spdlog::critical("cut off time: {}.", GContext::inst().cut_off_time);
        // spdlog::critical("next cut off time: {}.", GContext::inst().next_cut_off_time);

        spdlog::critical("flow cut off: {}.", GContext::inst().flow_cutoff);
        spdlog::critical("next flow cut off: {}.", GContext::inst().next_flow_cutoff);


        delete psim;
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
    GConf::inst().output_dir = "worker-" + std::to_string(GConf::inst().worker_id) + "/";

    setup_logger(true);
    log_config(); 
}