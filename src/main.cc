
#include <iostream>

#include "psim.h"
#include "protocol.h"
#include "network.h"
#include "options.h"
#include "protocol_builder.h"
#include <boost/program_options.hpp>
#include "spdlog/spdlog.h"
#include "gcontext.h"
#include <iomanip>

namespace po = boost::program_options;
using namespace psim;


// function declarations
void init(int argc, char** argv);

void log_core_status_history(int rep, PSim* psim); 

// main function
int main(int argc, char** argv) {
    init(argc, argv);
    std::vector<double> psim_time_list;

    GContext::initiate_device_shuffle_map();

    // Protocol* p = build_random_protocol(2000, 128);
    // std::ofstream ofs("a.out");
    // p->export_graph(ofs);
    // ofs.close();
    // exit(0);

    for (int rep = 1; rep <= GConf::inst().rep_count; rep ++) {
        std::string worker_dir = "workers/worker-" + std::to_string(GConf::inst().worker_id) + "/";
        change_log_path(worker_dir + "run-" + std::to_string(rep), "runtime.txt", true);
        GContext::start_new_run();

        PSim* psim = new PSim();


        psim->add_protocols_from_input();
        psim->inform_network_of_protocols();

        double psim_time = psim->simulate();
        GContext::this_run().psim_time = psim_time;
        psim_time_list.push_back(psim_time);

        change_log_path(worker_dir + "run-" + std::to_string(rep), "results.txt");
        psim->log_results(); 
        psim->measure_regret(); 


        if (rep == 1){
            GContext::inst().cut_off_time = psim_time;
            GContext::inst().cut_off_decrease_step = psim_time / GConf::inst().rep_count;
        } else {
            GContext::inst().cut_off_time -= GContext::inst().cut_off_decrease_step;
        }

        log_core_status_history(rep, psim);

        delete psim;
    }

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


void log_core_status_history(int rep, PSim* psim){

    std::string worker_dir = "workers/worker-" + std::to_string(GConf::inst().worker_id) + "/";


    if (GConf::inst().record_link_flow_loads and 
        GConf::inst().lb_scheme == LBScheme::FUTURE_LOAD or 
        GConf::inst().lb_scheme == LBScheme::FUTURE_LOAD_2) {

        std::ofstream ofs(worker_dir + "run-" + std::to_string(rep) + "/link_flow_loads.txt");
        for (auto& entry: GContext::this_run().network_status) {
            int time = entry.first;
            auto& status = entry.second;
            
            ofs << "time: " << time << " " << status.time << std::endl;
            std::vector<int> ids;

            auto link_up_map = psim->network->core_load_balancer->link_up_map;
            auto link_down_map = psim->network->core_load_balancer->link_down_map; 
            // std::map<std::pair<int, int>, Bottleneck*> link_up_map;
            int lower_level_items = 0; 
            int upper_level_items = 0; 

            for(auto& entry: link_up_map) {
                auto& link = entry.first;
                auto& bottleneck = entry.second;
                lower_level_items = std::max(lower_level_items, link.first + 1);
                upper_level_items = std::max(upper_level_items, link.second + 1);
            }

            for (int i = 0; i < lower_level_items; i++) {
                for (int j = 0; j < upper_level_items; j++) {
                    auto up_link_id = link_up_map[std::make_pair(i, j)]->id;
                    ofs << "up   link " << i << " to " << j; 
                    ofs << " with load " << status.link_loads[up_link_id] << ": ";
                    for (auto& entry: status.link_flow_loads[up_link_id]) {
                        ofs << std::setprecision(3) << entry << " ";
                    }
                    ofs << std::endl;
                }
            }


            for (int i = 0; i < lower_level_items; i++) {
                for (int j = 0; j < upper_level_items; j++) {
                    int down_link_id = link_down_map[std::make_pair(i, j)]->id;
                    ofs << "down link " << i << " to " << j; 
                    ofs << " with load " << status.link_loads[down_link_id] << ": ";
                    for (auto& entry: status.link_flow_loads[down_link_id]) {
                        ofs << std::setprecision(3) << entry << " ";
                    }
                    ofs << std::endl;
                }
            }

            // for (auto id: ids) {
            //     if (status.link_flow_loads[id].size() == 0) {
            //         continue; 
            //     }
            //     ofs << "core: " << id << " with load " << status.link_loads[id] << ": ";
            //     for (auto& entry: status.link_flow_loads[id]) {
            //         ofs << entry << " ";
            //     }
            //     ofs << std::endl;
            // }
            ofs << std::endl;
            ofs << "-------------------------------------------------"; 
            ofs << std::endl;
        }    
        ofs.close();
    }
}

