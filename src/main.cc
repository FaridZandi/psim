
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
    int rep_count = GConf::inst().rep_count; 

    for (int rep = 0; rep < rep_count; rep ++) {

        GConf::inst().output_dir = "output/run-" + std::to_string(rep);
        setup_logger();
        log_config();

        
        auto& ctx = GContext::inst();
        ctx.run_info_list.push_back(run_info());

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

        spdlog::critical("psim time: {}.", psim_time);


        // iterate over the run_info_list
        int run_counter = 0; 
        for (auto const& run_info : GContext::inst().run_info_list) {
            // spdlog::info("run_info_list[{}]:", run_counter);

            // iterate over the core_link_status_map
            for (auto const& [key, val] : run_info.core_link_status_map) {
                // spdlog::info("core_link_status_map[{}]:", key);

                // // iterate over the core_link_registered_rate_map
                // for (auto const& [key2, val2] : val.core_link_registered_rate_map_up) {
                //     spdlog::info("core_link_registered_rate_map_up[{},{}]: {}.", key2.first, key2.second, val2);
                // }

                // for (auto const& [key2, val2] : val.core_link_registered_rate_map_down) {
                //     spdlog::info("core_link_registered_rate_map_down[{},{}]: {}.", key2.first, key2.second, val2);
                // }

                // std::string output = "";
                // for (int i = 0; i < 11; i ++) {
                //     output += std::to_string(val.effective_flow_rate_buckets[i]) + ", ";
                // }
                // spdlog::info("effective_flow_rate_buckets: {}.", output);

                // output = "";
                // for (int i = 0; i < 11; i++) {
                //     output += std::to_string(val.current_flow_rate_buckets[i]) + ", ";
                // }
                // spdlog::info("  current_flow_rate_buckets: {} sum: {}.", output, val.current_flow_rate_sum);


                // output = "";
                // for (int i = 0; i < 11; i++) {
                //     output += std::to_string(val.last_flow_rate_buckets[i]) + ", ";
                // }
                // spdlog::info("     last_flow_rate_buckets: {} sum: {}.", output, val.last_flow_rate_sum);
            }

            run_counter += 1; 
        }

        // iterate over the core_selection int to int map
        // for (auto const& [key, val] : GContext::inst().core_selection) {
        //     spdlog::critical("core_selection[{}]: {}.", key, val);
        // }

        // write the numbers in a file named run-x.txt
        // std::ofstream myfile;
        // myfile.open("run-" + std::to_string(rep) + ".txt");
        // for (auto const& [key, val] : GContext::inst().flow_avg_transfer_rate) {
        //     myfile << val << "\n";
        // }
        // myfile.close();    
    }

    int run_number = 0; 
    for (auto psim_time : psim_time_list) {
        spdlog::critical("run {}: psim time: {}.", run_number, psim_time);
        run_number ++;
    }
    return 0;
}



void init(int argc, char** argv){
    srand(time(NULL));

    po::variables_map vm = parse_arguments(argc, argv);
    process_arguments(vm);

    setup_logger();
    log_config(); 
}