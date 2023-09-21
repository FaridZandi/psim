
#include <iostream>

#include "psim.h"
#include "protocol.h"
#include "network.h"
#include "options.h"
#include "protocol_builder.h"
#include <boost/program_options.hpp>
#include <boost/algorithm/string.hpp>
#include "spdlog/spdlog.h"

namespace po = boost::program_options;
using namespace psim;


// function declarations
void init(int argc, char** argv);


// main function
int main(int argc, char** argv) {
    init(argc, argv); 
    
    int rep_count = 1; 
    double psim_time_sum = 0;

    for (int rep = 0; rep < rep_count; rep ++) {
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
        delete psim;

        spdlog::critical("psim time: {}.", psim_time);
        psim_time_sum += psim_time;
    }

    spdlog::critical("average psim time: {}.", psim_time_sum / rep_count);
    
    return 0;
}



void init(int argc, char** argv){
    srand(time(NULL));

    po::variables_map vm = parse_arguments(argc, argv);
    process_arguments(vm);
    setup_logger(vm);
    log_config(); 
}