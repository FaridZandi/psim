#include "psim.h"
#include "protocol.h"
#include <iostream>
#include <boost/program_options.hpp>
#include "spdlog/spdlog.h"

namespace po = boost::program_options;
using namespace psim;


void parse_arguments_boost(int argc, char** argv);

void init();

int main(int argc, char** argv) {

    parse_arguments_boost(argc, argv);

    init(); 
    
    
    std::string path = GConf::inst().protocol_file_dir + "/" + GConf::inst().protocol_file_name;
    Protocol* proto = Protocol::load_protocol_from_file(path);
    Protocol* proto2 = Protocol::load_protocol_from_file(path);
    proto->build_dependency_graph();
    proto2->build_dependency_graph();

    if (GConf::inst().export_dot){
        proto->export_dot("protocol");
    }  

    spdlog::info("Welcome to spdlog!");



    if (GConf::inst().verbose) std::cout << "Running protocol" << std::endl;
    PSim* psim = new PSim();
    psim->add_protocol(proto);
    psim->add_protocol(proto2);

    double psim_time = psim->simulate();
    std::cout << "havij time: " << psim_time << std::endl;

    return 0;
}

void init(){
    srand(time(NULL));

    std::string rm_command = "rm -rf " + GConf::inst().output_dir;
    std::string mkdir_command = "mkdir " + GConf::inst().output_dir;

    int ret = system(rm_command.c_str());
    ret = system(mkdir_command.c_str());

    std::string log_path = GConf::inst().output_dir + "/log.txt";
}


void parse_arguments_boost(int argc, char** argv){
    // https://www.boost.org/doc/libs/1_75_0/doc/html/program_options/tutorial.html

    po::options_description desc("Allowed options");
    desc.add_options()
        ("help", "produce help message")
        ("verbose", po::value<int>()->implicit_value(1), "enable verbosity")
        ("step-size", po::value<double>(), "set step size constant")
        ("rate-increase", po::value<double>(), "set rate increase constant")
        ("initial-rate", po::value<double>(), "set initial rate constant")
        ("machine-count", po::value<int>(), "set machine count")
        ("link-bandwidth", po::value<double>(), "set link bandwidth")
        ("protocol-file-name", po::value<std::string>(), "set protocol file name")
        ("protocol-file-dir", po::value<std::string>(), "set protocol file path")
        ("plot-graphs", po::value<int>()->implicit_value(1), "enable plotting graphs")
        ("export-dot", po::value<int>()->implicit_value(1), "enable exporting dot")
        ("record-bottleneck-history", po::value<int>()->implicit_value(1), "enable recording bottleneck history")
        ("record-machine-history", po::value<int>()->implicit_value(1), "enable recording machine history")
        ("output-dir", po::value<std::string>(), "set output directory")
    ;

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    // update config with command line arguments
    if (vm.count("help")) {
        std::cout << desc << "\n";
        exit(0);
    }
    if (vm.count("verbose")) {
        GConf::inst().verbose = true;
        if (GConf::inst().verbose) {
            std::cout << "verbose set to " << GConf::inst().verbose << ".\n";
        }
    }
    if (vm.count("step-size")) {
        GConf::inst().step_size = vm["step-size"].as<double>();
        if (GConf::inst().verbose) {    
            std::cout << "step-size set to " << GConf::inst().step_size << ".\n";
        }
    }
    if (vm.count("rate-increase")) {
        GConf::inst().rate_increase = vm["rate-increase"].as<double>();
        if (GConf::inst().verbose) {    
            std::cout << "rate-increase set to " << GConf::inst().rate_increase << ".\n";
        }
    }
    if (vm.count("initial-rate")) {
        GConf::inst().initial_rate = vm["initial-rate"].as<double>();
        if (GConf::inst().verbose) {    
            std::cout << "initial-rate set to " << GConf::inst().initial_rate << ".\n";
        }
    }
    if (vm.count("machine-count")) {
        GConf::inst().machine_count = vm["machine-count"].as<int>();
        if (GConf::inst().verbose) {    
            std::cout << "machine-count set to " << GConf::inst().machine_count << ".\n";
        }
    }
    if (vm.count("link-bandwidth")) {
        GConf::inst().link_bandwidth = vm["link-bandwidth"].as<double>();
        if (GConf::inst().verbose) {    
            std::cout << "link-bandwidth set to " << GConf::inst().link_bandwidth << ".\n";
        }
    }
    if (vm.count("protocol-file-name")) {
        GConf::inst().protocol_file_name = vm["protocol-file-name"].as<std::string>();
        if (GConf::inst().verbose) {    
            std::cout << "protocol-file-name set to " << GConf::inst().protocol_file_name << ".\n";
        }
    }
    if (vm.count("protocol-file-dir")) {
        GConf::inst().protocol_file_dir = vm["protocol-file-dir"].as<std::string>();
        if (GConf::inst().verbose) {    
            std::cout << "protocol-file-dir set to " << GConf::inst().protocol_file_dir << ".\n";
        }
    }
    if (vm.count("plot-graphs")) {
        GConf::inst().plot_graphs = true;
        if (GConf::inst().verbose) {
            std::cout << "plot-graphs set to " << GConf::inst().plot_graphs << ".\n";
        }
    }
    if (vm.count("export-dot")) {
        GConf::inst().export_dot = true;
        if (GConf::inst().verbose) {
            std::cout << "export-dot set to " << GConf::inst().export_dot << ".\n";
        }
    }
    if (vm.count("record-bottleneck-history")) {
        GConf::inst().record_bottleneck_history = true;
        if (GConf::inst().verbose) {
            std::cout << "record-bottleneck-history set to " << GConf::inst().record_bottleneck_history << ".\n";
        }
    }
    if (vm.count("record-machine-history")) {
        GConf::inst().record_machine_history = true;
        if (GConf::inst().verbose) {
            std::cout << "record-machine-history set to " << GConf::inst().record_machine_history << ".\n";
        }
    }
    if (vm.count("output-dir")) {
        GConf::inst().output_dir = vm["output-dir"].as<std::string>();
        if (GConf::inst().verbose) {    
            std::cout << "output-dir set to " << GConf::inst().output_dir << ".\n";
        }
    }
}