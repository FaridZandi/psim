#include "psim.h"
#include "protocol.h"
#include <iostream>


using namespace psim;


#include <boost/program_options.hpp>
namespace po = boost::program_options;
void parse_arguments_boost(int argc, char** argv);

int main(int argc, char** argv) {
    srand(time(NULL));
    int ret = system("mkdir -p out");
    parse_arguments_boost(argc, argv);
    
    std::string path = GConf::inst().protocol_file_path + "/" + GConf::inst().protocol_file_name;

    // Protocol* proto = Protocol::build_random_protocol(1600, 16);
    Protocol* base_proto = Protocol::load_protocol_from_file(path);
    // Protocol* base_proto = Protocol::super_simple_protocol();
    // Protocol* base_proto = Protocol::super_simple_protocol();

    base_proto->build_dependency_graph();

    // Protocol* proto = Protocol::pipelinize_protocol(base_proto, 2, true);
    // Protocol* proto = base_proto->make_copy(true);
    Protocol* proto = base_proto; 

    // std::string path = "logs/protocol_log.txt";
    // std::ofstream simulation_log;
    // simulation_log.open(path);
    // proto->export_graph(simulation_log);
    // simulation_log.close();
    // proto->export_dot("protocol");

    PSim* psim = new PSim(proto);
    double psim_time = psim->simulate();
    std::cout << "havij time:" << psim_time << std::endl;

    return 0;
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
        ("protocol-file-path", po::value<std::string>(), "set protocol file path")
        ("should-plot-graphs", po::value<int>()->implicit_value(1), "enable plotting graphs")
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
    if (vm.count("protocol-file-path")) {
        GConf::inst().protocol_file_path = vm["protocol-file-path"].as<std::string>();
        if (GConf::inst().verbose) {    
            std::cout << "protocol-file-path set to " << GConf::inst().protocol_file_path << ".\n";
        }
    }
    if (vm.count("should-plot-graphs")) {
        GConf::inst().should_plot_graphs = true;
        if (GConf::inst().verbose) {
            std::cout << "should-plot-graphs set to " << GConf::inst().should_plot_graphs << ".\n";
        }
    }
}