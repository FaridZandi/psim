#include "psim.h"
#include "protocol.h"
#include <iostream>
#include <boost/program_options.hpp>
#include "spdlog/spdlog.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"

namespace po = boost::program_options;
using namespace psim;


void parse_arguments_boost(int argc, char** argv);

void init();

int main(int argc, char** argv) {

    parse_arguments_boost(argc, argv);

    init(); 

    spdlog::info("Starting simulation.");
    spdlog::debug("Starting simulation.");
    spdlog::critical("Starting simulation.");

    
    std::string path = GConf::inst().protocol_file_dir + "/" + GConf::inst().protocol_file_name;
    Protocol* proto = Protocol::load_protocol_from_file(path);
    proto->build_dependency_graph();
    if (GConf::inst().export_dot){
        proto->export_dot("protocol");
    }  

    PSim* psim = new PSim();
    psim->add_protocol(proto);

    double psim_time = psim->simulate();
    spdlog::info("psim time: {}.", psim_time);

    return 0;
}

void init(){
    srand(time(NULL));

    std::string rm_command = "rm -rf " + GConf::inst().output_dir;
    std::string mkdir_command = "mkdir " + GConf::inst().output_dir;

    int ret = system(rm_command.c_str());
    ret = system(mkdir_command.c_str());


    auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    console_sink->set_pattern("[%H:%M:%S:%e] [%^%l%$] %v");
    console_sink->set_level(spdlog::level::level_enum(GConf::inst().console_log_level));

    std::string log_path = GConf::inst().output_dir + "/log.txt";
    auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(log_path, true);
    file_sink->set_pattern("[%H:%M:%S:%e] [%^%l%$] %v");
    file_sink->set_level(spdlog::level::level_enum(GConf::inst().file_log_level));

    spdlog::set_default_logger(std::make_shared<spdlog::logger>("multi_sink", spdlog::sinks_init_list({console_sink, file_sink})));
    spdlog::set_level(spdlog::level::trace);
}


void parse_arguments_boost(int argc, char** argv){
    // https://www.boost.org/doc/libs/1_75_0/doc/html/program_options/tutorial.html

    po::options_description desc("Allowed options");
    desc.add_options()
        ("help", "produce help message")
        ("step-size", po::value<double>(), "set step size constant")
        ("rate-increase", po::value<double>(), "set rate increase constant")
        ("initial-rate", po::value<double>(), "set initial rate constant")
        ("machine-count", po::value<int>(), "set machine count")
        ("link-bandwidth", po::value<double>(), "set link bandwidth")
        ("protocol-file-name", po::value<std::string>(), "set protocol file name")
        ("protocol-file-dir", po::value<std::string>(), "set protocol file path")
        ("plot-graphs", po::value<int>()->implicit_value(1), "enable plotting graphs")
        ("export-dot", po::value<int>()->implicit_value(1), "enable exporting dot")
        ("record-bottleneck-history", po::value<int>()->implicit_value(1), "record bn history")
        ("record-machine-history", po::value<int>()->implicit_value(1), "record machine history")
        ("output-dir", po::value<std::string>(), "set output directory")
        ("console-log-level", po::value<int>(), "set console log level")
        ("file-log-level", po::value<int>(), "set file log level")
    ;

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    // update config with command line arguments
    if (vm.count("help")) {
        std::cout << desc << "\n";
        exit(0);
    }
    if (vm.count("step-size")) {
        GConf::inst().step_size = vm["step-size"].as<double>();
        spdlog::info("step-size set to {}.", GConf::inst().step_size);
    }
    if (vm.count("rate-increase")) {
        GConf::inst().rate_increase = vm["rate-increase"].as<double>();
        spdlog::info("rate-increase set to {}.", GConf::inst().rate_increase);
    }
    if (vm.count("initial-rate")) {
        GConf::inst().initial_rate = vm["initial-rate"].as<double>();
        spdlog::info("initial-rate set to {}.", GConf::inst().initial_rate);
    }
    if (vm.count("machine-count")) {
        GConf::inst().machine_count = vm["machine-count"].as<int>();
        spdlog::info("machine-count set to {}.", GConf::inst().machine_count);
    }
    if (vm.count("link-bandwidth")) {
        GConf::inst().link_bandwidth = vm["link-bandwidth"].as<double>();
        spdlog::info("link-bandwidth set to {}.", GConf::inst().link_bandwidth);
    }
    if (vm.count("protocol-file-name")) {
        GConf::inst().protocol_file_name = vm["protocol-file-name"].as<std::string>();
        spdlog::info("protocol-file-name set to {}.", GConf::inst().protocol_file_name);
    }
    if (vm.count("protocol-file-dir")) {
        GConf::inst().protocol_file_dir = vm["protocol-file-dir"].as<std::string>();
        spdlog::info("protocol-file-dir set to {}.", GConf::inst().protocol_file_dir);
    }
    if (vm.count("plot-graphs")) {
        GConf::inst().plot_graphs = true;
        spdlog::info("plot-graphs set to {}.", GConf::inst().plot_graphs);
    }
    if (vm.count("export-dot")) {
        GConf::inst().export_dot = true;
        spdlog::info("export-dot set to {}.", GConf::inst().export_dot);
    }
    if (vm.count("record-bottleneck-history")) {
        GConf::inst().record_bottleneck_history = true;
        spdlog::info("record-bottleneck-history set to {}.", GConf::inst().record_bottleneck_history);
    }
    if (vm.count("record-machine-history")) {
        GConf::inst().record_machine_history = true;
        spdlog::info("record-machine-history set to {}.", GConf::inst().record_machine_history);
    }
    if (vm.count("output-dir")) {
        GConf::inst().output_dir = vm["output-dir"].as<std::string>();
        spdlog::info("output-dir set to {}.", GConf::inst().output_dir);
    }
    if (vm.count("console-log-level")) {
        GConf::inst().console_log_level = vm["console-log-level"].as<int>();
        spdlog::info("console-log-level set to {}.", GConf::inst().console_log_level);
    }
    if (vm.count("file-log-level")) {
        GConf::inst().file_log_level = vm["file-log-level"].as<int>();
        spdlog::info("file-log-level set to {}.", GConf::inst().file_log_level);
    }
}