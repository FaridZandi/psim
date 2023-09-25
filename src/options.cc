#include "options.h"
#include "config.h"
#include <iostream> 

#include <boost/program_options.hpp>
#include <boost/algorithm/string.hpp>
#include "spdlog/spdlog.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"

namespace po = boost::program_options;

po::variables_map psim::parse_arguments(int argc, char** argv) {
    po::options_description desc("Allowed options");

    desc.add_options()
        ("help", "produce help message")
        ("step-size", po::value<double>(), "set step size constant")
        ("rate-increase", po::value<double>(), "set rate increase constant")
        ("initial-rate", po::value<double>(), "set initial rate constant")
        ("min-rate", po::value<int>(), "set min rate")
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
        ("network-type", po::value<std::string>(), "set network type")
        ("bn-priority-levels", po::value<int>(), "set bn priority levels")
        ("priority-allocator", po::value<std::string>(), "set priority allocator class")
        ("ft-server-per-rack", po::value<int>(), "set ft-server-per-rack")
        ("ft-rack-per-pod", po::value<int>(), "set ft-rack-per-pod")
        ("ft-agg-per-pod", po::value<int>(), "set ft-agg-per-pod")
        ("ft-pod-count", po::value<int>(), "set ft-pod-count")
        ("ft-core-count", po::value<int>(), "set ft-core-count")
        ("ft-server-tor-link-capacity-mult", po::value<double>(), "set ft-server-tor-link-capacity-mult")
        ("ft-tor-agg-link-capacity-mult", po::value<double>(), "set ft-tor-agg-link-capacity-mult")
        ("ft-agg-core-link-capacity-mult", po::value<double>(), "set ft-agg-core-link-capacity-mult")
    ;

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help")) {
        std::cout << desc << "\n";
        exit(0);
    }

    return vm;
}



void psim::setup_logger(po::variables_map vm) {
    // remove and create output directory
    std::string rm_command = "rm -rf " + GConf::inst().output_dir;
    std::string mkdir_command = "mkdir " + GConf::inst().output_dir;
    int ret = system(rm_command.c_str());
    ret = system(mkdir_command.c_str());

    // setup logger
    auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    console_sink->set_pattern("[%H:%M:%S.%e] [%^%l%$] %v");
    console_sink->set_level(spdlog::level::level_enum(GConf::inst().console_log_level));

    std::string log_path = GConf::inst().output_dir + "/log.log";
    auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(log_path, true);
    file_sink->set_pattern("[%H:%M:%S.%e] [%^%l%$] %v");
    file_sink->set_level(spdlog::level::level_enum(GConf::inst().file_log_level));

    auto multi_sink = spdlog::sinks_init_list({console_sink, file_sink}); 
    auto logger = std::make_shared<spdlog::logger>("logger", multi_sink);
    spdlog::set_default_logger(logger);
    int log_level = std::min(GConf::inst().console_log_level, GConf::inst().file_log_level);
    spdlog::set_level(spdlog::level::level_enum(log_level));
}



void psim::process_arguments(po::variables_map vm){
    if (vm.count("console-log-level")) {
        GConf::inst().console_log_level = vm["console-log-level"].as<int>();
    }
    if (vm.count("file-log-level")) {
        GConf::inst().file_log_level = vm["file-log-level"].as<int>();
    }
    if (vm.count("output-dir")) {
        GConf::inst().output_dir = vm["output-dir"].as<std::string>();
    }
    if (vm.count("min-rate")) {
        GConf::inst().min_rate = vm["min-rate"].as<int>();
    }
    if (vm.count("priority-allocator")){
        GConf::inst().priority_allocator = vm["priority-allocator"].as<std::string>();
    }
    if (vm.count("bn-priority-levels")){
        GConf::inst().bn_priority_levels = vm["bn-priority-levels"].as<int>();
    }
    if (vm.count("network-type")){
        GConf::inst().network_type = vm["network-type"].as<std::string>();
    }
    if (vm.count("ft-server-per-rack")) {
        GConf::inst().ft_server_per_rack = vm["ft-server-per-rack"].as<int>();
    }
    if (vm.count("ft-rack-per-pod")) {
        GConf::inst().ft_rack_per_pod = vm["ft-rack-per-pod"].as<int>();
    }
    if (vm.count("ft-agg-per-pod")) {
        GConf::inst().ft_agg_per_pod = vm["ft-agg-per-pod"].as<int>();
    }
    if (vm.count("ft-pod-count")) {
        GConf::inst().ft_pod_count = vm["ft-pod-count"].as<int>();
    }
    if (vm.count("ft-core-count")) {
        GConf::inst().ft_core_count = vm["ft-core-count"].as<int>();
    }
    if (vm.count("ft-server-tor-link-capacity-mult")) {
        GConf::inst().ft_server_tor_link_capacity_mult = vm["ft-server-tor-link-capacity-mult"].as<double>();
    }
    if (vm.count("ft-tor-agg-link-capacity-mult")) {
        GConf::inst().ft_tor_agg_link_capacity_mult = vm["ft-tor-agg-link-capacity-mult"].as<double>();
    }
    if (vm.count("ft-agg-core-link-capacity-mult")) {
        GConf::inst().ft_agg_core_link_capacity_mult = vm["ft-agg-core-link-capacity-mult"].as<double>();
    }
    if (vm.count("step-size")) {
        GConf::inst().step_size = vm["step-size"].as<double>();
    }
    if (vm.count("rate-increase")) {
        GConf::inst().rate_increase = vm["rate-increase"].as<double>();
    }
    if (vm.count("initial-rate")) {
        GConf::inst().initial_rate = vm["initial-rate"].as<double>();
    }
    if (vm.count("machine-count")) {
        GConf::inst().machine_count = vm["machine-count"].as<int>();
    }
    if (vm.count("link-bandwidth")) {
        GConf::inst().link_bandwidth = vm["link-bandwidth"].as<double>();
    }
    if (vm.count("protocol-file-name")) {
        GConf::inst().protocol_file_name = vm["protocol-file-name"].as<std::string>();
    }
    if (vm.count("protocol-file-dir")) {
        GConf::inst().protocol_file_dir = vm["protocol-file-dir"].as<std::string>();
    }
    if (vm.count("plot-graphs")) {
        GConf::inst().plot_graphs = true;
    }
    if (vm.count("export-dot")) {
        GConf::inst().export_dot = true;
    }
    if (vm.count("record-bottleneck-history")) {
        GConf::inst().record_bottleneck_history = true;
    }
    if (vm.count("record-machine-history")) {
        GConf::inst().record_machine_history = true;
    }
}

void psim::log_config() {
    //log all config in one step
    spdlog::info("---------------------------------------------");
    spdlog::info("------------  Run Configuration   -----------");
    spdlog::info("---------------------------------------------");
    spdlog::info("==== machine_count: {}", GConf::inst().machine_count);
    spdlog::info("==== step_size: {}", GConf::inst().step_size);
    spdlog::info("==== rate_increase: {}", GConf::inst().rate_increase);
    spdlog::info("==== initial_rate: {}", GConf::inst().initial_rate);
    spdlog::info("==== min_rate: {}", GConf::inst().min_rate);
    spdlog::info("==== link_bandwidth: {}", GConf::inst().link_bandwidth);
    spdlog::info("==== protocol_file_dir: {}", GConf::inst().protocol_file_dir);
    spdlog::info("==== protocol_file_name: {}", GConf::inst().protocol_file_name);
    spdlog::info("==== plot_graphs: {}", GConf::inst().plot_graphs);
    spdlog::info("==== export_dot: {}", GConf::inst().export_dot);
    spdlog::info("==== record_bottleneck_history: {}", GConf::inst().record_bottleneck_history);
    spdlog::info("==== record_machine_history: {}", GConf::inst().record_machine_history);
    spdlog::info("==== output_dir: {}", GConf::inst().output_dir);
    spdlog::info("==== console_log_level: {}", GConf::inst().console_log_level);
    spdlog::info("==== file_log_level: {}", GConf::inst().file_log_level);
    spdlog::info("==== network_type: {}", GConf::inst().network_type);
    spdlog::info("==== bn_priority_levels: {}", GConf::inst().bn_priority_levels);
    spdlog::info("==== priority_allocator: {}", GConf::inst().priority_allocator);
    spdlog::info("==== ft_server_per_rack: {}", GConf::inst().ft_server_per_rack);
    spdlog::info("==== ft_rack_per_pod: {}", GConf::inst().ft_rack_per_pod);
    spdlog::info("==== ft_agg_per_pod: {}", GConf::inst().ft_agg_per_pod);
    spdlog::info("==== ft_pod_count: {}", GConf::inst().ft_pod_count);
    spdlog::info("==== ft_core_count: {}", GConf::inst().ft_core_count);
    spdlog::info("==== ft_server_tor_link_capacity_mult: {}", GConf::inst().ft_server_tor_link_capacity_mult);
    spdlog::info("==== ft_tor_agg_link_capacity_mult: {}", GConf::inst().ft_tor_agg_link_capacity_mult);
    spdlog::info("==== ft_agg_core_link_capacity_mult: {}", GConf::inst().ft_agg_core_link_capacity_mult);
    spdlog::info("---------------------------------------------");
    spdlog::info("---------------------------------------------");
}