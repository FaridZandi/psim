#include "options.h"
#include "gconfig.h"
#include <iostream>
#include <string>
#include <sys/stat.h>

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
        ("worker-id", po::value<int>(), "set worker id")
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
        ("rep-count", po::value<int>(), "set rep-count")
        ("lb-scheme", po::value<std::string>(), "set core selection mechanism")
        ("shuffle-device-map", po::value<int>()->implicit_value(1), "shuffle device map")
        ("shuffle-map-file", po::value<std::string>(), "shuffle map file")
        ("load-metric", po::value<std::string>(), "load metric")
        ("core-status-profiling-interval", po::value<int>(), "core status profiling interval")
        ("log-file-name", po::value<std::string>(), "log file name")
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

void psim::change_log_path(std::string output_dir, 
                           std::string log_file_name, 
                           bool recreate_dir) {
                            
    GConf::inst().output_dir = output_dir;
    GConf::inst().log_file_name = log_file_name;

    psim::setup_logger(recreate_dir);
    psim::log_config();
}

void psim::setup_logger(bool recreate_dir) {
    // remove and create output directory

    struct stat buffer;
    bool dir_exists = (stat (GConf::inst().output_dir.c_str(), &buffer) == 0);

    if (recreate_dir or not dir_exists) {
        std::string rm_command = "rm -rf " + GConf::inst().output_dir;
        int ret = system(rm_command.c_str());

        std::string mkdir_command = "mkdir -p " + GConf::inst().output_dir;
        ret = system(mkdir_command.c_str());
    }

    // setup logger
    auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    console_sink->set_pattern("[%H:%M:%S.%e] [%^%l%$] %v");
    console_sink->set_level(spdlog::level::level_enum(GConf::inst().console_log_level));

    std::string log_path = GConf::inst().output_dir + "/" + GConf::inst().log_file_name;
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
    if (vm.count("load-metric")) {
        std::string load_metric_str = vm["load-metric"].as<std::string>();

        if (load_metric_str == "flowsize") {
            GConf::inst().load_metric = LoadMetric::FLOWSIZE;
        } else if (load_metric_str == "flowcount") {
            GConf::inst().load_metric = LoadMetric::FLOWCOUNT;
        } else if (load_metric_str == "utilization") {
            GConf::inst().load_metric = LoadMetric::UTILIZATION;
        } else if (load_metric_str == "allocated") {
            GConf::inst().load_metric = LoadMetric::ALLOCATED;
        } else if (load_metric_str == "registered") {
            GConf::inst().load_metric = LoadMetric::REGISTERED;
        } else {
            spdlog::error("Invalid load metric: {}", load_metric_str);
            exit(1);
        }
    }
    if (vm.count("core-status-profiling-interval")) {
        GConf::inst().core_status_profiling_interval = vm["core-status-profiling-interval"].as<int>();
    }
    if (vm.count("log-file-name")) {
        GConf::inst().log_file_name = vm["log-file-name"].as<std::string>();
    }
    if (vm.count("worker-id")) {
        GConf::inst().worker_id = vm["worker-id"].as<int>();
    }
    if (vm.count("shuffle-device-map")) {
        GConf::inst().shuffle_device_map = true;
    }
    if (vm.count("shuffle-map-file")) {
        GConf::inst().shuffle_map_file = vm["shuffle-map-file"].as<std::string>();
    }
    if (vm.count("lb-scheme")) {
        std::string lb_scheme_str = vm["lb-scheme"].as<std::string>();

        if (lb_scheme_str == "random") {
            GConf::inst().lb_scheme = LBScheme::RANDOM;
        } else if (lb_scheme_str == "roundrobin") {
            GConf::inst().lb_scheme = LBScheme::ROUND_ROBIN;
        } else if (lb_scheme_str == "leastloaded") {
            GConf::inst().lb_scheme = LBScheme::LEAST_LOADED;
        } else if (lb_scheme_str.substr(0, 7) == "powerof") {
            GConf::inst().lb_scheme = LBScheme::POWER_OF_K;
            GConf::inst().lb_samples = std::stoi(lb_scheme_str.substr(7));
        } else if (lb_scheme_str == "futureload") {
            GConf::inst().lb_scheme = LBScheme::FUTURE_LOAD;
        } else if (lb_scheme_str == "futureload2") {
            GConf::inst().lb_scheme = LBScheme::FUTURE_LOAD_2;
        } else if (lb_scheme_str == "robinhood") {
            GConf::inst().lb_scheme = LBScheme::ROBIN_HOOD;
        } else if (lb_scheme_str == "sita-e") {
            GConf::inst().lb_scheme = LBScheme::SITA_E;
        } else {
            spdlog::error("Invalid lb scheme: {}", lb_scheme_str);
            exit(1);
        }
    }
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
        std::string priority_allocator_str = vm["priority-allocator"].as<std::string>();

        if (priority_allocator_str == "priorityqueue") {
            GConf::inst().priority_allocator = PriorityAllocator::PRIORITY_QUEUE;
        } else if (priority_allocator_str == "fixedlevels") {
            GConf::inst().priority_allocator = PriorityAllocator::FIXED_LEVELS;
        } else if (priority_allocator_str == "fairshare") {
            GConf::inst().priority_allocator = PriorityAllocator::FAIR_SHARE;
        } else {
            spdlog::error("Invalid priority allocator: {}", priority_allocator_str);
            exit(1);
        }
    }
    if (vm.count("bn-priority-levels")){
        GConf::inst().bn_priority_levels = vm["bn-priority-levels"].as<int>();
    }
    if (vm.count("network-type")){

        std::string network_type_str = vm["network-type"].as<std::string>();

        if (network_type_str == "fattree") {
            GConf::inst().network_type = NetworkType::FAT_TREE;
        } else if (network_type_str == "bigswitch") {
            GConf::inst().network_type = NetworkType::BIG_SWITCH;
        } else if (network_type_str == "leafspine") {
            GConf::inst().network_type = NetworkType::LEAF_SPINE;
        } else {
            spdlog::error("Invalid network type: {}", network_type_str);
            exit(1);
        }
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
    if (vm.count("rep-count")) {
        GConf::inst().rep_count = vm["rep-count"].as<int>();
    }
}

void psim::log_config() {
    //log all config in one step
    spdlog::info("---------------------------------------------");
    spdlog::info("------------  Run Configuration   -----------");
    spdlog::info("---------------------------------------------");
    spdlog::info("==== worker_id: {}", GConf::inst().worker_id);
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
    spdlog::info("==== network_type: {}", int(GConf::inst().network_type));
    spdlog::info("==== bn_priority_levels: {}", GConf::inst().bn_priority_levels);
    spdlog::info("==== priority_allocator: {}", int(GConf::inst().priority_allocator));
    spdlog::info("==== ft_server_per_rack: {}", GConf::inst().ft_server_per_rack);
    spdlog::info("==== ft_rack_per_pod: {}", GConf::inst().ft_rack_per_pod);
    spdlog::info("==== ft_agg_per_pod: {}", GConf::inst().ft_agg_per_pod);
    spdlog::info("==== ft_pod_count: {}", GConf::inst().ft_pod_count);
    spdlog::info("==== ft_core_count: {}", GConf::inst().ft_core_count);
    spdlog::info("==== ft_server_tor_link_capacity_mult: {}", GConf::inst().ft_server_tor_link_capacity_mult);
    spdlog::info("==== ft_tor_agg_link_capacity_mult: {}", GConf::inst().ft_tor_agg_link_capacity_mult);
    spdlog::info("==== ft_agg_core_link_capacity_mult: {}", GConf::inst().ft_agg_core_link_capacity_mult);
    spdlog::info("==== rep_count: {}", GConf::inst().rep_count);
    spdlog::info("==== lb_scheme: {}", int(GConf::inst().lb_scheme));
    spdlog::info("==== shuffle_device_map: {}", GConf::inst().shuffle_device_map);
    spdlog::info("==== shuffle_map_file: {}", GConf::inst().shuffle_map_file);
    spdlog::info("==== load_metric: {}", int(GConf::inst().load_metric));
    spdlog::info("==== core_status_profiling_interval: {}", GConf::inst().core_status_profiling_interval);
    spdlog::info("==== log_file_name: {}", GConf::inst().log_file_name);
    spdlog::info("---------------------------------------------");
    spdlog::info("---------------------------------------------");
}
