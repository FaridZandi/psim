#ifndef CONFIG_H
#define CONFIG_H

enum class LoadMetric {
    DEFAULT,
    REGISTERED,
    UTILIZATION,
    ALLOCATED,
    FLOWSIZE,
    FLOWCOUNT,
};

enum class LBScheme{
    ECMP, 
    ZERO, 
    RANDOM,
    ROUND_ROBIN,
    READ_FILE,
    LEAST_LOADED,
    POWER_OF_K,
    ROBIN_HOOD,
    FUTURE_LOAD,
    SITA_E,
};


enum class PriorityAllocator {
    PRIORITY_QUEUE,
    FIXED_LEVELS,
    FAIR_SHARE,
    MAX_MIN_FAIR_SHARE,
};

enum class NetworkType {
    FAT_TREE,
    BIG_SWITCH,
    LEAF_SPINE,
};

enum class RegretMode {
    CRITICAL,
    ALL, 
    NONE,
};


class GConf {
public:
    static GConf& inst() {
        static GConf instance;
        return instance;
    }

    GConf(GConf const&) = delete;
    void operator=(GConf const&) = delete;

    int worker_id = 0;
    std::string workers_dir = "workers/";
    double step_size = 0.01;
    double rate_increase = 1.1;
    double initial_rate = 10;
    double min_rate = 1;
    double link_bandwidth = 100;
    double rate_decrease_factor = 0.5; 
    double drop_chance_multiplier = 1.0;
    std::string protocol_file_dir = "../input";
    std::string protocol_file_name = "vgg.txt";
    bool plot_graphs = false;
    bool export_dot = false;
    bool record_bottleneck_history = false;
    bool record_machine_history = false;
    std::string output_dir = "output/";
    int console_log_level = 2;
    int file_log_level = 2;
    int bn_priority_levels = 1;
    int rep_count = 2;
    bool shuffle_device_map = false;
    std::string shuffle_map_file = "";
    std::string lb_decisions_file = ""; 

    PriorityAllocator priority_allocator = PriorityAllocator::FAIR_SHARE;
    NetworkType network_type = NetworkType::FAT_TREE;
    LBScheme lb_scheme = LBScheme::ROUND_ROBIN;
    int lb_samples = 2;
    LoadMetric load_metric = LoadMetric::UTILIZATION;

    RegretMode regret_mode = RegretMode::NONE;

    bool profile_core_status = true;

    int core_status_profiling_interval = 10;
    std::string log_file_name = "log.txt";

    // int machine_count = 16;
    // int ft_server_per_rack = 4;
    // int ft_rack_per_pod = 2;
    // int ft_agg_per_pod = 2;
    // int ft_pod_count = 2;
    // int ft_core_count = 2;
    // int ft_server_tor_link_capacity_mult = 2;
    // int ft_tor_agg_link_capacity_mult = 2;
    // int ft_agg_core_link_capacity_mult = 4;

    int machine_count = 128;
    int ft_server_per_rack = 8;
    int ft_rack_per_pod = 4;
    int ft_agg_per_pod = 4;
    int ft_pod_count = 4;
    int ft_core_count = 4;
    double ft_server_tor_link_capacity_mult = 1;
    double ft_tor_agg_link_capacity_mult = 2;
    double ft_agg_core_link_capacity_mult = 8;


    bool record_link_flow_loads = false;

    // experimental shit
    bool adaptive_step_size = false; 
    double adaptive_step_size_min = 0.1; 
    double adaptive_step_size_max = 1; 

    // I wanna set these so that I could use them for experimental purposes, such that I don't have to change 
    // the name and go through the process every time something comes up. 
    int general_param_1 = 0; 
    int general_param_2 = 0;
    int general_param_3 = 0;
    int general_param_4 = 0;
    int general_param_5 = 0;
    int general_param_6 = 0;
    int general_param_7 = 0;
    int general_param_8 = 0;
    int general_param_9 = 0;
    int general_param_10 = 0;

    GConf() {}
};

#endif // CONFIG_H
