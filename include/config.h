#ifndef CONFIG_H
#define CONFIG_H

class GConf {
public:
    static GConf& inst() {
        static GConf instance;
        return instance;
    }

    GConf(GConf const&) = delete;
    void operator=(GConf const&) = delete;

    double step_size = 0.01;
    double rate_increase = 1.1; 
    double initial_rate = 10;  
    double min_rate = 1; 
    double link_bandwidth = 100; 
    std::string protocol_file_dir = "../input";
    std::string protocol_file_name = "vgg.txt";
    bool plot_graphs = false; 
    bool export_dot = false;  
    bool record_bottleneck_history = false; 
    bool record_machine_history = false; 
    std::string output_dir = "out/"; 
    int console_log_level = 2; 
    int file_log_level = 2; 
    std::string network_type = "fattree"; // "fattree" or "bigswitch"
    int bn_priority_levels = 1; 
    std::string priority_allocator = "priorityqueue"; // "priorityqueue" or "fixedlevels" or "fairshare

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

private:
    GConf() {}
};

#endif // CONFIG_H