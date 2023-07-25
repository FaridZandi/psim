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

    bool verbose;
    double step_size;
    double rate_increase;
    double initial_rate; 
    int machine_count; 
    double link_bandwidth;
    std::string protocol_file_dir;
    std::string protocol_file_name;
    bool plot_graphs;
    bool export_dot; 
    bool record_bottleneck_history;
    bool record_machine_history;
    std::string output_dir;

private:
    GConf() {
        verbose = false;
        step_size = 0.01;
        rate_increase = 1.1;
        initial_rate = 10;
        machine_count = 16;
        link_bandwidth = 100;
        protocol_file_dir = "../input/";
        protocol_file_name = "vgg.txt";
        plot_graphs = false;
        export_dot = false;
        record_bottleneck_history = false;
        record_machine_history = false;
        output_dir = "out/";
    }
};

#endif // CONFIG_H


