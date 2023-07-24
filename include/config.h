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
    std::string protocol_file_path;
    std::string protocol_file_name;
    bool plot_graphs;
    bool export_dot; 

private:
    GConf() {
        verbose = false;
        step_size = 0.01;
        rate_increase = 1.1;
        initial_rate = 10;
        machine_count = 16;
        link_bandwidth = 100;
        protocol_file_path = "../input/";
        protocol_file_name = "vgg.txt";
        plot_graphs = false;
        export_dot = false;
    }
};

#endif // CONFIG_H


