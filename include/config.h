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
    double step_size_constant;
    double rate_increase_constant;
    double initial_rate_constant; 
    int machine_count; 
    double link_bandwidth;
    std::string protocol_file_path;
    std::string protocol_file_name;

private:
    GConf() {
        verbose = false;
        step_size_constant = 1;
        rate_increase_constant = 1.1;
        initial_rate_constant = 1000;
        machine_count = 128;
        link_bandwidth = 1000;
        protocol_file_path = "../input/";
        protocol_file_name = "simple.txt";
    }
};

#endif // CONFIG_H


