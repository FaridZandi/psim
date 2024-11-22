#include <iostream>
#include <vector>
#include <string>
#include <map>
#include <algorithm>
#include <nlohmann/json.hpp>

using json = nlohmann::json;



// For convenience
using json = nlohmann::json;

// Define data structures
struct JobLoad {
    int job_id;
    std::vector<double> load;
    int period;
    int iter_count;  
};

struct Delta {
    int job_id;
    int delta_value;
};

struct InputData {
    std::vector<JobLoad> job_loads;
    std::vector<Delta> deltas;
};

// Function to parse and store the data
InputData parseJsonInput(const std::string& json_input) {
    // Parse the JSON input
    json input_data_json = json::parse(json_input);

    // Create an instance of InputData
    InputData input_data;

    // Parse job_loads
    for (const auto& job : input_data_json["job_loads"]) {
        JobLoad job_load;

        job_load.job_id = job["job_id"];
        job_load.load = job["load"].get<std::vector<double>>();
        job_load.period = job["period"];
        job_load.iter_count = job["iter_count"];
        
        input_data.job_loads.push_back(job_load);
    }

    // Parse deltas
    for (const auto& delta : input_data_json["deltas"]) {
        Delta delta_entry;
        delta_entry.job_id = delta[0];
        delta_entry.delta_value = delta[1];

        input_data.deltas.push_back(delta_entry);
    }

    return input_data;
}


// Utility function to get the delta for a specific job_id from the deltas vector
int get_delta_for_job_in_decisions(std::vector<Delta>& deltas, int job_id) {
    for (const auto& delta : deltas) {
        if (delta.job_id == job_id) {
            return delta.delta_value;
        }
    }
    return 0; // Default value if not found
}

// Evaluate candidate function
double evaluate_candidate_farid(
    InputData& input_data,   
    int sim_length,
    int link_logical_bandwidth, 
    std::string compat_score_mode   
) {
    std::vector<double> sum_signal(sim_length, 0);

    for (const auto& job_load : input_data.job_loads) {
        int job_id = job_load.job_id; 
        int current_time = get_delta_for_job_in_decisions(input_data.deltas, job_id);
        int repeat_count = job_load.iter_count; 
        
        // while (current_time < sim_length) {
        //     for (int j = 0; j < job_load.load.size(); ++j) {
        //         sum_signal[current_time] += job_load.load[j];

        //         current_time++;
        //         if (current_time >= sim_length) {
        //             break;
        //         }
        //     }
        // }

        for (int i = 0; i < repeat_count; ++i) {
            for (int j = 0; j < job_load.load.size(); ++j) {
                sum_signal[current_time] += job_load.load[j];

                current_time++;
                if (current_time >= sim_length) {
                    break;
                }
            }
        }
    }

    // double max_util = *std::max_element(sum_signal.begin(), sum_signal.end());
    // double max_util_score = (link_logical_bandwidth - max_util) / static_cast<double>(link_logical_bandwidth);

    // // Calculate compat_score
    // double compat_score = 0;
    // for (int i = 0; i < sim_length; ++i) {
    //     if (sum_signal[i] <= link_logical_bandwidth) {
    //         compat_score++;
    //     }
    // }
    // compat_score /= sim_length;

    double compat_score = 0;

    if (compat_score_mode == "under-cap") {
        for (int i = 0; i < sim_length; ++i) {
            if (sum_signal[i] < link_logical_bandwidth) {
                compat_score += 1;
            }
        }
        compat_score /= sim_length;

    } else if (compat_score_mode == "time-no-coll") {
        for (int i = 0; i < sim_length; ++i) {
            if (sum_signal[i] <= link_logical_bandwidth) {
                compat_score += 1;
            } else {
                break;
            }
        }
        compat_score /= sim_length;

    } else if (compat_score_mode == "max-util-left") {
        double max_util = *std::max_element(sum_signal.begin(), sum_signal.end());
        compat_score = (link_logical_bandwidth - max_util) / link_logical_bandwidth;
    }

    // std::cout << "max_util_score: " << max_util_score << ", compat_score: " << compat_score << std::endl;
    return compat_score;
}

int main(int argc, char* argv[]) {  
    // getting the input arguments  
    if (argc != 4) {
        std::cerr << "Usage: " << argv[0] << " <sim_length> <link_logical_bandwidth> <compat_score_mode>" << std::endl;
        return 1;
    }   
    int sim_length = std::stoi(argv[1]);
    int link_logical_bandwidth = std::stoi(argv[2]);
    std::string compat_score_mode = argv[3]; 

    // Reading JSON input from stdin
    std::string json_input;
    std::getline(std::cin, json_input);

    // Parse the JSON input
    InputData input_data = parseJsonInput(json_input);
    double compat_score = evaluate_candidate_farid(input_data, sim_length, 
                                                   link_logical_bandwidth, 
                                                   compat_score_mode);
    std::cout << compat_score << std::endl;   
    
    return 0;
}