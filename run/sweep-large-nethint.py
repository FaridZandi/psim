from utils.util import *
from utils.sweep_base import ConfigSweeper
from utils.util import default_load_metric_map
import pandas as pd 
import numpy as np  

RANDOM_REP_COUNT = 5 
COMPARED_LB = "perfect"
    
placement_mode_map = {1: "compact placement+optimal ring",
                      2: "random placement+optimal ring",
                      3: "compact placement+random ring",
                      4: "random placement+random ring"}

total_capacity = 800

experiment_seed = 45 

options = {
    "step-size": 1,
    "core-status-profiling-interval": 100000,
    "rep-count": 1, 
    "console-log-level": 4,
    "file-log-level": 3,
    
    "initial-rate": 100,
    "min-rate": 100,
    "drop-chance-multiplier": 0, 
    "rate-increase": 1, 
    
    # "priority-allocator": "fairshare",
    "priority-allocator": "maxmin",
    # "priority-allocator": "priorityqueue", 

    "network-type": "leafspine",    
    "link-bandwidth": 100,
    "ft-server-per-rack": 8,
    "ft-rack-per-pod": 1,
    "ft-agg-per-pod": 1,
    # "ft-core-count": 4,
    "ft-pod-count": -1,
    "ft-server-tor-link-capacity-mult": 1,
    "ft-tor-agg-link-capacity-mult": 1,
    "ft-agg-core-link-capacity-mult": 1,
    
    # "lb-scheme": "random",
    # "lb-scheme": "roundrobin",
    # "lb-scheme": "roundrobin",
    
    "shuffle-device-map": False,
    "regret-mode": "none",
    
    "general-param-1": 4, # number of machines for each job, low 
    "general-param-3": 8, # number of machines for each job, high 
}

sweep_config = {
    "protocol-file-name": ["nethint-test"],
    "machine-count": [32, 64, 128],
    "ft-core-count": [1, 2, 4, 8],
    "general-param-2": [ # placement mode
        1, # "compact placement+optimal ring",
        2, # "random placement+optimal ring",
        3, # "compact placement+random ring",
        4, # "random placement+random ring"                    
    ], 
    "placement-seed": list(range(1, 11)), # this is a dummy parameter. basically repeat the experiment 10 times
    "lb-scheme": ["random", COMPARED_LB],
} 


# I want to have a shuffle map file for each experiment, that is shared between the different 
# lb schemes. So the different experiments that only differ in the lb scheme will use the same shuffle map file.
shuffle_map_cache = {}


def run_command_options_modifier(options, config_sweeper):
    options["simulation-seed"] = experiment_seed
     
    options["general-param-9"] = 0 
    
    if options["lb-scheme"] == "random":
        options["rep-count"] = RANDOM_REP_COUNT
        if options["ft-core-count"] == 1:
            options["rep-count"] = 1
            
        options["ft-agg-core-link-capacity-mult"] = (total_capacity / options["link-bandwidth"]) / options["ft-core-count"] 
        
        options["general-param-10"] = 0 
    
    if options["lb-scheme"] == "perfect":
        options["lb-scheme"] = "random"
        options["general-param-9"] = options["ft-core-count"]
        options["ft-agg-core-link-capacity-mult"] = (total_capacity / options["link-bandwidth"])
        options["ft-core-count"] = 1
        options["rep-count"] = 1
        options["general-param-10"] = 1234 
    
    options["load-metric"] = default_load_metric_map[options["lb-scheme"]]
    
    # I want to have a fixed amount of capacity to the core layer. 
    # If there are more cores, then the capacity per link should be divided.
    # e.g. 1 * 800, 2 * 400, 4 * 200, 8 * 100
    # max_core_count = max(sweep_config["ft-core-count"])
    # options["ft-agg-core-link-capacity-mult"] = max_core_count / options["ft-core-count"]
                 
    return ["load-metric", "ft-agg-core-link-capacity-mult", "simulation-seed", "general-param-10", "general-param-9"]

def run_results_modifier(results, output):
    # rename the field: general-param-2 -> placement-mode
    results["placement-mode"] = results.pop("general-param-2")
    results["placement-mode"] = placement_mode_map[results["placement-mode"]] 
    
    
    if results["general-param-10"] == 1234:
        # this is a perfect lb scheme, so we need to change the lb-scheme to "perfect"
        results["lb-scheme"] = "perfect"
        results["ft-core-count"] = results["general-param-9"]
        
        
    # go through the output, print all the lines that have "PLACEMENT" in them
    # for line in output.split("\n"):
    #     if "PLACEMENT" in line:
    #         print(line)
            
    
    
# compute the speedup of the "roundrobin" over the "random" lb scheme
def global_results_modifier(exp_results_df, config_sweeper): 
    # filter the results for the two lb schemes
    random_df = exp_results_df[exp_results_df["lb-scheme"] == "random"]
    compared_lb_df = exp_results_df[exp_results_df["lb-scheme"] == COMPARED_LB]
    
    # merge the two dataframes
    merge_on = ["protocol-file-name", "machine-count", "ft-core-count", "placement-mode", "placement-seed"]
    merged_df = pd.merge(random_df, compared_lb_df, on=merge_on, suffixes=('_random', '_compared'))
    merged_df["speedup"] = merged_df["avg_psim_time_random"] / merged_df["avg_psim_time_compared"] 
    
    saved_columns = merge_on + ["speedup"] + ["avg_psim_time_random", "avg_psim_time_compared"]
    merged_df[saved_columns].to_csv(config_sweeper.merged_csv_path)
    
    # reduce the dataframe on "placement-seed"
    group_on = merge_on.copy()
    group_on.remove("placement-seed")
    grouped_df = merged_df.groupby(by=group_on).agg({"speedup": ["min", "max", lambda x: sorted(list(x))]})
    grouped_df.columns = ['_'.join(col).strip() for col in grouped_df.columns.values]

    # rename the values column to speedup_values
    grouped_df = grouped_df.rename(columns={"speedup_<lambda_0>": "speedup_values"})
    
    return grouped_df
            
    
random.seed(experiment_seed)

cs = ConfigSweeper(options, sweep_config, run_command_options_modifier, run_results_modifier, global_results_modifier) 
results_dir, csv_path, exp_results = cs.sweep()


cs.plot_results(interesting_keys=["machine-count", "ft-core-count", "placement-mode"], 
                plotted_key_min="speedup_min", 
                plotted_key_max="speedup_max")

cs.plot_cdfs(csv_path, 
             separating_params=["machine-count", "ft-core-count"], 
             same_plot_param="placement-mode", 
             cdf_params=["speedup_values"])