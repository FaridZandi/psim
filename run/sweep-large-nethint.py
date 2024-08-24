from utils.util import *
from utils.sweep_base import ConfigSweeper
from utils.util import default_load_metric_map
import pandas as pd 
import numpy as np  
from processing.itertimes_multirep import get_all_rep_iter_lengths
from pprint import pprint 

RANDOM_REP_COUNT = 5 
COMPARED_LB = "perfect"
    
placement_mode_map = {1: "compact placement+optimal ring",
                      2: "random placement+optimal ring",
                      3: "compact placement+random ring",
                      4: "random placement+random ring"}

total_capacity = 400

experiment_seed = 45 

PERFECT_LB_MAGIC = 1234 

base_options = {
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
    
    "general-param-1": 4,  # number of machines for each job, low 
    "general-param-3": 8, # number of machines for each job, high 
}

sweep_config = {
    "protocol-file-name": ["nethint-test"],

    # placement and workload parameters
    "placement-seed": list(range(1, 3)), # this is a dummy parameter. basically repeat the experiment 10 times
    "machine-count": [32],
    "general-param-4": [4000, 8000, 16000], # comm_size
    "general-param-5": [200, 400, 800], # comp size
    "general-param-6": [1, 2, 4, 8], # layer_count
    
    "general-param-2": [ # placement mode
        1, # "compact placement+optimal ring",
        2, # "random placement+optimal ring",
        3, # "compact placement+random ring",
        4, # "random placement+random ring"                    
    ], 

    # load balancing parameters
    "ft-core-count": [2, 4],
    "lb-scheme": ["random", COMPARED_LB],
} 


def run_command_options_modifier(options, config_sweeper):
    options["simulation-seed"] = experiment_seed 
    
    # regardless of the other stuff that we do, this is what we want: 
    per_link_capacity = total_capacity / options["ft-core-count"]
    options["ft-agg-core-link-capacity-mult"] = per_link_capacity / options["link-bandwidth"]
     
    # I want to have a fixed amount of capacity to the core layer. 
    # If there are more cores, then the capacity per link should be divided.
    # e.g. 1 * 800, 2 * 400, 4 * 200, 8 * 100
    
    run_context = {
        "perfect_lb": False,
        "original_mult": options["ft-agg-core-link-capacity-mult"],
        "original_core_count": options["ft-core-count"],
    }        

    if options["lb-scheme"] == "random":
        # there's no point in running more than one rep for random if there's only one core. 
        if options["ft-core-count"] > 1:
            options["rep-count"] = RANDOM_REP_COUNT
    
    if options["lb-scheme"] == "leastloaded":
        pass 
    
    
    if options["lb-scheme"] == "perfect":
        run_context["perfect_lb"] = True
        
        # perfect lb is actually a random lb with with the combined capacity in one link. 
        options["lb-scheme"] = "random"
        options["ft-agg-core-link-capacity-mult"] = (total_capacity / options["link-bandwidth"])
        options["ft-core-count"] = 1
        options["rep-count"] = 1
    
    
    options["load-metric"] = default_load_metric_map[options["lb-scheme"]]
       
    changed_keys = ["ft-agg-core-link-capacity-mult", "ft-core-count", "rep-count", "load-metric"]
              
    return changed_keys, run_context 


def result_extractor_function(output, options, this_exp_results):
    
    iter_lengths = get_all_rep_iter_lengths(output, options["rep-count"])
    avg_iter_lengths = [] 
    
    for rep in iter_lengths:
        sum_iter_lengths = 0 
        iter_count = 0  
        for job, iters in rep.items():
            sum_iter_lengths += sum(iters) 
            iter_count += len(iters)    
            
        avg_iter_length = round(sum_iter_lengths / iter_count, 2) 
        avg_iter_lengths.append(avg_iter_length)    
    
    this_exp_results.update({
        "min_avg_iter_length": min(avg_iter_lengths),
        "max_avg_iter_length": max(avg_iter_lengths),
        "last_avg_iter_length": avg_iter_lengths[-1],
        "avg_avg_iter_length": round(np.mean(avg_iter_lengths), 2),
        "all_iter_lengths": avg_iter_lengths,
    })
    
    

def run_results_modifier(results, options, output, run_context):
    # rename the field: general-param-2 -> placement-mode
    results["placement-mode"] = results.pop("general-param-2")
    results["placement-mode"] = placement_mode_map[results["placement-mode"]] 

    results["job-info"] = "{} comm + {} comp + {} layers".format(results["general-param-4"], 
                                                                 results["general-param-5"], 
                                                                 results["general-param-6"])
    
    # this is a perfect lb scheme, so we need to change the lb-scheme to "perfect"
    # and change othe other fields back to the original values.
    if run_context["perfect_lb"]:
        results["lb-scheme"] = "perfect"
        results["ft-agg-core-link-capacity-mult"] = run_context["original_mult"]
        results["ft-core-count"] = run_context["original_core_count"]
    
    results["cores"] = "{} x {} Gbps".format(results["ft-core-count"], 
                                           int(options["link-bandwidth"] * results["ft-agg-core-link-capacity-mult"]))
    
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
    merge_on = ["protocol-file-name", "machine-count", "cores", "placement-mode", "placement-seed", "job-info"]
    merged_df = pd.merge(random_df, compared_lb_df, on=merge_on, suffixes=('_random', '_compared'))
    merged_df["speedup"] = round(merged_df["avg_avg_iter_length_random"] / merged_df["avg_avg_iter_length_compared"], 2)  
    
    saved_columns = merge_on + ["speedup"] + ["avg_avg_iter_length_random", "avg_avg_iter_length_compared"]
    merged_df[saved_columns].to_csv(config_sweeper.merged_csv_path)
    
    # reduce the dataframe on "placement-seed"
    group_on = merge_on.copy()
    group_on.remove("placement-seed")

    grouped_df = merged_df.groupby(by=group_on).agg(
        speedup_min=("speedup", "min"),
        speedup_max=("speedup", "max"),
        speedup_values=("speedup", lambda x: sorted(list(x))),
    )
    return grouped_df
            
if __name__ == "__main__":
    random.seed(experiment_seed)
    
    cs = ConfigSweeper(base_options, sweep_config, 
                       run_command_options_modifier, 
                       run_results_modifier, 
                       global_results_modifier, 
                       result_extractor_function,
                       worker_thread_count=30)
    
    results_dir, csv_path, exp_results = cs.sweep()

    cs.plot_results(interesting_keys=["machine-count" , "cores", "placement-mode", "job-info"], 
                    plotted_key_min="speedup_min", 
                    plotted_key_max="speedup_max", 
                    title="Speedup of perfect over random, under {} Gbps total capacity".format(total_capacity))

    # cs.plot_cdfs(csv_path, 
    #              separating_params=["machine-count", "ft-core-count"], 
    #              same_plot_param="placement-mode", 
    #              cdf_params=["speedup_values"])