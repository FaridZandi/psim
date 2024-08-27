from utils.util import *
from utils.sweep_base import ConfigSweeper
from utils.util import default_load_metric_map
import pandas as pd 
import numpy as np  
from processing.itertimes_multirep import get_all_rep_iter_lengths, get_all_rep_all_reduce_times
from pprint import pprint 

    
placement_mode_map = {1: "compact placement+optimal ring",
                      2: "random placement+optimal ring",
                      3: "compact placement+random ring",
                      4: "random placement+random ring"}

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
    "ft-server-per-rack": 32,
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
    
    "general-param-1": 8,  # number of machines for each job, low 
    "general-param-3": 16, # number of machines for each job, high 
}

total_capacity = 400
RANDOM_REP_COUNT = 2
COMPARED_LB = "perfect"
experiment_seed = 45 

sweep_config = {
    "protocol-file-name": ["nethint-test"],

    # placement and workload parameters
    "placement-seed": list(range(1, 5)), # this is a dummy parameter. basically repeat the experiment 10 times
    "machine-count": [64],
    "general-param-4": [4000], # comm_size
    "general-param-5": [500], # comp size
    "general-param-6": [1], # layer_count
    "general-param-7": [1], # iterationcount
    
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
    job_numbers = get_all_rep_all_reduce_times(output, options["rep-count"])
    job_numbers = get_all_rep_iter_lengths(output, options["rep-count"])
    
    avg_job_numbers = [] 
    
    for rep in job_numbers:
        sum_job_numbers = 0 
        number_count = 0
          
        for job, numbers in rep.items():
            sum_job_numbers += sum(numbers) 
            number_count += len(numbers)    
            
        avg_job_number = round(sum_job_numbers / number_count, 2) 
        avg_job_numbers.append(avg_job_number)    
    
    this_exp_results.update({
        "min_avg_job_number": min(avg_job_numbers),
        "max_avg_job_number": max(avg_job_numbers),
        "last_avg_job_number": avg_job_numbers[-1],
        "avg_avg_job_number": round(np.mean(avg_job_numbers), 2),
        "all_job_numbers": avg_job_numbers,
    })
    
    

def run_results_modifier(results, options, output, run_context):
    # rename the field: general-param-2 -> placement-mode
    node_placement = -1 
    ring_mode = -1
     
    general_param_2 = results["general-param-2"] 
    
    if general_param_2 == 1:
        node_placement = "compact"
        ring_mode = "optimal"
    elif general_param_2 == 2:
        node_placement = "random"
        ring_mode = "optimal"
    elif general_param_2 == 3:
        node_placement = "compact"
        ring_mode = "random"
    elif general_param_2 == 4:
        node_placement = "random"
        ring_mode = "random"
        
    results["node-placement"] = node_placement
    results["ring-mode"] = ring_mode
    
    # results["placement-mode"] = results.pop("general-param-2")
    # results["placement-mode"] = placement_mode_map[results["placement-mode"]] 

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
    

def plot_results(interesting_keys, plotted_key_min, plotted_key_max, 
                 title, random_seed, compact_csv_path, random_csv_path, 
                 compact_plot_path, random_plot_path):
     
    keys_arg = ",".join(interesting_keys)
    
    for csv_path, plot_path in [(compact_csv_path, compact_plot_path), 
                                (random_csv_path, random_plot_path)]:
            
        plot_command = "python plot.py {} {} {} {} {} {}".format(csv_path, plot_path, keys_arg, 
                                                                 plotted_key_min, plotted_key_max, 
                                                                 title, random_seed)

        os.system(plot_command)
    
        print("To redraw the plot, use the following command: ")
        print(plot_command)
    
    
    
def plot_cdfs(separating_params, cdf_params, 
              compact_csv_path, random_csv_path, plots_dir):
    separating_params_str = ",".join(separating_params)
    cdf_params_str = ",".join(cdf_params)
        
    plot_command = "python plot_cdf.py {} {} {} {} {}".format(compact_csv_path,
                                                           random_csv_path,  
                                                           separating_params_str, 
                                                           cdf_params_str, plots_dir)

    os.system(plot_command)
    
    print("To redraw the plot, use the following command: ")
    print(plot_command)
                
            
def custom_save_results_funch(exp_results_df, config_sweeper): 
    compact_placement_df = exp_results_df[exp_results_df["node-placement"] == "compact"]
    random_placement_df = exp_results_df[exp_results_df["node-placement"] == "random"]
    
    merge_on = ["protocol-file-name", "machine-count", "cores", "placement-seed", "job-info"]

    for placement, placement_df in [("compact", compact_placement_df), ("random", random_placement_df)]:
        # base 
        random_ring_random_lb = placement_df[(placement_df["ring-mode"] == "random") & (placement_df["lb-scheme"] == "random")] 

        # compared
        random_ring_compared_lb = placement_df[(placement_df["ring-mode"] == "random") & (placement_df["lb-scheme"] == COMPARED_LB)]
        optimal_ring_random_lb = placement_df[(placement_df["ring-mode"] == "optimal") & (placement_df["lb-scheme"] == "random")]
        optimal_ring_compared_lb = placement_df[(placement_df["ring-mode"] == "optimal") & (placement_df["lb-scheme"] == COMPARED_LB)]
        
        comparisions = [
            ("OR", optimal_ring_random_lb), 
            ("LB", random_ring_compared_lb),
            ("OR + LB", optimal_ring_compared_lb),
        ]
                
        for comparision_name, compared_df in comparisions:
            merged_df = pd.merge(random_ring_random_lb, compared_df, on=merge_on, suffixes=('_random', '_compared'))
            merged_df["speedup"] = round(merged_df["avg_avg_job_number_random"] / merged_df["avg_avg_job_number_compared"], 2)
            
            saved_columns = merge_on + ["speedup"]
            
            if comparision_name == "OR":
                or_speedup_df = merged_df[saved_columns]
                csv_path = config_sweeper.csv_dir + f"speedup_{placement}_or.csv"
                or_speedup_df.to_csv(csv_path, index=False)
            
            elif comparision_name == "LB":
                lb_speedup_df = merged_df[saved_columns]
                csv_path = config_sweeper.csv_dir + f"speedup_{placement}_lb.csv" 
                lb_speedup_df.to_csv(csv_path, index=False)
                
            elif comparision_name == "OR + LB":
                or_lb_speedup_df = merged_df[saved_columns]
                csv_path = config_sweeper.csv_dir + f"speedup_{placement}_or_lb.csv"
                or_lb_speedup_df.to_csv(csv_path, index=False)
                    
        super_merged = pd.merge(or_speedup_df, lb_speedup_df, on=merge_on, suffixes=('_or', '_lb')).merge(or_lb_speedup_df, on=merge_on)
        super_merged.rename(columns={"speedup": "speedup_or_lb"}, inplace=True)
        
        
        # reduce the dataframe on "placement-seed"
        group_on = merge_on.copy()
        group_on.remove("placement-seed")

        grouped_df = super_merged.groupby(by=group_on).agg(
            speedup_or_min=("speedup_or", "min"),
            speedup_or_max=("speedup_or", "max"),  
            speedup_or_values=("speedup_or", lambda x: sorted(list(x))),
            
            speedup_lb_min=("speedup_lb", "min"),
            speedup_lb_max=("speedup_lb", "max"),  
            speedup_lb_values=("speedup_lb", lambda x: sorted(list(x))),
            
            speedup_or_lb_min=("speedup_or_lb", "min"),
            speedup_or_lb_max=("speedup_or_lb", "max"),  
            speedup_or_lb_values=("speedup_or_lb", lambda x: sorted(list(x)))
        )
        
        if placement == "compact":
            compact_grouped_df = grouped_df
        else:
            random_grouped_df = grouped_df
            
    # compact_grouped_df, random_grouped_df

    compact_csv_path = config_sweeper.csv_dir + "compact_results.csv"
    random_csv_path = config_sweeper.csv_dir + "random_results.csv"
    compact_plot_path = config_sweeper.plots_dir + "compact_plot.png"
    random_plot_path = config_sweeper.plots_dir + "random_plot.png"
        
    compact_grouped_df.reset_index().to_csv(compact_csv_path, index=False)
    random_grouped_df.reset_index().to_csv(random_csv_path, index=False)
            
    title = "Speedup of perfect over random, under {} Gbps total capacity".format(total_capacity)
    title = title.replace(" ", "$")
    
    plot_results(interesting_keys=["machine-count" , "cores", "placement-mode", "job-info"], 
                 plotted_key_min="speedup_or_lb_min", 
                 plotted_key_max="speedup_or_lb_max", 
                 title=title, 
                 random_seed=experiment_seed,
                 compact_csv_path=compact_csv_path,
                 random_csv_path=random_csv_path,
                 compact_plot_path=compact_plot_path,
                 random_plot_path=random_plot_path)

    plot_cdfs(separating_params=["machine-count", "cores", "job-info"], 
              cdf_params=["speedup_or_values", "speedup_lb_values", "speedup_or_lb_values"], 
              compact_csv_path=compact_csv_path,
              random_csv_path=random_csv_path, 
              plots_dir=config_sweeper.plots_dir)
    
    
    



    
    
        
if __name__ == "__main__":
    random.seed(experiment_seed)
    
    cs = ConfigSweeper(base_options, sweep_config, 
                       run_command_options_modifier, 
                       run_results_modifier, 
                       custom_save_results_funch, 
                       result_extractor_function,
                       worker_thread_count=40)
    
    cs.sweep()

