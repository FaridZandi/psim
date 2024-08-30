from utils.util import *
from utils.sweep_base import ConfigSweeper
from utils.util import default_load_metric_map
import pandas as pd 
import numpy as np  
from processing.itertimes_multirep import get_all_rep_iter_lengths, get_all_rep_all_reduce_times
from pprint import pprint 
import copy
    
placement_mode_map = {1: "compact placement+optimal ring",
                      2: "random placement+optimal ring",
                      3: "compact placement+random ring",
                      4: "random placement+random ring"}


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
    
}

# total_capacity = 800
RANDOM_REP_COUNT = 5
experiment_seed = 54
oversub = 1

compared_lb_scheme = "" 
compared_timing_scheme = "" 

base_lb_scheme = "random"
base_timing_scheme = "none"

total_capacities = [800, 400]

interesting_metrics = ["avg_ar_time", "avg_iter_time"] #"iter_minus_ar_time"]

compared_lb_schemes = ["roundrobin", "roundrobin", "ecmp", "perfect", "powerof2", "ideal"]
lbs_involving_randomness = ["random", "ecmp", "powerof2"]

compared_timing_schemes = ["inc", "random"]

sweep_config = {
    "protocol-file-name": ["nethint-test"],

    # placement and workload parameters
    "placement-seed": list(range(1, 20)), # this is a dummy parameter. basically repeat the experiment 10 times
    
    "machine-count": [128],
    "ft-server-per-rack": [16],
    
    "general-param-1": [8],  # number of machines for each job, low 
    "general-param-3": [16], # number of machines for each job, high 
    "general-param-4": [20000], # comm_size, to be divided by the number of machines in a job
    "general-param-5": [1000], # comp size
    "general-param-6": [1], # layer count
    "general-param-7": [30], # iteration count
    
    "general-param-2": [ # placement mode
        1, # "compact placement+optimal ring",
        2, # "random placement+optimal ring",
        3, # "compact placement+random ring",
        4, # "random placement+random ring"                    
    ], 
} 

# the all-reduce time is technically comm_size / machine_count * 2 * (machine_count - 1) / link_bandwidth
# roughly equal to comm_size / link_bandwidth * 2
# comm_size = 20000, and link_bandwidth = 100 -> ar_time = 20000 / 100 * 2 = 400


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
        "ideal_network": False,
        "original_mult": options["ft-agg-core-link-capacity-mult"],
        "original_core_count": options["ft-core-count"],
    }        


    # if the lb scheme involves making random decisions, then we need to run multiple reps.    
    if options["lb-scheme"] in lbs_involving_randomness:
        # there's no point in running more than one rep for random if there's only one core. 
        if options["ft-core-count"] > 1:
            options["rep-count"] = RANDOM_REP_COUNT
    

    # perfect lb will create a network where the core layer does perfect load balancing.
    # probably something that could be achieved with a perfect packet spraying mechanism.
    # technically, it's just a random lb with with the combined capacity in one link. 

    if options["lb-scheme"] == "perfect":
        run_context["perfect_lb"] = True
        
        options["lb-scheme"] = "random"
        options["ft-agg-core-link-capacity-mult"] = (total_capacity / options["link-bandwidth"])
        options["ft-core-count"] = 1
    
    
    # ideal network will create a network where the core layer has infinite capacity.
    if options["lb-scheme"] == "ideal":
        run_context["ideal_network"] = True 
        
        options["lb-scheme"] = "random"
        options["ft-agg-core-link-capacity-mult"] = 1000
        options["ft-core-count"] = 1
    
    options["load-metric"] = default_load_metric_map[options["lb-scheme"]]
       
    changed_keys = ["ft-agg-core-link-capacity-mult", "ft-core-count", "rep-count", "load-metric"]
              
    return changed_keys, run_context 


def result_extractor_function(output, options, this_exp_results):
    
    for metric in interesting_metrics:
        
        if metric == "avg_ar_time":
            job_numbers = get_all_rep_all_reduce_times(output, options["rep-count"], all_jobs_running=True)
        
        elif metric == "avg_iter_time": 
            job_numbers = get_all_rep_iter_lengths(output, options["rep-count"], all_jobs_running=True)
            
        elif metric == "iter_minus_ar_time":
            ar_times = get_all_rep_all_reduce_times(output, options["rep-count"], all_jobs_running=True)
            iter_times = get_all_rep_iter_lengths(output, options["rep-count"], all_jobs_running=True)
            job_numbers = [] 
            
            for rep in range(options["rep-count"]):
                ar_time = ar_times[rep]
                iter_time = iter_times[rep]
                
                rep_numbers = {} 
                
                for job in ar_time.keys():
                    rep_job_iter_times = iter_time[job]
                    rep_job_ar_times = ar_time[job]
                    rep_numbers[job] = []
                    
                    for i in range(len(rep_job_iter_times)):
                        rep_job_iter_time = rep_job_iter_times[i]
                        rep_job_ar_time = rep_job_ar_times[i]
                        diff = rep_job_iter_time - rep_job_ar_time 
                        rep_numbers[job].append(diff)
                        
                job_numbers.append(rep_numbers)
        
        else: 
            print("Unknown metric: ", metric)
            sys.exit(1)
            
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
            "min_{}".format(metric): min(avg_job_numbers),
            "max_{}".format(metric): max(avg_job_numbers),
            "last_{}".format(metric): avg_job_numbers[-1],
            "avg_{}".format(metric): round(np.mean(avg_job_numbers), 2),
            "all_{}".format(metric): avg_job_numbers,  
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
    
    if run_context["ideal_network"]:
        results["lb-scheme"] = "ideal"
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
                 compact_plot_path, random_plot_path, script_path):
     
    keys_arg = ",".join(interesting_keys)
    
    for csv_path, plot_path in [(compact_csv_path, compact_plot_path), 
                                (random_csv_path, random_plot_path)]:
            
        plot_command = "python3 plot.py {} {} {} {} {} {}".format(csv_path, plot_path, keys_arg, 
                                                                 plotted_key_min, plotted_key_max, 
                                                                 title, random_seed)

        os.system(plot_command)
    
        print("To redraw the plot, use the following command: ")
        print(plot_command)
        
        with open(script_path, "a+") as f:
            f.write(plot_command)
            f.write("\n")
    
    
def plot_cdfs(separating_params, cdf_params, 
              compact_csv_path, random_csv_path, plots_dir, script_path):
    separating_params_str = ",".join(separating_params)
    cdf_params_str = ",".join(cdf_params)
        
    plot_command = "python3 plot_cdf.py {} {} {} {} {}".format(compact_csv_path,
                                                           random_csv_path,  
                                                           separating_params_str, 
                                                           cdf_params_str, plots_dir)

    os.system(plot_command)
    
    print("To redraw the plot, use the following command: ")
    print(plot_command)
    
    with open(script_path, "a+") as f:
        f.write(plot_command)
        f.write("\n")   
                
            
def custom_save_results_func(exp_results_df, config_sweeper, plot=False): 
    
    for metric in interesting_metrics:
        avg_metric_key = "avg_{}".format(metric)
        metric_csv_dir = config_sweeper.csv_dir + metric + "/" 
        metric_plots_dir = config_sweeper.plots_dir + metric + "/" 
        
        os.makedirs(metric_csv_dir, exist_ok=True)
        os.makedirs(metric_plots_dir, exist_ok=True)
        
        compact_placement_df = exp_results_df[exp_results_df["node-placement"] == "compact"]
        random_placement_df = exp_results_df[exp_results_df["node-placement"] == "random"]
        
        merge_on = ["protocol-file-name", "machine-count", "cores", "placement-seed", "job-info"]

        for placement, placement_df in [("compact", compact_placement_df), 
                                        ("random", random_placement_df)]:

            base = {"ring-mode": "random", "lb-scheme": base_lb_scheme, "timing-scheme": base_timing_scheme}
            
            comparisons = [
                ("OR", {"ring-mode": "optimal", "lb-scheme": compared_lb_scheme, "timing-scheme": base_timing_scheme}), 
                ("LB", {"ring-mode": "random", "lb-scheme": compared_lb_scheme, "timing-scheme": base_timing_scheme}),
                ("TS", {"ring-mode": "random", "lb-scheme": base_lb_scheme, "timing-scheme": compared_timing_scheme}),
                
                ("OR+LB", {"ring-mode": "optimal", "lb-scheme": compared_lb_scheme, "timing-scheme": base_timing_scheme}),
                ("OR+TS", {"ring-mode": "optimal", "lb-scheme": base_lb_scheme, "timing-scheme": compared_timing_scheme}),
                ("LB+TS", {"ring-mode": "random", "lb-scheme": compared_lb_scheme, "timing-scheme": compared_timing_scheme}),
                
                ("OR+LB+TS", {"ring-mode": "optimal", "lb-scheme": compared_lb_scheme, "timing-scheme": compared_timing_scheme}),
                    
                ("Ideal", {"ring-mode": "optimal", "lb-scheme": "ideal", "timing-scheme": base_timing_scheme}),    
                # ("Ideal+TS", {"ring-mode": "optimal", "lb-scheme": "ideal", "timing-scheme": compared_timing_scheme}),
            ]
            
            # base_df = placement_df[(placement_df["ring-mode"] == base["ring-mode"]) & 
            #                        (placement_df["lb-scheme"] == base["lb-scheme"]) &
            #                        (placement_df["timing-scheme"] == base["timing-scheme"])]
            
            base_df = placement_df 
            for key, value in base.items():
                base_df = base_df[base_df[key] == value]    
                                
            comparison_results = [] 
                                
            for comparison_name, compared_df_setting in comparisons:
                
                # compared_df = placement_df[(placement_df["ring-mode"] == compared_df_setting["ring-mode"]) &
                #                            (placement_df["lb-scheme"] == compared_df_setting["lb-scheme"]) &
                #                            (placement_df["timing-scheme"] == compared_df_setting["timing-scheme"])]
                
                compared_df = placement_df
                for key, value in compared_df_setting.items():
                    compared_df = compared_df[compared_df[key] == value]
                
                merged_df = pd.merge(base_df, compared_df, 
                                     on=merge_on, suffixes=('_base', '_compared'))
                
                base_avg_metric_key = "{}_base".format(avg_metric_key)
                compared_avg_metric_key = "{}_compared".format(avg_metric_key)
                
                merged_df["speedup"] = round(merged_df[base_avg_metric_key] / merged_df[compared_avg_metric_key], 2)
                
                saved_columns = merge_on + ["speedup"]

                csv_path = metric_csv_dir + f"speedup_{placement}_{comparison_name}.csv"
                speedup_df = merged_df[saved_columns]
                speedup_df.to_csv(csv_path, index=False)

                comparison_results.append((comparison_name, speedup_df))
            
            super_merged = comparison_results[0][1]
            
            column_name = "speedup_{}".format(comparison_results[0][0]) 
            super_merged.rename(columns={"speedup": column_name}, inplace=True)              

            for comparison_name, comparison_df in comparison_results[1:]:
                super_merged = super_merged.merge(comparison_df, on=merge_on)
                super_merged.rename(columns={"speedup": "speedup_{}".format(comparison_name)}, inplace=True)
            
            
            # reduce the dataframe on "placement-seed"
            group_on = merge_on.copy()
            group_on.remove("placement-seed")

            agg_dict = {}
            for comparison_name, _ in comparisons:
                agg_dict[f"speedup_{comparison_name}_min"] = (f"speedup_{comparison_name}", "min")
                agg_dict[f"speedup_{comparison_name}_max"] = (f"speedup_{comparison_name}", "max")
                agg_dict[f"speedup_{comparison_name}_values"] = (f"speedup_{comparison_name}", lambda x: sorted(list(x)))

            cdf_params = ["speedup_{}_values".format(comparison_name) for comparison_name, _ in comparisons] 
            
            grouped_df = super_merged.groupby(by=group_on).agg(**agg_dict)
            
            if placement == "compact":
                compact_grouped_df = grouped_df
            else:
                random_grouped_df = grouped_df
                

        compact_csv_path = metric_csv_dir + "compact_results.csv"
        random_csv_path  = metric_csv_dir + "random_results.csv"
        compact_plot_path = metric_plots_dir + "compact_plot.png"
        random_plot_path  = metric_plots_dir + "random_plot.png"
            
        compact_grouped_df.reset_index().to_csv(compact_csv_path, index=False)
        random_grouped_df.reset_index().to_csv(random_csv_path, index=False)
    
        if plot:
            title = "Speedup in {} of {} over {}, {} Gbps".format(
                metric, 
                compared_lb_scheme,
                base_lb_scheme,
                total_capacity
            )
            title = title.replace(" ", "$")
            
            # plot_results(interesting_keys=["machine-count" , "cores", "job-info"], 
            #              plotted_key_min="speedup_or_lb_min", 
            #              plotted_key_max="speedup_or_lb_max", 
            #              title=title, 
            #              random_seed=experiment_seed,
            #              compact_csv_path=compact_csv_path,
            #              random_csv_path=random_csv_path,
            #              compact_plot_path=compact_plot_path,
            #              random_plot_path=random_plot_path, 
            #              script_path=config_sweeper.plot_commands_script)

            plot_cdfs(separating_params=["machine-count", "cores"], 
                      cdf_params=cdf_params, 
                      compact_csv_path=compact_csv_path,
                      random_csv_path=random_csv_path, 
                      plots_dir=metric_plots_dir, 
                      script_path=config_sweeper.plot_commands_script)
    
        
if __name__ == "__main__":
    random.seed(experiment_seed)
    
    for total_capacity in total_capacities: 
        for lb_scheme in compared_lb_schemes: 
            for timing_scheme in compared_timing_schemes:
                compared_lb_scheme = lb_scheme 
                compared_timing_scheme = timing_scheme
            
                exp_sweep_config = copy.deepcopy(sweep_config)  
                
                exp_sweep_config["lb-scheme"] = [base_lb_scheme, compared_lb_scheme, "ideal"]
                exp_sweep_config["timing-scheme"] = [base_timing_scheme, compared_timing_scheme] 
                
                core_count = int(total_capacity / base_options["link-bandwidth"] / oversub)
                exp_sweep_config["ft-core-count"] = [core_count]
                    
                cs = ConfigSweeper(
                    base_options, exp_sweep_config, 
                    run_command_options_modifier, 
                    run_results_modifier, 
                    custom_save_results_func, 
                    result_extractor_function,
                    exp_name="nethint_{}_{}_{}_{}".format(compared_lb_scheme, 
                                                          compared_timing_scheme, 
                                                          total_capacity, 
                                                          experiment_seed),
                    worker_thread_count=40, 
                )
                
                cs.sweep()

