from utils.util import *
from utils.sweep_base import ConfigSweeper
from utils.util import default_load_metric_map
import pandas as pd 
import numpy as np  
from processing.itertimes_multirep import get_all_rep_iter_lengths, get_all_rep_all_reduce_times
from pprint import pprint 
import copy
import json
from algo.timing import generate_timing_file
from algo.placement import generate_placement_file
import time

#########################
# TODO: 
# 1. Add support for multiple placement modes. Dynamically handle any placement mode that is given,
#    in the placement file generation. 

# 2. Add another placement mode, something between the random and the compact. something like assigning
#    chunks of servers in the same rack to a job. Then repeat this for all the jobs and the racks. 

# 3. Improve the current version of the timing. Get some improvements from the other version.
#########################

lbs_involving_randomness = ["random", "ecmp", "powerof2"]
random_rep_count = 1
experiment_seed = 58
placement_files_state = {} 
timing_files_states = {} 

settings = [
    { # big settings
        "machine-count": 256,
        "ft-server-per-rack": 16,
        "machine-count-low": 8,
        "machine-count-high": 16,
        "placement-seed-range": 10,
        "comm-size": [20000, 40000],
        "comp-size": [1000, 2000],
        "layer-count": [2, 3],
        "iter-count": [30], # iteration count
    }, 
    { # small settings
        "machine-count": 64,
        "ft-server-per-rack": 8,
        "machine-count-low": 4,
        "machine-count-high": 8,
        "placement-seed-range": 10,
        "comm-size": [20000, 10000],
        "comp-size": [500, 1000],
        "layer-count": [2, 3],
        "iter-count": [10], # iteration count
    },
    { # tiny settings
        "machine-count": 16,
        "ft-server-per-rack": 8,
        "machine-count-low": 4,
        "machine-count-high": 8,
        "placement-seed-range": 10,
        "comm-size": [20000],
        "comp-size": [1000],
        "layer-count": [1],
        "iter-count": [20], # iteration count
    },
    { # tiny settings
        "machine-count": 256,
        "ft-server-per-rack": 16,
        "machine-count-low": 8,
        "machine-count-high": 16,
        "placement-seed-range": 10,
        "comm-size": [20000],
        "comp-size": [1000],
        "layer-count": [1],
        "iter-count": [20], # iteration count
    }
]

selected_setting = settings[0]
    

# the all-reduce time is technically comm_size / machine_count * 2 * (machine_count - 1) / link_bandwidth
# roughly equal to comm_size / link_bandwidth * 2
# comm_size = 20000, and link_bandwidth = 100 -> ar_time = 20000 / 100 * 2 = 400

   
def run_command_options_modifier(options, config_sweeper, run_context):
    
    # I want to have a fixed amount of capacity to the core layer. 
    # If there are more cores, then the capacity per link should be divided.
    # e.g. 1 * 800, 2 * 400, 4 * 200, 8 * 100
    run_context.update({
        "perfect_lb": False,
        "ideal_network": False,
        "original_mult": options["ft-agg-core-link-capacity-mult"],
        "original_core_count": options["ft-core-count"],

        "original_ring_mode": options["ring-mode"],
        "original_timing_scheme": options["timing-scheme"],
    })

    # if the lb scheme involves making random decisions, then we need to run multiple reps.    
    if options["lb-scheme"] in lbs_involving_randomness:
        # there's no point in running more than one rep for random if there's only one core. 
        if options["ft-core-count"] > 1:
            options["rep-count"] = random_rep_count

    # perfect lb will create a network where the core layer does perfect load balancing.
    # probably something that could be achieved with a perfect packet spraying mechanism.
    # technically, it's just a random lb with with the combined capacity in one link. 
    if options["lb-scheme"] == "perfect":
        run_context["perfect_lb"] = True
        
        options["lb-scheme"] = "random"
        options["ft-agg-core-link-capacity-mult"] = run_context["original_core_count"]
        options["ft-core-count"] = 1
    
    
    # ideal network will create a network where the core layer has infinite capacity.
    if options["lb-scheme"] == "ideal":
        run_context["ideal_network"] = True 

        options["lb-scheme"] = "random"
        options["ft-agg-core-link-capacity-mult"] = 1000
        options["ft-core-count"] = 1
        options["ring-mode"] = run_context["base-ring-mode"]
        options["timing-scheme"] = run_context["base-timing-scheme"]
    
    options["load-metric"] = default_load_metric_map[options["lb-scheme"]]
    
    ### based on the placement seed, we need to generate the placements. 
    global placement_files_state
    
    # handle the placement
    placement_mode = options["placement-mode"] 
    ring_mode = options["ring-mode"]
    placement_seed = options["placement-seed"] 
    timing_scheme = options["timing-scheme"]    
    
    with config_sweeper.thread_lock:
        placements_dir = "{}/placements/{}-{}/".format(config_sweeper.custom_files_dir, 
                                                    placement_mode, ring_mode) 
        os.makedirs(placements_dir, exist_ok=True)
        
        placement_file_path = "{}/seed-{}.txt".format(placements_dir, placement_seed)
        
        if placement_file_path not in placement_files_state:
            jobs = generate_placement_file(placement_file_path, placement_seed,   
                                           options, run_context, selected_setting)
            
            placement_files_state[placement_file_path] = jobs

        else: 
            jobs = placement_files_state[placement_file_path]

        options["placement-file"] = placement_file_path
    
    
    
    
    
    # handle the timing
    timings_dir = "{}/timings/{}-{}/{}/".format(config_sweeper.custom_files_dir, 
                                               placement_mode, ring_mode, placement_seed)
    os.makedirs(timings_dir, exist_ok=True)
    
    timing_file_path = "{}/{}.txt".format(timings_dir, timing_scheme)
    
    # get the cache status. We don't have the lock at this point. 
    
    cache_status = None 
    with config_sweeper.thread_lock:
        if timing_file_path in timing_files_states:
            cache_content = timing_files_states[timing_file_path]
            if cache_content == "in progress":
                cache_status = "in progress"
            else:
                cache_status = "ready"
        else:
            cache_status = "you generate it"
            timing_files_states[timing_file_path] = "in progress"   
    
    if cache_status == "ready":
        job_timings = timing_files_states[timing_file_path]    
    
    elif cache_status == "you generate it":
        job_timings = generate_timing_file(timing_file_path, placement_seed, jobs,
                                           options, run_context)
        
        timing_files_states[timing_file_path] = job_timings
        
    elif cache_status == "in progress":
        with open(run_context["output-file"], "a+") as f:   
            f.write("Waiting for the timing file to be ready.\n")
            f.write(timing_file_path)
            
            
        while timing_files_states[timing_file_path] == "in progress":
            print("Waiting for the timing file to be ready.")
            time.sleep(1)
        
        job_timings = timing_files_states[timing_file_path]
        
    
    
    options["timing-file"] = timing_file_path
        
    # move the placement-related stuff out of the options, into the run_context.
    # The simulator should not be concerned with these things. the placement file 
    # should be enough for the simulator to know what to do.
    run_context.update({
        "placement-mode": options["placement-mode"],
        "ring-mode": options["ring-mode"],
        "placement-seed": options["placement-seed"], 
        "timing-scheme": options["timing-scheme"],  
    })
    
    options.pop("placement-mode")   
    options.pop("ring-mode")
    options.pop("placement-seed")   
    options.pop("timing-scheme")

def result_extractor_function(output, options, this_exp_results, run_context):
    for metric in run_context["interesting-metrics"]:
        
        if metric == "avg_ar_time":
            job_numbers = get_all_rep_all_reduce_times(output, options["rep-count"], all_jobs_running=True, )
            
                    
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
        
        
        if "output-file" in run_context: 
            with open(run_context["output-file"], "a+") as f:
                f.write("\nResults for {}:\n".format(metric))
                f.write(json.dumps(job_numbers, indent=4))
                f.write("\n")    
                
        avg_job_numbers = [] 
        for rep in job_numbers:
            sum_job_numbers = 0 
            number_count = 0
            
            for job, numbers in rep.items():
                sum_job_numbers += sum(numbers) 
                number_count += len(numbers)    
                
            avg_job_number = round(sum_job_numbers / number_count, rounding_precision) 
            avg_job_numbers.append(avg_job_number)    
        
        
        this_exp_results.update({
            "min_{}".format(metric): min(avg_job_numbers),
            "max_{}".format(metric): max(avg_job_numbers),
            "last_{}".format(metric): avg_job_numbers[-1],
            "avg_{}".format(metric): round(np.mean(avg_job_numbers), rounding_precision),
            "all_{}".format(metric): avg_job_numbers,  
        })
    

def run_results_modifier(results):
    
    # the results will have everything inside. 
    # results["job-info"] = "{} comm + {} comp + {} layers".format(results["general-param-4"], 
    #                                                              results["general-param-5"], 
    #                                                              results["general-param-6"])
    
    # this is a perfect lb scheme, so we need to change the lb-scheme to "perfect"
    # and change othe other fields back to the original values.
    if results["perfect_lb"]:
        results["lb-scheme"] = "perfect"
        results["ft-agg-core-link-capacity-mult"] = results["original_mult"]
        results["ft-core-count"] = results["original_core_count"]
    
    if results["ideal_network"]:
        results["lb-scheme"] = "ideal"
        results["ft-agg-core-link-capacity-mult"] = results["original_mult"]
        results["ft-core-count"] = results["original_core_count"]
        results["ring-mode"] = results["original_ring_mode"]
        results["timing-scheme"] = results["original_timing_scheme"]
        
    upper_tier_link_capacity = int(results["link-bandwidth"] * results["ft-agg-core-link-capacity-mult"])
    results["cores"] = "{} x {} Gbps".format(results["ft-core-count"], upper_tier_link_capacity)
    
    rack_count = results["machine-count"] // results["ft-server-per-rack"]
    results["machines"] = "{}M x {}R".format(results["ft-server-per-rack"], rack_count)
    
def plot_results(interesting_keys, plotted_key_min, plotted_key_max, 
                 title, random_seed, compact_csv_path, random_csv_path, 
                 compact_plot_path, random_plot_path, script_path, 
                 actually_plot=True):
     
    keys_arg = ",".join(interesting_keys)
    
    for csv_path, plot_path in [(compact_csv_path, compact_plot_path), 
                                (random_csv_path, random_plot_path)]:
            
        plot_command = "python3 plot.py {} {} {} {} {} {}".format(csv_path, plot_path, keys_arg, 
                                                                 plotted_key_min, plotted_key_max, 
                                                                 title, random_seed)

        if actually_plot:
            os.system(plot_command)
    
        with open(script_path, "a+") as f:
            f.write(plot_command)
            f.write("\n")
    
    
def plot_cdfs(separating_params, cdf_params, 
              compact_csv_path, random_csv_path, plots_dir, script_path,
              actually_plot=True):
    
    
    separating_params_str = ",".join(separating_params)
    cdf_params_str = ",".join(cdf_params)
        
    plot_command = "python3 plot_cdf.py {} {} {} {} {}".format(compact_csv_path,
                                                           random_csv_path,  
                                                           separating_params_str, 
                                                           cdf_params_str, plots_dir)

    if actually_plot:
        os.system(plot_command)
    
    with open(script_path, "a+") as f:
        f.write(plot_command)
        f.write("\n")   
                
            
def custom_save_results_func(exp_results_df, config_sweeper, exp_context, plot=False): 
    # refresh the plot commands script
    with open(config_sweeper.plot_commands_script, "w") as f:
        f.write("#!/bin/bash\n")
        
    for metric in exp_context["interesting-metrics"]:
        avg_metric_key = "avg_{}".format(metric)
        metric_csv_dir = config_sweeper.csv_dir + metric + "/" 
        metric_plots_dir = config_sweeper.plots_dir + metric + "/" 
        
        os.makedirs(metric_csv_dir, exist_ok=True)
        os.makedirs(metric_plots_dir, exist_ok=True)
        
        compact_placement_df = exp_results_df[exp_results_df["placement-mode"] == "compact"]
        random_placement_df = exp_results_df[exp_results_df["placement-mode"] == "random"]
        
        merge_on = ["protocol-file-name", "machines", "cores", "placement-seed"]

        compared_ring_mode = exp_context["compared-ring-mode"]  
        compared_lb_scheme = exp_context["compared-lb-scheme"]
        compared_timing_scheme = exp_context["compared-timing-scheme"]
        base_ring_mode = exp_context["base-ring-mode"]
        base_lb_scheme = exp_context["base-lb-scheme"]
        base_timing_scheme = exp_context["base-timing-scheme"]
        
        for placement, placement_df in [("compact", compact_placement_df), 
                                        ("random", random_placement_df)]:

            base = {"ring-mode": "random", "lb-scheme": base_lb_scheme, "timing-scheme": base_timing_scheme}
            
            comparisons = [
                ("OR", {"ring-mode": compared_ring_mode, "lb-scheme": base_lb_scheme, "timing-scheme": base_timing_scheme}), 
                ("LB", {"ring-mode": base_ring_mode, "lb-scheme": compared_lb_scheme, "timing-scheme": base_timing_scheme}),
                ("TS", {"ring-mode": base_ring_mode, "lb-scheme": base_lb_scheme, "timing-scheme": compared_timing_scheme}),
                
                ("OR+LB", {"ring-mode": compared_ring_mode, "lb-scheme": compared_lb_scheme, "timing-scheme": base_timing_scheme}),
                ("OR+TS", {"ring-mode": compared_ring_mode, "lb-scheme": base_lb_scheme, "timing-scheme": compared_timing_scheme}),
                ("LB+TS", {"ring-mode": base_ring_mode, "lb-scheme": compared_lb_scheme, "timing-scheme": compared_timing_scheme}),
                
                ("OR+LB+TS", {"ring-mode": compared_ring_mode, "lb-scheme": compared_lb_scheme, "timing-scheme": compared_timing_scheme}),
                    
                ("Ideal", {"ring-mode": base_ring_mode, "lb-scheme": "ideal", "timing-scheme": base_timing_scheme}),    
            ]
            
            base_df = placement_df 
            for key, value in base.items():
                base_df = base_df[base_df[key] == value]    
                                
            comparison_results = [] 
                                
            for comparison_name, compared_df_setting in comparisons:
                compared_df = placement_df
                for key, value in compared_df_setting.items():
                    compared_df = compared_df[compared_df[key] == value]
                
                merged_df = pd.merge(base_df, compared_df, 
                                     on=merge_on, suffixes=('_base', '_compared'))
                
                base_avg_metric_key = "{}_base".format(avg_metric_key)
                compared_avg_metric_key = "{}_compared".format(avg_metric_key)
                
                merged_df["speedup"] = round(merged_df[base_avg_metric_key] / merged_df[compared_avg_metric_key], rounding_precision)
                
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
                
        # store the final output
        compact_csv_path = metric_csv_dir + "compact_results.csv"
        random_csv_path  = metric_csv_dir + "random_results.csv"
            
        compact_grouped_df.reset_index().to_csv(compact_csv_path, index=False)
        random_grouped_df.reset_index().to_csv(random_csv_path, index=False)

        

                
        # do the plotting, or at least store the plotting commands
        compact_plot_path = metric_plots_dir + "compact_plot.png"
        random_plot_path  = metric_plots_dir + "random_plot.png"
    
        title = "Speedup in {} of {} over {}".format(
            metric, 
            compared_lb_scheme,
            base_lb_scheme,
        )
        
        title = title.replace(" ", "$") # to feed it through the command line arguments.
        
        # plot_results(interesting_keys=["machines" , "cores"], 
        #              plotted_key_min="speedup_or_lb_min", 
        #              plotted_key_max="speedup_or_lb_max", 
        #              title=title, 
        #              random_seed=experiment_seed,
        #              compact_csv_path=compact_csv_path,
        #              random_csv_path=random_csv_path,
        #              compact_plot_path=compact_plot_path,
        #              random_plot_path=random_plot_path, 
        #              script_path=config_sweeper.plot_commands_script, 
        #              actually_plot=plot)

        plot_cdfs(separating_params=["machines", "cores"], 
                  cdf_params=cdf_params, 
                  compact_csv_path=compact_csv_path,
                  random_csv_path=random_csv_path, 
                  plots_dir=metric_plots_dir, 
                  script_path=config_sweeper.plot_commands_script, 
                  actually_plot=plot)
    
        
def main():
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
        
        "priority-allocator": "maxmin", # "fairshare",

        "network-type": "leafspine",    
        "link-bandwidth": 100,
        "ft-rack-per-pod": 1,
        "ft-agg-per-pod": 1,
        "ft-pod-count": -1,
        "ft-server-tor-link-capacity-mult": 1,
        "ft-tor-agg-link-capacity-mult": 1,
        "ft-agg-core-link-capacity-mult": 1,
        
        "shuffle-device-map": False,
        "regret-mode": "none",
        
        "machine-count": selected_setting["machine-count"],
        "ft-server-per-rack": selected_setting["ft-server-per-rack"],
        "general-param-1": selected_setting["machine-count-low"],  
        "general-param-3": selected_setting["machine-count-high"],

        "simulation-seed": experiment_seed 
    }

    interesting_metrics = ["avg_ar_time", "avg_iter_time"] # "iter_minus_ar_time", 
    placement_modes = ["compact", "random"]

    base_lb_scheme = "random"
    base_timing_scheme = "random"
    base_ring_mode = "random" 

    compared_lb_schemes = ["leastloaded",] #"powerof2", "roundrobin", "ecmp", "perfect",
    compared_timing_schemes = ["cassini", "inc"] # "random" "inc"
    compared_ring_modes = ["optimal"]
    oversubs = [2]
    
    random.seed(experiment_seed)
    
    for oversub in oversubs: 
        for lb_scheme in compared_lb_schemes: 
            for timing_scheme in compared_timing_schemes:
                for ring_mode in compared_ring_modes:
                    
                    exp_sweep_config = {
                        "protocol-file-name": ["nethint-test"],

                        # placement and workload parameters
                        "lb-scheme": [base_lb_scheme, lb_scheme, "ideal"],
                        "timing-scheme": [timing_scheme, base_timing_scheme],
                        "ring-mode": [base_ring_mode, ring_mode],
                        "placement-mode": placement_modes, 
                        
                        "ft-core-count": [base_options["ft-server-per-rack"] // oversub],
                        "placement-seed": list(range(1, selected_setting["placement-seed-range"] + 1)), 
                    } 
                    
                    
                    # to be give to the CS, which will be used to populate the run_context.
                    # the run_context will be then handed back to the custom functions. 
                    # am I making this too complicated? I think I am.
                    exp_context = {
                        # base options
                        "base-lb-scheme": base_lb_scheme,
                        "base-timing-scheme": base_timing_scheme,
                        "base-ring-mode": base_ring_mode,
                        
                        # compared options
                        "compared-lb-scheme": lb_scheme,
                        "compared-timing-scheme": timing_scheme,
                        "compared-ring-mode": ring_mode,
                        
                        # other stuff
                        "random-rep-count": random_rep_count,
                        "interesting-metrics": interesting_metrics,
                        "experiment-seed": experiment_seed,
                        "oversub": oversub,
                    } 

                        
                    cs = ConfigSweeper(
                        base_options, exp_sweep_config, exp_context,
                        run_command_options_modifier, 
                        run_results_modifier, 
                        custom_save_results_func, 
                        result_extractor_function,
                        exp_name="nethint_LB+{}_TS+{}_R+{}_{}_{}".format(lb_scheme, 
                                                                         timing_scheme,
                                                                         ring_mode,  
                                                                         oversub, 
                                                                         experiment_seed),
                        worker_thread_count=40, 
                    )
                    
                    cs.sweep()


if __name__ == "__main__":
    main() 
    