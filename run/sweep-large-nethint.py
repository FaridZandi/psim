from utils.util import *
from utils.sweep_base import ConfigSweeper
from utils.util import default_load_metric_map
import pandas as pd 
import numpy as np  
from processing.itertimes_multirep import get_all_rep_iter_lengths, get_all_rep_all_reduce_times
from pprint import pprint 
import copy
import json

settings = [
    { # big settings
        "machine-count": 256,
        "ft-server-per-rack": 16,
        "machine-count-low": 8,
        "machine-count-high": 16,
        "placement-seed-range": 100,
        "comm-size": 20000,
        "comp-size": 1000,
        "layer-count": 3, # layer count
        "iter-count": 30, # iteration count
    }, 
    {
        "machine-count": 64,
        "ft-server-per-rack": 8,
        "machine-count-low": 4,
        "machine-count-high": 8,
        "placement-seed-range": 10,
        "comm-size": 20000,
        "comp-size": 1000,
        "layer-count": 2, # layer count
        "iter-count": 10, # iteration count
    }
]

# selected_settings = settings[0]
selected_setting = settings[1]

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
}

RANDOM_REP_COUNT = 5
experiment_seed = 55

base_lb_scheme = "random"
base_timing_scheme = "random"
base_ring_mode = "random" 

compared_lb_scheme = "" 
compared_timing_scheme = "" 
compared_ring_mode = "optimal"  

oversubs = [1, 2, 4]
interesting_metrics = ["avg_ar_time", "avg_iter_time"] # "iter_minus_ar_time", 
compared_lb_schemes = ["leastloaded"] # "powerof2", "roundrobin", "ecmp", "perfect",
lbs_involving_randomness = ["random", "ecmp", "powerof2"]
compared_timing_schemes = ["inc"]#, "random"]

sweep_config = {
    "protocol-file-name": ["nethint-test"],

    # placement and workload parameters
    "placement-seed": list(range(1, selected_setting["placement-seed-range"] + 1)), # this is a dummy parameter. basically repeat the experiment 10 times
    

    "general-param-4": [selected_setting["comm-size"]], # comm size
    "general-param-5": [selected_setting["comp-size"]], # comp size
    "general-param-6": [selected_setting["layer-count"]], # layer count
    "general-param-7": [selected_setting["iter-count"]], # iteration count
    
    "placement-mode": ["random", "compact"], 
    "ring-mode": ["optimal", "random"],

    "ft-core-count": [], # will be set later
    "lb-scheme": [], # will be set later
    "timing-scheme": [], # will be set later
} 

# the all-reduce time is technically comm_size / machine_count * 2 * (machine_count - 1) / link_bandwidth
# roughly equal to comm_size / link_bandwidth * 2
# comm_size = 20000, and link_bandwidth = 100 -> ar_time = 20000 / 100 * 2 = 400

def generate_timing_file(timing_file_path, jobs, options):
    job_timings = [] 
    
    for job in jobs:
        job_timing = 0 
        
        timing_scheme = options["timing-scheme"]
        
        if timing_scheme == "inc":
            job_timing = 400 * (job["job_id"] - 1)
        elif timing_scheme == "random":
            job_timing = random.randint(0, 8400)
        elif timing_scheme == "zero":
            job_timing = 0
        
        job_timings.append({
            "initial_wait": job_timing,
            "job_id": job["job_id"]
        })    
        
    json.dump(job_timings, open(timing_file_path, "w"))
    
    return job_timings
    
def generate_placement_file(placement_path, placement_seed,   
                            options, run_context):
    
    # the seed will be set at this point everything from here on will be deterministic.
    # for the same experiment seed and placement seed. 
    random.seed(experiment_seed + placement_seed)
    
    machine_count = options["machine-count"]

    placement_mode = options["placement-mode"]
    ring_mode = options["ring-mode"]    
    
    jobs_machine_count_high = options["general-param-3"] 
    jobs_machine_count_low = options["general-param-1"]
    
    jobs = [] 
    
    machines_left = machine_count 
    current_job_id = 1 
    
    
    # random_mode = "either_or"
    random_mode = "range"

    # assigning 1 machine to a job would be bad, beceasse there would be no communication. 
    # so if just one machine is left, then we skip it.    
    while machines_left > 1:
        
        if random_mode == "either_or":
            this_job_machine_count = random.choice([jobs_machine_count_low, jobs_machine_count_high])  
        elif random_mode == "range":
            this_job_machine_count = random.randint(jobs_machine_count_low, jobs_machine_count_high)
            
        if this_job_machine_count > machines_left:
            this_job_machine_count = machines_left    
            
        jobs.append({
            "job_id": current_job_id,   
            "machine_count": this_job_machine_count, 
            "machines": []
        })
        
        machines_left -= this_job_machine_count     
        current_job_id += 1
        
    all_machines = list(range(1, machine_count + 1))
    
    if placement_mode == "random":
        for job in jobs:
            job["machines"] = random.sample(all_machines, job["machine_count"])
            for machine in job["machines"]:
                all_machines.remove(machine)
    elif placement_mode == "compact":   
        for job in jobs:
            job["machines"] = all_machines[:job["machine_count"]]
            all_machines = all_machines[job["machine_count"]:]
                
    if ring_mode == "random":
        for job in jobs:
            random.shuffle(job["machines"]) 
    else:
        for job in jobs:
            job["machines"] = sorted(job["machines"])   
    
    json.dump(jobs, open(placement_path, "w"))

    return jobs
    
placement_files_state = {} 

def run_command_options_modifier(options, config_sweeper, run_context):
    options["simulation-seed"] = experiment_seed 
    
    # I want to have a fixed amount of capacity to the core layer. 
    # If there are more cores, then the capacity per link should be divided.
    # e.g. 1 * 800, 2 * 400, 4 * 200, 8 * 100
    run_context.update({
        "perfect_lb": False,
        "ideal_network": False,
        "original_mult": options["ft-agg-core-link-capacity-mult"],
        "original_core_count": options["ft-core-count"],
    })

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
        options["ft-agg-core-link-capacity-mult"] = run_context["original_core_count"]
        options["ft-core-count"] = 1
    
    
    # ideal network will create a network where the core layer has infinite capacity.
    if options["lb-scheme"] == "ideal":
        run_context["ideal_network"] = True 

        options["lb-scheme"] = "random"
        options["ft-agg-core-link-capacity-mult"] = 1000
        options["ft-core-count"] = 1
    
    options["load-metric"] = default_load_metric_map[options["lb-scheme"]]
    
    ### based on the placement seed, we need to generate the placements. 
    global placement_files_state
    
    # handle the placement
    placements_dir = "{}/placements/".format(config_sweeper.custom_files_dir) 
    os.makedirs(placements_dir, exist_ok=True)
    
    placement_seed = options["placement-seed"] 
    placement_mode = options["placement-mode"] 
    ring_mode = options["ring-mode"]
    
    placement_file_path = "{}/{}-{}-{}.txt".format(placements_dir, placement_seed, 
                                                   placement_mode, ring_mode)
    
    if placement_file_path not in placement_files_state:
        jobs = generate_placement_file(placement_file_path, placement_seed,   
                                       options, run_context)
        
        placement_files_state[placement_file_path] = jobs
    
    options["placement-file"] = placement_file_path
    
    # handle the timing
    timings_dir = "{}/timings/".format(config_sweeper.custom_files_dir)
    os.makedirs(timings_dir, exist_ok=True)
    
    jobs = placement_files_state[placement_file_path]
    timing_file_path = "{}/timings-{}.txt".format(timings_dir, get_random_string(10))
    job_timings = generate_timing_file(timing_file_path, jobs, options)
    
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
    

def run_results_modifier(results):
    
    # the results will have everything inside. 
    print("Run results modifier")
    print("Results: ") 
    pprint(results)
    
    results["job-info"] = "{} comm + {} comp + {} layers".format(results["general-param-4"], 
                                                                 results["general-param-5"], 
                                                                 results["general-param-6"])
    
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
        
    results["cores"] = "{} x {} Gbps".format(results["ft-core-count"], 
                                             int(results["link-bandwidth"] * results["ft-agg-core-link-capacity-mult"]))
    

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
                
            
def custom_save_results_func(exp_results_df, config_sweeper, plot=False): 
    
    for metric in interesting_metrics:
        avg_metric_key = "avg_{}".format(metric)
        metric_csv_dir = config_sweeper.csv_dir + metric + "/" 
        metric_plots_dir = config_sweeper.plots_dir + metric + "/" 
        
        os.makedirs(metric_csv_dir, exist_ok=True)
        os.makedirs(metric_plots_dir, exist_ok=True)
        
        compact_placement_df = exp_results_df[exp_results_df["placement-mode"] == "compact"]
        random_placement_df = exp_results_df[exp_results_df["placement-mode"] == "random"]
        
        merge_on = ["protocol-file-name", "machine-count", "cores", "placement-seed", "job-info"]

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
                
        # store the final output
        compact_csv_path = metric_csv_dir + "compact_results.csv"
        random_csv_path  = metric_csv_dir + "random_results.csv"
            
        compact_grouped_df.reset_index().to_csv(compact_csv_path, index=False)
        random_grouped_df.reset_index().to_csv(random_csv_path, index=False)

        
        # refresh the plot commands script
        with open(config_sweeper.plot_commands_script, "w") as f:
            f.write("#!/bin/bash\n")
                
        # do the plotting, or at least store the plotting commands
        compact_plot_path = metric_plots_dir + "compact_plot.png"
        random_plot_path  = metric_plots_dir + "random_plot.png"
    
        title = "Speedup in {} of {} over {}".format(
            metric, 
            compared_lb_scheme,
            base_lb_scheme,
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
        #              script_path=config_sweeper.plot_commands_script, 
        #              actually_plot=plot)

        plot_cdfs(separating_params=["machine-count", "cores"], 
                  cdf_params=cdf_params, 
                  compact_csv_path=compact_csv_path,
                  random_csv_path=random_csv_path, 
                  plots_dir=metric_plots_dir, 
                  script_path=config_sweeper.plot_commands_script, 
                  actually_plot=plot)
    
        
if __name__ == "__main__":
    random.seed(experiment_seed)
    
    for oversub in oversubs: 
        for lb_scheme in compared_lb_schemes: 
            for timing_scheme in compared_timing_schemes:
                compared_lb_scheme = lb_scheme 
                compared_timing_scheme = timing_scheme
            
                exp_sweep_config = copy.deepcopy(sweep_config)  
                
                exp_sweep_config["lb-scheme"] = [base_lb_scheme, compared_lb_scheme, "ideal"]
                exp_sweep_config["timing-scheme"] = [base_timing_scheme, compared_timing_scheme] 
                
                # with oversub=1, tor uplinks and downlinks are the same.
                # with oversub=2, tor uplinks are half of the downlinks, and so on.
                tor_uplink_links = base_options["ft-server-per-rack"] // oversub
                exp_sweep_config["ft-core-count"] = [tor_uplink_links]
                    
                cs = ConfigSweeper(
                    base_options, exp_sweep_config, 
                    run_command_options_modifier, 
                    run_results_modifier, 
                    custom_save_results_func, 
                    result_extractor_function,
                    exp_name="nethint_{}_{}_{}_{}".format(compared_lb_scheme, 
                                                          compared_timing_scheme, 
                                                          oversub, 
                                                          experiment_seed),
                    worker_thread_count=30, 
                )
                
                cs.sweep()

