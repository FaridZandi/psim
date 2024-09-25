from utils.util import *
from utils.util import default_load_metric_map
import pandas as pd 
import numpy as np  
from processing.itertimes_multirep import get_all_rep_iter_lengths, get_all_rep_all_reduce_times
from pprint import pprint 
import copy
import json

# from algo.timing import generate_timing_file
from algo import timing
from algo.placement import generate_placement_file
import time
import subprocess

#########################
# TODO: 
# 4. Add support for more complex protocols to be recognized by the timing. Eventually, ideally, 
#    There would be something like: running the simulator, only with one job, and acquire the loads 
#    from the simulator. Then use these loads to generate the timing.
#########################

# get the current executable. like if this file was run with python3 or pypy3, for example.
current_executable = sys.executable

run_cassini_timing_in_subprocess = True
lbs_involving_randomness = ["random", "ecmp", "powerof2"]
placement_files_state = {} 
timing_files_states = {} 
    

nethint_settings = [
    { # big settings
        "machine-count": 256,
        "ft-server-per-rack": 16,
        "jobs-machine-count-low": 8,
        "jobs-machine-count-high": 16,
        "placement-seed-range": 5,
        "comm-size": [20000, 40000],
        "comp-size": [1000, 2000],
        "layer-count": [1],
        "iter-count": [30], # iteration count
    }, 
    { # small settings
        "machine-count": 64,
        "ft-server-per-rack": 8,
        "jobs-machine-count-low": 4,
        "jobs-machine-count-high": 8,
        "placement-seed-range": 10,
        "comm-size": [20000, 10000],
        "comp-size": [500, 1000],
        "layer-count": [2, 3],
        "iter-count": [10], # iteration count
    },
    { # tiny settings
        "machine-count": 80,
        "ft-server-per-rack": 16,
        "jobs-machine-count-low": 10,
        "jobs-machine-count-high": 10,
        "placement-seed-range": 10,
        "comm-size": [20000],
        "comp-size": [1000],
        "layer-count": [1],
        "iter-count": [20], # iteration count
    },
    { # tiny settings
        "machine-count": 256,
        "ft-server-per-rack": 16,
        "jobs-machine-count-low": 8,
        "jobs-machine-count-high": 16,
        "placement-seed-range": 10,
        "comm-size": [20000],
        "comp-size": [1000],
        "layer-count": [1],
        "iter-count": [20], # iteration count
    },
    { # very small
        "machine-count": 128,
        "ft-server-per-rack": 8,
        "jobs-machine-count-low": 4,
        "jobs-machine-count-high": 8,
        "placement-seed-range": 30,
        "comm-size": [20000],
        "comp-size": [1000],
        "layer-count": [1],
        "iter-count": [50], # iteration count
    }
]




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
            options["rep-count"] = run_context["random-rep-count"]

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
        options["ring-mode"] = run_context["comparison-base"]["ring-mode"]
        options["timing-scheme"] = run_context["comparison-base"]["timing-scheme"]
    
    options["load-metric"] = default_load_metric_map[options["lb-scheme"]]
    
    
    # the subflows are fed to simulator as general-param-1
    # TODO: I don't like this. 
    run_context["subflows"] = options["subflows"]
    options["general-param-1"] = options["subflows"]
    options.pop("subflows") 
    
    ### based on the placement seed, we need to generate the placements. 
    global placement_files_state
    global timing_files_states
    
    # handle the placement
    placement_mode = options["placement-mode"] 
    ring_mode = options["ring-mode"]
    placement_seed = options["placement-seed"] 
    timing_scheme = options["timing-scheme"]    
    
    
    config_sweeper.log_for_thread(run_context, "Going to acquire the lock to generating the placement file ...")
    with config_sweeper.thread_lock:
        
        config_sweeper.log_for_thread(run_context, "Acquired the lock to generating the placement file ...")

        placements_dir = "{}/placements/{}-{}/".format(config_sweeper.custom_files_dir, 
                                                    placement_mode, ring_mode) 
        os.makedirs(placements_dir, exist_ok=True)
        
        placement_file_path = "{}/seed-{}.txt".format(placements_dir, placement_seed)
        
        if placement_file_path not in placement_files_state:
            jobs = generate_placement_file(placement_file_path, placement_seed,   
                                           options, run_context)
            
            placement_files_state[placement_file_path] = jobs

        else: 
            jobs = placement_files_state[placement_file_path]

        options["placement-file"] = placement_file_path
    
    config_sweeper.log_for_thread(run_context, "Releasing the lock to generating the placement file ...")
    
    
    # handle the timing
    timings_dir = "{}/timings/{}-{}/{}/".format(config_sweeper.custom_files_dir, 
                                               placement_mode, ring_mode, placement_seed)
    os.makedirs(timings_dir, exist_ok=True)
    
    timing_file_path = "{}/{}.txt".format(timings_dir, timing_scheme)
    
    # get the cache status. We don't have the lock at this point. 
    
    cache_status = None 
    config_sweeper.log_for_thread(run_context, "Going to acquire the lock to check the timing file ...")
    
    with config_sweeper.thread_lock:
        
        config_sweeper.log_for_thread(run_context, "acquired the lock to check the timing file ...")
        
        if timing_file_path in timing_files_states:
            cache_content = timing_files_states[timing_file_path]
            if cache_content == "in progress":
                cache_status = "in progress"
            else:
                cache_status = "ready"
        else:
            cache_status = "you generate it"
            timing_files_states[timing_file_path] = "in progress"   
    
    config_sweeper.log_for_thread(run_context, "Releasing the lock to check the timing file ...")
    config_sweeper.log_for_thread(run_context, "cache status: {}".format(cache_status))
    
    if cache_status == "ready":
        job_timings = timing_files_states[timing_file_path]    
    
    elif cache_status == "you generate it":
        
        timing_scheme = options["timing-scheme"]
        
        if timing_scheme != "cassini" or not run_cassini_timing_in_subprocess: 
            job_timings = timing.generate_timing_file(timing_file_path, placement_seed, jobs,
                                                      options, run_context)
        else: 
            # create a subprocess to run the cassini timing. 
            args = {
                "timing_file_path": timing_file_path,
                "placement_seed": placement_seed,
                "jobs": jobs,   
                "options": options, 
                "run_context": run_context,
            }

            # create a python subprocess, feed the json dump of the args to the subprocess.
            process = subprocess.Popen([current_executable, "algo/timing.py"], 
                                        stdin=subprocess.PIPE, 
                                        stdout=subprocess.PIPE, 
                                        stderr=subprocess.PIPE)
                                       
            input_data = json.dumps(args).encode("utf-8")
            stdout, stderr = process.communicate(input=input_data)
            job_timings = json.loads(stdout.decode("utf-8")) 
            
        
        timing_files_states[timing_file_path] = job_timings
        
    elif cache_status == "in progress":
        with open(run_context["output-file"], "a+") as f:   
            f.write("Waiting for the timing file to be ready.\n")
            f.write(timing_file_path)
            
        print("going to wait for the timing file to be ready ...")    
        while timing_files_states[timing_file_path] == "in progress":
            time.sleep(1)
        print("timing file is ready ...")
        
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
                 title, random_seed, csv_path, plot_path, script_path, 
                 actually_plot=True):
     
    keys_arg = ",".join(interesting_keys)
    
    plot_command = "python3 plot.py {} {} {} {} {} {}".format(csv_path, plot_path, keys_arg, 
                                                                plotted_key_min, plotted_key_max, 
                                                                title, random_seed)

    if actually_plot:
        print("Running: ", plot_command)
        os.system(plot_command)

    with open(script_path, "a+") as f:
        f.write(plot_command)
        f.write("\n")
    
    
def plot_cdfs(separating_params, cdf_params, 
              placement_names, placement_csv_paths, 
              plots_dir, script_path,
              actually_plot=True):
    
    
    separating_params_str = ",".join(separating_params)
    cdf_params_str = ",".join(cdf_params)
    placement_names_str = ",".join(placement_names) 
    placement_csv_paths_str = ",".join(placement_csv_paths)
        
    plot_command = "python3 plot_cdf.py {} {} {} {} {}".format(placement_names_str,
                                                               placement_csv_paths_str,  
                                                               separating_params_str, 
                                                               cdf_params_str, 
                                                               plots_dir)

    if actually_plot:
        print("Running: ", plot_command)
        os.system(plot_command)
    
    with open(script_path, "a+") as f:
        f.write(plot_command)
        f.write("\n")   
                
            
def custom_save_results_func(exp_results_df, config_sweeper, exp_context, plot=False): 
    
    print("Saving the results ...") 
    
    # refresh the plot commands script
    with open(config_sweeper.plot_commands_script, "w") as f:
        f.write("#!/bin/bash\n")
        
    all_placement_modes = exp_context["all-placement-modes"]
    
    for metric in exp_context["interesting-metrics"]:
        avg_metric_key = "avg_{}".format(metric)
        metric_csv_dir = config_sweeper.csv_dir + metric + "/" 
        metric_plots_dir = config_sweeper.plots_dir + metric + "/" 
        
        os.makedirs(metric_csv_dir, exist_ok=True)
        os.makedirs(metric_plots_dir, exist_ok=True)
        
        merge_on = ["protocol-file-name", "machines", "cores", "placement-seed"]
        
        placement_results = []
        
        for placement in all_placement_modes:
            placement_df = exp_results_df[exp_results_df["placement-mode"] == placement]
            metric_placement_csv_dir = metric_csv_dir + placement + "/"
            
            os.makedirs(metric_placement_csv_dir, exist_ok=True)

            base_setting = exp_context["comparison-base"]
            comparisons = exp_context["comparisons"]
            
            base_df = placement_df 
            for key, value in base_setting.items():
                base_df = base_df[base_df[key] == value]    
                                
            comparison_results = [] 
                                
            for comparison_name, compared_df_setting in comparisons:
                compared_df = placement_df
                
                full_compared_df_setting = base_setting.copy()
                full_compared_df_setting.update(compared_df_setting)
                
                for key, value in full_compared_df_setting.items():
                    compared_df = compared_df[compared_df[key] == value]
                
                merged_df = pd.merge(base_df, compared_df, 
                                     on=merge_on, suffixes=('_base', '_compared'))
                
                base_avg_metric_key = "{}_base".format(avg_metric_key)
                compared_avg_metric_key = "{}_compared".format(avg_metric_key)
                
                merged_df["speedup"] = round(merged_df[base_avg_metric_key] / merged_df[compared_avg_metric_key], rounding_precision)
                
                saved_columns = merge_on + ["speedup"]

                csv_path = metric_placement_csv_dir + f"speedup_{comparison_name}.csv"
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
                
            # store the results for the current placement 
            metric_placement_grouped_csv_path = "{}/{}_results.csv".format(metric_placement_csv_dir, placement)   
            grouped_df.reset_index().to_csv(metric_placement_grouped_csv_path, index=False)

            # do some plotting as well. 
            # metric_placement_plot_path = metric_plots_dir + "{}.png".format(placement)

            # title = "Speedup in {} of {} over {}".format(
            #     metric, compared_lb_scheme, base_lb_scheme,
            # ).replace(" ", "$") 
            
            # plot_results(interesting_keys=["machines" , "cores"], 
            #              plotted_key_min="speedup_or_lb_min", 
            #              plotted_key_max="speedup_or_lb_max", 
            #              title=title, 
            #              random_seed=experiment_seed,
            #              csv_path=metric_placement_grouped_csv_path,
            #              plot_path=metric_placement_plot_path,
            #              script_path=config_sweeper.plot_commands_script, 
            #              actually_plot=plot)
            
            placement_results.append((placement, metric_placement_grouped_csv_path))


        placement_names = [placement for placement, _ in placement_results]
        placement_csv_paths = [csv_path for _, csv_path in placement_results]
        
        # store the final output
        plot_cdfs(separating_params=["machines", "cores"], 
                  cdf_params=cdf_params, 
                  placement_names=placement_names,
                  placement_csv_paths=placement_csv_paths,
                  plots_dir=metric_plots_dir, 
                  script_path=config_sweeper.plot_commands_script, 
                  actually_plot=plot)

    print("Done with the metric: ", metric)
    
    