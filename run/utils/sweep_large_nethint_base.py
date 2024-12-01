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
from utils.cache import NonBlockingCache

#########################
# TODO: 
# 4. Add support for more complex protocols to be recognized by the timing. Eventually, ideally, 
#    There would be something like: running the simulator, only with one job, and acquire the loads 
#    from the simulator. Then use these loads to generate the timing.
#########################

# get the current executable. like if this file was run with python3 or pypy3, for example.
current_executable = sys.executable

run_cassini_timing_in_subprocess = False # don't turn this on. 
lbs_involving_randomness = ["random", "ecmp", "powerof2"]
placement_files_state = {} 
# timing_files_states = {} 
    
# self, lock, logger_func, run_context, cache_dir, calc_func
timing_cache = NonBlockingCache("timing-cache")
placement_cache = NonBlockingCache("placement-cache")    

nethint_settings = [
    { #0 big settings
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
    { #1 small settings
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
    { #2 tiny settings
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
    { #3 tiny settings
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
    { #4 very small
        "machine-count": 24,
        "ft-server-per-rack": 8,
        "jobs-machine-count-low": 6,
        "jobs-machine-count-high": 6,
        "placement-seed-range": 5,
        "comm-size": [20000],
        "comp-size": [1000],
        "layer-count": [1],
        "iter-count": [30], # iteration count
    }, 
    { #5 quite small. 
        "machine-count": 32,
        "ft-server-per-rack": 8,
        "jobs-machine-count-low": 4,
        "jobs-machine-count-high": 8,
        "placement-seed-range": 10,
        "comm-size": [80000],
        "comp-size": [1300, 1000],
        "layer-count": [1],
        "iter-count": [30], # iteration count
    },
    { #5 quite small. 
        "machine-count": 64,
        "ft-server-per-rack": 16,
        "jobs-machine-count-low": 12,
        "jobs-machine-count-high": 16,
        "placement-seed-range": 10,
        "comm-size": [8000, 4000, 2000],
        "comp-size": [200, 100, 400],
        "layer-count": [1, 2],
        "iter-count": [30], # iteration count
    }
]


def calc_timing(timing_file_path, routing_file_path, placement_seed, 
                jobs, options, run_context, run_cassini_timing_in_subprocess): 
    import json 
    
    timing_scheme = run_context["timing-scheme"]
    
    if not run_cassini_timing_in_subprocess: 
        job_timings, lb_decisions = timing.generate_timing_file(timing_file_path, 
                                                                routing_file_path,
                                                                placement_seed, 
                                                                jobs, 
                                                                options, 
                                                                run_context)
    else: 
        # create a subprocess to run the cassini timing. 
        args = {
            "timing_file_path": timing_file_path,
            "routing_file_path": routing_file_path, 
            "placement_seed": placement_seed,
            "jobs": jobs,   
            "options": options, 
            "run_context": run_context,
        }
        
        # create a python subprocess, feed the json dump of the args to the subprocess.
        process = subprocess.Popen([current_executable, "-m", "algo.timing"], 
                                    stdin=subprocess.PIPE, 
                                    stdout=subprocess.PIPE, 
                                    stderr=subprocess.PIPE)
                                    
        input_data = json.dumps(args).encode("utf-8")
        
        stdout, _ = process.communicate(input=input_data)
        output = json.loads(stdout.decode("utf-8")) 
        
        job_timings = output["job_timings"] 
        lb_decisions = output["lb_decisions"]
        
    return job_timings, lb_decisions    
   
def calc_placement(placement_file_path, placement_seed, options, run_context, config_sweeper):
    
    jobs = generate_placement_file(placement_file_path, 
                                   placement_seed,   
                                   options, 
                                   run_context,
                                   config_sweeper)  
    
    return jobs
   
def run_command_options_modifier(options, config_sweeper, run_context):
    
    # I want to have a fixed amount of capacity to the core layer. 
    # If there are more cores, then the capacity per link should be divided.
    # e.g. 1 * 800, 2 * 400, 4 * 200, 8 * 100
    run_context.update({
        "perfect_lb": False,
        "ideal_network": False,
        "farid_timing": False,   
        "original_mult": options["ft-agg-core-link-capacity-mult"],
        "original_core_count": options["ft-core-count"],
        "original_lb_scheme": options["lb-scheme"],
        "original_ring_mode": options["ring-mode"],
        "original_timing_scheme": options["timing-scheme"],
    })

    if "routing-fit-strategy" in options:
        run_context["routing-fit-strategy"] = options["routing-fit-strategy"]
        options.pop("routing-fit-strategy") 
    else: 
        run_context["routing-fit-strategy"] = ""
        
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
    
    # if it's farid, then we will feed the routing decisions to the simulator, 
    # through the input files. So the lb scheme should be readprotocol. 
    if options["timing-scheme"] == "farid":
        run_context["farid_timing"] = True
        options["lb-scheme"] = "readprotocol"
        
    if "compat-score-mode" in options: 
        run_context["compat-score-mode"] = options["compat-score-mode"]
        options.pop("compat-score-mode")    
    
    options["load-metric"] = default_load_metric_map[options["lb-scheme"]]
    
    # the subflows are fed to simulator as general-param-1
    # TODO: I don't like this. 
    # run_context["subflows"] = options["subflows"]
    # options["general-param-1"] = options["subflows"]
    # options.pop("subflows") 
    
    ### based on the placement seed, we need to generate the placements. 
    global placement_files_state
    global timing_files_states
    
    # handle the placement
    placement_mode = options["placement-mode"] 
    ring_mode = options["ring-mode"]
    placement_seed = options["placement-seed"] 
    timing_scheme = options["timing-scheme"]    
    
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

    #########################################################################################################
    # handle the placement  
    placements_dir = "{}/placements/{}-{}/{}/".format(config_sweeper.custom_files_dir, 
                                                      placement_mode, ring_mode, placement_seed) 
    profiles_dir = "{}/profiles/{}-{}/{}/".format(config_sweeper.custom_files_dir, 
                                                    placement_mode, ring_mode, placement_seed)

    os.makedirs(placements_dir, exist_ok=True)
    os.makedirs(profiles_dir, exist_ok=True)

    run_context["placements_dir"] = placements_dir
    run_context["profiles-dir"]   = profiles_dir

    placement_file_path = "{}/placement.txt".format(placements_dir)
        
    jobs = placement_cache.get(key=placement_file_path, 
                               lock=config_sweeper.thread_lock, 
                               logger_func=config_sweeper.log_for_thread, 
                               run_context=run_context, 
                               calc_func=calc_placement, 
                               calc_func_args=(placement_file_path, placement_seed, 
                                               options, run_context, config_sweeper))

    options["placement-file"] = placement_file_path
    
    #########################################################################################################
    
    # handle the timing
    timings_dir = "{}/timings/{}-{}/{}/{}/{}/{}/".format(config_sweeper.custom_files_dir, 
                                                placement_mode, ring_mode, placement_seed, 
                                                timing_scheme, 
                                                run_context["routing-fit-strategy"], 
                                                run_context["compat-score-mode"])   
    
    routings_dir = "{}/routings/{}-{}/{}/{}/{}/{}".format(config_sweeper.custom_files_dir,    
                                                placement_mode, ring_mode, placement_seed,
                                                timing_scheme, 
                                                run_context["routing-fit-strategy"], 
                                                run_context["compat-score-mode"])
    
    os.makedirs(timings_dir, exist_ok=True)
    os.makedirs(routings_dir, exist_ok=True)    
    
    run_context["timings-dir"] = timings_dir    
    run_context["routings-dir"] = routings_dir
    
    timing_file_path = "{}/timing.txt".format(timings_dir, timing_scheme)
    routing_file_path = "{}/routing.txt".format(routings_dir, timing_scheme)
    
    
    job_timings, lb_decisions = timing_cache.get(key=timing_file_path, 
                                                 lock=config_sweeper.thread_lock, 
                                                 logger_func=config_sweeper.log_for_thread, 
                                                 run_context=run_context, 
                                                 calc_func=calc_timing, 
                                                 calc_func_args=(timing_file_path, routing_file_path,
                                                                 placement_seed, jobs, options, 
                                                                 run_context, run_cassini_timing_in_subprocess))
    
    options["timing-file"] = timing_file_path
    options["routing-file"] = routing_file_path 
    
    #########################################################################################################
        

def result_extractor_function(output, options, this_exp_results, run_context):
    for metric in run_context["interesting-metrics"]:
        
        all_jobs_running = False
        
        if metric == "avg_ar_time":
            job_numbers = get_all_rep_all_reduce_times(output, options["rep-count"], 
                                                       all_jobs_running=all_jobs_running)
            
                    
        elif metric == "avg_iter_time": 
            job_numbers = get_all_rep_iter_lengths(output, options["rep-count"], 
                                                   all_jobs_running=all_jobs_running)
            
        elif metric == "iter_minus_ar_time":
            ar_times = get_all_rep_all_reduce_times(output, options["rep-count"], 
                                                    all_jobs_running=all_jobs_running)
            
            iter_times = get_all_rep_iter_lengths(output, options["rep-count"], 
                                                  all_jobs_running=all_jobs_running)
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
            rage_quit("Unknown metric: {}".format(metric))
        
        
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
    
    if results["farid_timing"]: 
        results["lb-scheme"] = results["original_lb_scheme"]   
         
        
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
                
            
def custom_save_results_func(exp_results_df, config_sweeper, global_context, plot=False): 
    
    print("Saving the results ...") 
    
    # refresh the plot commands script
    with open(config_sweeper.plot_commands_script, "w") as f:
        f.write("#!/bin/bash\n")
        
    all_placement_modes = global_context["all-placement-modes"]
    
    for metric in global_context["interesting-metrics"]:
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

            base_setting = global_context["comparison-base"]
            comparisons = global_context["comparisons"]
            
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
    

def check_comparison_sanity(exp_context, sweep_config):
    
    comparison_base  = exp_context["comparison-base"]   
    comparisons = exp_context["comparisons"] 
    
    comparison_reqs = {} 
    
    for key, value in comparison_base.items():
        comparison_reqs[key] = [value]
            
    for comparison_name, comparison_settings in comparisons:
        for key, value in comparison_settings.items():
            comparison_reqs[key].append(value)
            comparison_reqs[key] = list(set(comparison_reqs[key]))  
        
    pprint(comparison_reqs)    
    reasons = [] 
    
    for key, values in comparison_reqs.items():
        sweep_config_values = sweep_config[key] 
        
        for value in values:
            if value not in sweep_config_values:
                print("{}:{} is not in the sweep config.".format(key, value))
                reasons.append("{}:{} is not in the sweep config.".format(key, value))
    
    if len(reasons) == 0:
        return True, ["All good"]
    else: 
        return False, reasons