from utils.util import *
from utils.util import default_load_metric_map
import pandas as pd 
import numpy as np  
from processing.itertimes_multirep import get_all_rep_iter_lengths, get_all_rep_all_reduce_times
from pprint import pprint 
import copy
import json
from processing.flowprogress import get_job_profiles    
from hashlib import md5

# from algo.timing import generate_timing_file
from algo import timing
from algo.placement import generate_placement_file
import time
import subprocess
from utils.cache import NonBlockingCache

import shutil   

# get the current executable. like if this file was run with python3 or pypy3, for example.
current_executable = sys.executable
lbs_involving_randomness = ["random", "ecmp", "powerof2"]

run_cassini_timing_in_subprocess = True # don't turn this on. 
    
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
    { #6 quite small. 
        "machine-count": 64,
        "ft-server-per-rack": 16,
        "jobs-machine-count-low": 12,
        "jobs-machine-count-high": 16,
        "placement-seed-range": 50,
        "comm-size": [8000, 4000, 2000],
        "comp-size": [600, 800, 400],
        "layer-count": [1],
        "iter-count": [30], # iteration count
    },
    { #7 manual_1
        "machine-count": 16,
        "ft-server-per-rack": 4,
        "jobs-machine-count-low": 4,
        "jobs-machine-count-high": 4,
        "placement-seed-range": 6,
        "comm-size": [8000, 4000, 2000],
        "comp-size": [200, 100, 400],
        "layer-count": [1, 2],
        "iter-count": [30], # iteration count
    },
    { #8 manual_2
        "machine-count": 32,
        "ft-server-per-rack": 8,
        "jobs-machine-count-low": 4,
        "jobs-machine-count-high": 8,
        "placement-seed-range": 30,
        "comm-size": [8000, 4000, 2000],
        "comp-size": [200, 100, 400],
        "layer-count": [1, 2],
        "iter-count": [30], # iteration count
    },
    { #9 manual_3
        "machine-count": 8,
        "ft-server-per-rack": 4,
        "jobs-machine-count-low": 4,
        "jobs-machine-count-high": 4,
        "placement-seed-range": 5,
        "comm-size": [8000, 4000, 2000],
        "comp-size": [200, 100, 400],
        "layer-count": [1, 2],
        "iter-count": [30], # iteration count
    },
    { #10 manual_4
        "machine-count": 12,
        "ft-server-per-rack": 6,
        "jobs-machine-count-low": 5,
        "jobs-machine-count-high": 3,
        "placement-seed-range": 1,
        "comm-size": [8000, 4000, 2000],
        "comp-size": [200, 100, 400],
        "layer-count": [1, 2],
        "iter-count": [30], # iteration count
    }
]

# things that would affect the profiling and the placement. 
placement_related_keys = ["placement-mode", "ring-mode", 
                          "placement-seed", "min-rate", 
                          "punish-oversubscribed", "punish-oversubscribed-min"]

# things that would affect the scheduling, the timing and the routing.
scheduling_related_keys = ["timing-scheme", "subflows", "throttle-search", 
                           "routing-fit-strategy", "compat-score-mode", "farid-rounds", "lb-scheme", "inflate"] 

def summarize_key_ids(key): 
    s = key.split("-")
    # take all the first letters of the words.  
    short_form = "".join([word[0] for word in s])
    return short_form


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
        
        stdout, stderr = process.communicate(input=input_data)
        try:
            output = json.loads(stdout.decode("utf-8")) 

            err_output = stderr.decode("utf-8") 
            with open(run_context["output-file"], "w") as f:
                f.write(err_output) 
                
        except json.JSONDecodeError as e:
            print("Error in the subprocess: ", e)
            print("stdout: ", stdout.decode("utf-8"))
            print("stderr: ", stderr.decode("utf-8"))   
            
            print("input_data: ", input_data.decode("utf-8"))   
            rage_quit("Error in the subprocess")    
        
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
    
    original_options = copy.deepcopy(options)   
    
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
        # options["timing-scheme"] = run_context["comparison-base"]["timing-scheme"]
        options["timing-scheme"] = "zero"
    
    if options["timing-scheme"] == "farid":
        run_context["farid_timing"] = True
        
    if "compat-score-mode" in options: 
        run_context["compat-score-mode"] = options["compat-score-mode"]
        options.pop("compat-score-mode") 
        
    if "throttle-search" in options: 
        run_context["throttle-search"] = options["throttle-search"]
        options.pop("throttle-search")
    
    if "farid-rounds" in options:
        run_context["farid-rounds"] = options["farid-rounds"]
        options.pop("farid-rounds")   
    
    options["load-metric"] = default_load_metric_map[options["lb-scheme"]]
    
    # the subflows are fed to simulator as general-param-1
    # TODO: I don't like this. 
    # run_context["subflows"] = options["subflows"]
    # options["general-param-1"] = options["subflows"]
    # options.pop("subflows") 
    
    ### based on the placement seed, we need to generate the placements. 
    
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
        "inflate": options["inflate"],
    })
    
    options.pop("placement-mode")   
    options.pop("ring-mode")
    options.pop("placement-seed")   
    options.pop("timing-scheme")
    options.pop("inflate")
    
    #########################################################################################################
    # handle the placement
    placement_related_base_path = config_sweeper.custom_files_dir + "/" + "p-"
    placement_related_added_keys = [] 
    for key in placement_related_keys:
        if key in config_sweeper.relevant_keys:
            placement_related_added_keys.append(key)
    # sort the keys so that the path is always the same.    
    # placement_related_added_keys.sort()
    
    # add the keys to the base path.
    placement_identifier = ""   
    for key in placement_related_added_keys:  
        if key in original_options:  
            value = original_options[key]
        else:
            value = run_context[key]
        placement_identifier += summarize_key_ids(key) + "-" + str(value) + "-"
    placement_identifier = placement_identifier[:-1]
    
    placement_related_base_path += (placement_identifier + "/")
    
    tstate = "exp-{}-placement-{}".format(run_context["exp-uuid"], placement_identifier)
    config_sweeper.thread_states[run_context["worker-id-for-profiling"]] = tstate
    
    run_context["placement-related-dir"] = placement_related_base_path

    placements_dir = placement_related_base_path + "placements/"   
    profiles_dir = placement_related_base_path + "profiles/"
    
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

    run_context["jobs"] = jobs  
    
    jobs_str = json.dumps(jobs, sort_keys=True).encode("utf-8")
    run_context["placement-hash"] = md5(jobs_str).hexdigest()
    
    options["placement-file"] = placement_file_path
    
    #######################################################################################################
    
    # handle the timing
    # timings_dir = "{}/timings/{}-{}/{}/{}/{}/{}/{}/{}/".format(config_sweeper.custom_files_dir, 
    #                                             placement_mode, ring_mode, placement_seed, 
    #                                             timing_scheme, 
    #                                             run_context["routing-fit-strategy"], 
    #                                             run_context["compat-score-mode"], 
    #                                             run_context["throttle-search"], 
    #                                             options["subflows"])       
    
    # routings_dir = "{}/routings/{}-{}/{}/{}/{}/{}/{}/{}/".format(config_sweeper.custom_files_dir,    
    #                                             placement_mode, ring_mode, placement_seed,
    #                                             timing_scheme, 
    #                                             run_context["routing-fit-strategy"], 
    #                                             run_context["compat-score-mode"],
    #                                             run_context["throttle-search"], 
    #                                             options["subflows"])   
    
    scheduling_related_base_path = placement_related_base_path + "s-"
    scheduling_related_added_keys = []  
    for key in scheduling_related_keys:
        if key in config_sweeper.relevant_keys:
            scheduling_related_added_keys.append(key)
    
    # scheduling_related_added_keys.sort()
    scheduling_identifier = "" 
    for key in scheduling_related_added_keys:
        if key in original_options:  
            value = original_options[key]    
        else:
            value = run_context[key]    
        scheduling_identifier += summarize_key_ids(key) + "-" + str(value) + "-"
    
    scheduling_identifier = scheduling_identifier[:-1]
    scheduling_related_base_path += (scheduling_identifier + "/")
    
    timings_dir = scheduling_related_base_path + "timings/" 
    routings_dir = scheduling_related_base_path + "routings/"
    
    os.makedirs(timings_dir, exist_ok=True)
    os.makedirs(routings_dir, exist_ok=True)    
    
    run_context["schedulings-dir"] = scheduling_related_base_path   
    run_context["timings-dir"] = timings_dir    
    run_context["routings-dir"] = routings_dir
    
    timing_file_path = "{}/timing.txt".format(timings_dir, timing_scheme)
    routing_file_path = "{}/routing.txt".format(routings_dir, timing_scheme)
    
    config_sweeper.thread_states[run_context["worker-id-for-profiling"]] = "exp-{}-scheduling-{}".format(run_context["exp-uuid"],
                                                                                                         scheduling_identifier)
    
    job_timings, lb_decisions = timing_cache.get(key=timing_file_path, 
                                                 lock=config_sweeper.thread_lock, 
                                                 logger_func=config_sweeper.log_for_thread, 
                                                 run_context=run_context, 
                                                 calc_func=calc_timing, 
                                                 calc_func_args=(timing_file_path, routing_file_path,
                                                                 placement_seed, jobs, options, 
                                                                 run_context, run_cassini_timing_in_subprocess))
    
    if lb_decisions is not None:
        total_subflows = 0
        total_flows = 0   
        for lb_decision in lb_decisions:    
            total_subflows += lb_decision["spine_count"]
            total_flows += 1 
        
        if total_flows == 0:
            run_context["subflow_ratio"] = 1
        else:
            run_context["subflow_ratio"] = total_subflows / total_flows
    else:
        if run_context["perfect_lb"]:
            run_context["subflow_ratio"] = run_context["original_core_count"]
        else:
            run_context["subflow_ratio"] = options["subflows"]   
        
    options["timing-file"] = timing_file_path
    options["routing-file"] = routing_file_path 
    
    run_context["job-timings"] = job_timings    
    
    ###############################################################################################
        
    runtime_related_base_path = run_context["schedulings-dir"] + "r-"
    runtime_related_added_keys = [] 
    for key in config_sweeper.relevant_keys:
        if key not in placement_related_keys and key not in scheduling_related_keys:    
            # everything else that remains would be related to the runtime.
            runtime_related_added_keys.append(key)  
            
    runtime_related_added_keys.sort()   
    
    runtime_identifier = ""
    for key in runtime_related_added_keys:  
        if key in original_options:  
            value = original_options[key]    
        else:
            value = run_context[key]
        runtime_identifier += summarize_key_ids(key) + "-" + str(value) + "-"
    runtime_identifier = runtime_identifier[:-1]    
    runtime_related_base_path += (runtime_identifier + "/")
    
    run_context["runtime-dir"] = runtime_related_base_path
    os.makedirs(runtime_related_base_path, exist_ok=True)    
    
    ###############################################################################################
    options["simulation-seed"] += run_context["placement-seed"]
    
def plot_runtime(output, options, this_exp_results, run_context, config_sweeper):
    if "visualize-timing" not in run_context or run_context["placement-seed"] not in run_context["visualize-timing"]: 
        return   

    # where are the flow files? Make a backup for easy access.
    run_path = "{}/worker-{}/run-1".format(config_sweeper.workers_dir,
                                            run_context["worker-id-for-profiling"])
    flow_files_path = "{}/flow-info.txt".format(run_path)   

    shutil.copy(flow_files_path, run_context["runtime-dir"] + "/flow-info.txt") 


    # copy the flow files to the runtime dir, get the link loads.
    summarized_job_profiles, _, _ = get_job_profiles(flow_files_path, only_summary=True)

    link_loads, _ = timing.get_link_loads_runtime(
        jobs=run_context["jobs"],
        options=options, 
        run_context=run_context,
        summarized_job_profiles=summarized_job_profiles 
    )
    
    
    # the stupid matplotlib doesn't work in a thread.   
    with config_sweeper.thread_lock:
        for smoothing_window in [1, 100]:
            timing.visualize_link_loads_runtime(
                link_loads=link_loads,
                run_context=run_context,
                smoothing_window=smoothing_window, 
                plot_dir=run_context["runtime-dir"],
                suffix="_runtime_{}".format(smoothing_window)
            )
    
    # copy the final timing output to the runtime dir.
    final_timing_output = run_context["timings-dir"] + "/demand_final.png"   
    shutil.copy(final_timing_output, run_context["runtime-dir"] + "/demand_final.png")   
    
    
def get_rolling_numbers(job_numbers, options):
    new_job_numbers = []     
    for rep in range(options["rep-count"]):
        rep_job_numbers = job_numbers[rep]
        new_rep_job_numbers = {}
        
        for job, numbers in rep_job_numbers.items():
            new_rep_job_numbers[job] = []
            for i in range(len(numbers)):
                new_rep_job_numbers[job].append(np.mean(numbers[:i+1]))
                
        new_job_numbers.append(new_rep_job_numbers)
        
    return new_job_numbers


def get_rolling_costs(output, options, this_exp_results, run_context, config_sweeper):
    timing_data = run_context["job-timings"]
    rep_numbers = {}

    for job_timing in timing_data:
        job_id = job_timing["job_id"] 

        # the cost that have been added because of the dalays. 
        delta_costs = job_timing["deltas"]

        # the cost that have been added because of the throttling. 
        throttle_costs = [] 
        this_job = None
        for job in run_context["jobs"]:
            if job["job_id"] == job_id:
                this_job = job
                break 
                
        for throttle_rate in job_timing["throttle_rates"]:    
            base_period = this_job["base_period"]   
            throttled_period = this_job["period"][str(throttle_rate)]
            throttle_cost = throttled_period - base_period
            throttle_costs.append(throttle_cost)    

        # adding the two costs together.    
        total_costs = [delta + throttle for delta, throttle in zip(delta_costs, throttle_costs)] 
        rep_numbers[job_id] = total_costs
        
    job_numbers = [rep_numbers]
    job_numbers = get_rolling_numbers(job_numbers, options)
    
    return job_numbers

def add_up_job_numbers(numbers1, numbers2): 
    new_numbers = [] 
    
    for rep in range(len(numbers1)):
        rep_numbers1 = numbers1[rep]
        rep_numbers2 = numbers2[rep]
    
        new_rep_numbers = {}
        
        for job in rep_numbers1.keys():
            new_rep_numbers[job] = [x + y for x, y in zip(rep_numbers1[job], rep_numbers2[job])]
        
        new_numbers.append(new_rep_numbers)
        
    return new_numbers

def result_extractor_function(output, options, this_exp_results, run_context, config_sweeper):
    plot_runtime(output, options, this_exp_results, run_context, config_sweeper)
    
    # copy the output_file to the runtime dir.
    if "output-file" in run_context:
        shutil.copy(run_context["output-file"],  
                    run_context["runtime-dir"] + "/output.txt")
                        
    printed_metrics = [] 
    run_context["job_numbers"] = {}    
    
    for metric in run_context["interesting-metrics"].keys(): 
        metric_info = run_context["interesting-metrics"][metric]    
        
        all_jobs_running = False
        
        if metric == "rolling_costs":
            job_numbers = get_rolling_costs(output, options, this_exp_results, run_context, config_sweeper)  

        elif metric == "avg_ar_time":
            job_numbers = get_all_rep_all_reduce_times(output, options["rep-count"], 
                                                       all_jobs_running=all_jobs_running)
        
        elif metric == "rolling_ar_time":   
            ar_times = get_all_rep_all_reduce_times(output, options["rep-count"], 
                                                    all_jobs_running=all_jobs_running)
            
            job_numbers = get_rolling_numbers(ar_times, options)  
            
        elif metric == "avg_iter_time": 
            job_numbers = get_all_rep_iter_lengths(output, options["rep-count"], 
                                                   all_jobs_running=all_jobs_running)

        elif metric == "rolling_iter_time":
            iter_times = get_all_rep_iter_lengths(output, options["rep-count"],
                                                  all_jobs_running=all_jobs_running)

            job_numbers = get_rolling_numbers(iter_times, options)   
            
        elif metric == "rolling_ar_plus_cost":
            ar_times = get_all_rep_all_reduce_times(output, options["rep-count"], 
                                                    all_jobs_running=all_jobs_running)

            rollied_ar_times = get_rolling_numbers(ar_times, options)   
            
            job_costs = get_rolling_costs(output, options, this_exp_results, run_context, config_sweeper)   
            
            job_numbers = add_up_job_numbers(rollied_ar_times, job_costs)   

        elif metric == "subflow_ratio":
            job_numbers = run_context["subflow_ratio"]   
                        
        else: 
            rage_quit("Unknown metric: {}".format(metric))

        run_context["job_numbers"][metric] = job_numbers    
        
        if "output-file" in run_context:
            results_path = "{}/results-{}.json".format(run_context["runtime-dir"], metric)
            with open(results_path, "w") as f:
                json.dump(job_numbers, f, indent=4)
        
        if metric_info["avg_cdf_plot"]:
            if metric_info["type"] == "single_number":
                printed_metrics.append(metric)
                this_exp_results.update({   
                    "min_{}".format(metric): job_numbers,   
                    "max_{}".format(metric): job_numbers,
                    "last_{}".format(metric): job_numbers,
                    "avg_{}".format(metric): job_numbers,
                    "all_{}".format(metric): job_numbers,
                })
            elif metric_info["type"] == "per_iter":
                avg_job_numbers = [] 
                for rep in job_numbers:
                    sum_job_numbers = 0 
                    number_count = 0
                    
                    for job, numbers in rep.items():
                        sum_job_numbers += sum(numbers) 
                        number_count += len(numbers)    
                        
                    avg_job_number = round(sum_job_numbers / number_count, rounding_precision) 
                    avg_job_numbers.append(avg_job_number)    
                
                printed_metrics.append("avg_{}".format(metric)) 
                
                this_exp_results.update({
                    "min_{}".format(metric): min(avg_job_numbers),
                    "max_{}".format(metric): max(avg_job_numbers),
                    "last_{}".format(metric): avg_job_numbers[-1],
                    "avg_{}".format(metric): round(np.mean(avg_job_numbers), rounding_precision),
                    "all_{}".format(metric): avg_job_numbers,  
                })
    
    return printed_metrics  

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
                
comparison_color_options_base = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf"]
comparison_color_options = comparison_color_options_base.copy()  
comparison_color_cache = {} 

def get_comparison_color(comparison):
    global comparison_color_options
    if comparison not in comparison_color_cache:
        comparison_color_cache[comparison] = comparison_color_options.pop(0)
        if len(comparison_color_options) == 0:
            comparison_color_options = comparison_color_options_base.copy() 
    return comparison_color_cache[comparison]   


def plot_job_iteration_times(exp_results_df, config_sweeper, global_context):
    import matplotlib.pyplot as plt 
    import seaborn as sns   
    
    iter_avg_plot_metrics = [] 
    for metric in global_context["interesting-metrics"].keys(): 
        metric_info = global_context["interesting-metrics"][metric] 
        if metric_info["iter_avg_plot"]:    
            iter_avg_plot_metrics.append(metric)
    
    exp_results_df = exp_results_df.copy()
    
    all_data_df = pd.DataFrame(
        columns=["placement-hash", "placement-mode", "placement-seed", "ring-mode", "comparison", "metric", "job_id", "iter_id", "value"]    
    )
    all_data_list = []
    
    # for each metric in the interesting metrics, we will plot the iteration times
    # iterate over all rows of the exp_results_df.  
    base_setting = global_context["comparison-base"]
    comparisons = global_context["comparisons"] + [("base", base_setting)]
    
    for c, comp in enumerate(comparisons):
        comparison_name, compared_df_setting = comp 
        
        # get the base setting, update the keys that have been changed.         
        full_compared_df_setting = base_setting.copy()
        full_compared_df_setting.update(compared_df_setting)

        # start with the full df, narrow it down with the settings.
        compared_df = exp_results_df
        for key, value in full_compared_df_setting.items():
            compared_df = compared_df[compared_df[key] == value]
            
        for i, row in compared_df.iterrows():
            placement_hash = row["placement-hash"] 
            placement_mode = row["placement-mode"] 
            placement_seed = row["placement-seed"] 
            ring_mode = row["ring-mode"]   
            job_numbers = row["job_numbers"]
                    
            for metric in iter_avg_plot_metrics:
                metric_numbers = job_numbers[metric][0]
                
                for job_id, numbers in metric_numbers.items():
                    for iter_id, number in enumerate(numbers):  
                        all_data_list.append({
                            "placement-hash": placement_hash,
                            "placement-mode": placement_mode,
                            "placement-seed": placement_seed,   
                            "ring-mode": ring_mode, 
                            "comparison": comparison_name,
                            "metric": metric,
                            "job_id": job_id,
                            "iter_id": iter_id,
                            "value": number
                        })
            
            
    all_data_df = pd.DataFrame(all_data_list)
    
    # merge placement-mode and ring-mode into one column. 
    # with pd.option_context('display.max_rows', None, 'display.max_columns', None):  
    #     print(all_data_df)
        
    csv_path = config_sweeper.csv_dir + f"metric-{metric}.csv"
    all_data_df.to_csv(csv_path, index=False)

    # reduce mean over placement-seed
    all_columns_but_value = list(all_data_df.columns).copy()
    all_columns_but_value.remove("value")    
    all_data_df = all_data_df.groupby(all_columns_but_value).mean().reset_index()
    
    # for each metric, placement, job, iteration, 
    # normalize with respect to the base comparison.
    # stort the results in the "normalized" column. 
    for metric in iter_avg_plot_metrics:
        metric_group = all_data_df[all_data_df["metric"] == metric]
        
        for placement_hash in all_data_df["placement-hash"].unique():
            placement_group = metric_group[metric_group["placement-hash"] == placement_hash]
            
            for job_id in placement_group["job_id"].unique():
                job_group = placement_group[placement_group["job_id"] == job_id]
                
                for iter_id in job_group["iter_id"].unique():
                    iter_group = job_group[job_group["iter_id"] == iter_id]
                    
                    base_value = iter_group[iter_group["comparison"] == "base"].iloc[0]["value"]
                    
                    for i, row in iter_group.iterrows():
                        value = row["value"]
                        
                        metric_info = global_context["interesting-metrics"][metric]  
                        
                        if metric_info["compare_mode"] == "self":
                            normalized_value = value
                        elif metric_info["compare_mode"] == "divide": 
                            normalized_value = value / base_value
                        elif metric_info["compare_mode"] == "subtract":
                            normalized_value = value - base_value
                                
                        all_data_df.loc[i, "normalized"] = normalized_value
    
    csv_path = config_sweeper.csv_dir + f"reduced-{metric}.csv"
    all_data_df.to_csv(csv_path, index=False)
    

    
    
    for plot_type in ["line", "bar"]:
        for metric in iter_avg_plot_metrics:
            metric_group = all_data_df[all_data_df["metric"] == metric] 
            # number of unique placement hashes.    
            placement_hashes = metric_group["placement-hash"].unique()
            plot_width = len(placement_hashes) 
            
            max_height = 0 
            for placement_hash in placement_hashes:  
                placement_group = metric_group[metric_group["placement-hash"] == placement_hash]
                
                jobs = placement_group["job_id"].unique() 
                plot_height = len(jobs)
                max_height = max(max_height, plot_height)    
            
            fig, axs = plt.subplots(max_height, plot_width, figsize=(5 * plot_width, 3 * max_height), squeeze=False)
            
            for i, placement_hash in enumerate(placement_hashes):
                placement_group = metric_group[metric_group["placement-hash"] == placement_hash]
                
                placement_mode = placement_group["placement-mode"].iloc[0] 
                ring_mode = placement_group["ring-mode"].iloc[0]
                placement_seed = placement_group["placement-seed"].iloc[0] 
                column_title = f"{placement_mode}-{ring_mode}-{placement_seed}"
                
                job_ids = placement_group["job_id"].unique() 
                job_ids.sort()
                height = len(job_ids) 
                
                job_plot_index = {job_id: j for j, job_id in enumerate(job_ids)}
                
                # average value for each comparison 
                comparison_mean = placement_group.groupby(["comparison"])["normalized"].mean().reset_index()
                comparison_mean_dict = {row["comparison"]: row["normalized"] for i, row in comparison_mean.iterrows()}
                # sort the comparisons by the mean value.
                sorted_comparisons = sorted(comparison_mean_dict.items(), key=lambda x: x[1])
                sorted_comparisons = [comp for comp, _ in sorted_comparisons]
                colors = [get_comparison_color(comp) for comp in sorted_comparisons]    
                
                for job_id in job_ids:
                    j = job_plot_index[job_id]
                    ax = axs[j, i]  
                    job_group = placement_group[placement_group["job_id"] == job_id]
                    
                    print("plotting the ax: ", i, j, job_id)    
                    legend = (j == height - 1)
                    
                    if plot_type == "line": 
                        sns.lineplot(data=job_group, ax=ax,
                                    x="iter_id", y="normalized", 
                                    hue="comparison", hue_order=sorted_comparisons,
                                    palette=colors,
                                    style="comparison", markers=True,  
                                    legend=legend)
                        
                    elif plot_type == "bar":    
                        sns.barplot(data=job_group, ax=ax,
                                    x="iter_id", y="normalized",
                                    hue="comparison", 
                                    hue_order=sorted_comparisons,
                                    palette=colors,
                                    dodge=True, alpha=0.75, 
                                    errorbar=None)
                        if not legend:
                            ax.get_legend().remove()    

                    if legend: 
                        ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.05))
                        
                    ax.set_xlabel("Iteration")
                    ax.set_ylabel("Time (ms)")  
                
                    if j == 0:
                        ax.set_title(column_title)
                    if i == 0:
                        ax.set_ylabel(f"{job_id}") 
                    
                    
                        
            plt.tight_layout()
            
            plot_dir = "{}/iterations/{}/".format(config_sweeper.plots_dir, plot_type)
            os.makedirs(plot_dir, exist_ok=True)
            plot_path = plot_dir + f"{metric}.png"
            
            plt.savefig(plot_path)
            plt.close()
            plt.clf()
            plt.cla()
        
    return 


def custom_save_results_func(exp_results_df, config_sweeper, global_context, plot=False): 
    if plot: 
        if "plot-iteration-graphs" in global_context and global_context["plot-iteration-graphs"]:
            plot_job_iteration_times(exp_results_df, config_sweeper, global_context)
    
    summary = []
    
    print("Saving the results ...") 
    
    # refresh the plot commands script
    with open(config_sweeper.plot_commands_script, "w") as f:
        f.write("#!/bin/bash\n")
        
    for metric in global_context["interesting-metrics"].keys():
        metric_info = global_context["interesting-metrics"][metric]
        if not metric_info["avg_cdf_plot"]:
            continue
        
        avg_metric_key = "avg_{}".format(metric)
        
        metric_csv_dir = config_sweeper.csv_dir + metric + "/" 
        metric_plots_dir = config_sweeper.plots_dir + "/cdfs/" + metric  
        
        os.makedirs(metric_csv_dir, exist_ok=True)
        os.makedirs(metric_plots_dir, exist_ok=True)
        
        merge_on = ["protocol-file-name", "machines", "cores", "placement-seed"]

        base_setting = global_context["comparison-base"]
        comparisons = global_context["comparisons"]
        
        base_df = exp_results_df 
        for key, value in base_setting.items():
            base_df = base_df[base_df[key] == value]    
                            
        for comparison_name, compared_df_setting in comparisons:
            compared_df = exp_results_df
            
            full_compared_df_setting = base_setting.copy()
            full_compared_df_setting.update(compared_df_setting)
            
            for key, value in full_compared_df_setting.items():
                compared_df = compared_df[compared_df[key] == value]
            
            merged_df = pd.merge(base_df, compared_df, on=merge_on, 
                                 suffixes=('_base', '_compared'))
            
            base_avg_metric_key = "{}_base".format(avg_metric_key)
            compared_avg_metric_key = "{}_compared".format(avg_metric_key)
            
            if metric_info["compare_mode"] == "self":
                merged_df["speedup"] = merged_df[compared_avg_metric_key]
            elif metric_info["compare_mode"] == "divide":
                if metric_info["better"] == "lower":    
                    merged_df["speedup"] = round(merged_df[base_avg_metric_key] / merged_df[compared_avg_metric_key], rounding_precision)
                elif metric_info["better"] == "higher": 
                    merged_df["speedup"] = round(merged_df[compared_avg_metric_key] / merged_df[base_avg_metric_key], rounding_precision)
            elif metric_info["compare_mode"] == "subtract":
                # merged_df["speedup"] = merged_df[compared_avg_metric_key] - merged_df[base_avg_metric_key]
                if metric_info["better"] == "lower":    
                    merged_df["speedup"] = round(merged_df[base_avg_metric_key] - merged_df[compared_avg_metric_key], rounding_precision)
                elif metric_info["better"] == "higher": 
                    merged_df["speedup"] = round(merged_df[compared_avg_metric_key] - merged_df[base_avg_metric_key], rounding_precision)
                    
            summary.append({
                "metric": metric,   
                "comparison": comparison_name,
                "mean": merged_df["speedup"].mean(),    
                "values": sorted(list(merged_df["speedup"]))
            })
            
            saved_columns = merge_on + ["speedup"]

            csv_path = metric_csv_dir + f"speedup_{comparison_name}.csv"
            speedup_df = merged_df[saved_columns]
            speedup_df.to_csv(csv_path, index=False)

            # comparison_results.append((comparison_name, speedup_df))
        
        # super_merged = comparison_results[0][1]
        
        # column_name = "speedup_{}".format(comparison_results[0][0]) 
        # super_merged.rename(columns={"speedup": column_name}, inplace=True)              

        # for comparison_name, comparison_df in comparison_results[1:]:
        #     super_merged = super_merged.merge(comparison_df, on=merge_on)
        #     super_merged.rename(columns={"speedup": "speedup_{}".format(comparison_name)}, inplace=True)
        
        # # reduce the dataframe on "placement-seed"
        # group_on = merge_on.copy()
        # group_on.remove("placement-seed")

        # agg_dict = {}
        
        # for comparison_name, _ in comparisons:
        #     agg_dict[f"speedup_{comparison_name}_min"] = (f"speedup_{comparison_name}", "min")
        #     agg_dict[f"speedup_{comparison_name}_max"] = (f"speedup_{comparison_name}", "max")
        #     agg_dict[f"speedup_{comparison_name}_values"] = (f"speedup_{comparison_name}", lambda x: sorted(list(x)))

        # cdf_params = ["speedup_{}_values".format(comparison_name) for comparison_name, _ in comparisons] 
        
        # grouped_df = super_merged.groupby(by=group_on).agg(**agg_dict)
            
        # # store the results for the current placement 
        # metric_grouped_csv_path = "{}/results.csv".format(metric_csv_dir)   
        # grouped_df.reset_index().to_csv(metric_grouped_csv_path, index=False)

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
        
        # if config_sweeper.plot_cdfs:
        #     # store the final output
        #     plot_cdfs(separating_params=["machines", "cores"], 
        #             cdf_params=cdf_params, 
        #             placement_names="placement_names",
        #             placement_csv_paths=[metric_grouped_csv_path],
        #             plots_dir=metric_plots_dir, 
        #             script_path=config_sweeper.plot_commands_script, 
        #             actually_plot=plot)

    return summary
    
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
    
    for key, value in sweep_config.items():
        if len(value) > 1: 
            if key not in comparison_base and key != "placement-seed" and key != "placement-mode":
                print("{} is not in the comparison base, but is being swept".format(key))
                reasons.append("{} is not in the comparison base, but is being swept".format(key))
    
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
    
def exp_filter_function(permutations_dicts, config_sweeper):    
    
    # print("starting to filter the permutations, starting with {} permutations".format(len(permutations_dicts)))

    # comparison_base = config_sweeper.global_context["comparison-base"].copy()
    # comparisons = config_sweeper.global_context["comparisons"].copy() 

    # comparison_settings = [comparison_base.copy()]  
    # for comparison in comparisons:  
    #     comparison_setting = comparison_base.copy()
    #     for key, value in comparison[1].items():
    #         comparison_setting[key] = value
    #     comparison_settings.append(comparison_setting)  
        
    # filtered_permutations = []
    # relevant_keys = set() 
    
    # for exp in permutations_dicts:
    #     exp_valid = False    
    #     for comparison in comparison_settings:  
    #         found_in_this_comparison = True 
    #         for key, value in comparison.items():
    #             relevant_keys.add(key)
    #             if exp[key] != value:
    #                 found_in_this_comparison = False
    #                 break
    #         if found_in_this_comparison:
    #             exp_valid = True
    #             break
    #     if exp_valid:
    #         filtered_permutations.append(exp)
            
    # for key, value in config_sweeper.sweep_config.items():
    #     if len(value) > 1:
    #         relevant_keys.add(key)
    
    # print("filtered down to {} permutations".format(len(filtered_permutations)))
    
    # return filtered_permutations, relevant_keys
    

    comparison_base = config_sweeper.global_context["comparison-base"].copy()
    comparisons = config_sweeper.global_context["comparisons"].copy() 
    
    comparison_permutations = [] 
    
    # add the comparison_base to the permutations_dicts
    comparison_permutations.append(comparison_base)
    
    for comparison in comparisons:  
        comparison_setting = comparison_base.copy()
        for key, value in comparison[1].items():
            comparison_setting[key] = value

        comparison_permutations.append(comparison_setting)  
    
    
    # for all comparisons, and for all permutations_dicts, put them together and add them to the filtered_permutations
    
    filtered_permutations = []
    
    for comparison in comparison_permutations:
        for exp in permutations_dicts:
            new_exp = exp.copy()
            
            new_exp.update(comparison)
            filtered_permutations.append(new_exp)   
            
    relevant_keys = set()  
    
    for perm in filtered_permutations:
        for key in perm.keys():
            relevant_keys.add(key)
    
    return filtered_permutations, relevant_keys 
     