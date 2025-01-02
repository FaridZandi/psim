# cython: language_level=3

import random
import json
from pprint import pprint 
import math 
import time 
import sys 
import copy 
from algo.routing import route_flows
import subprocess
import os 
import pickle as pkl 
import numpy as np 
from utils.util import rage_quit

####################################################################################
##################  HELPER FUNCTIONS  ##############################################
####################################################################################
# EVAL_MODE = "cpp"
EVAL_MODE = "python"

def log_results(run_context, key, value):
    # print to stderr first  
    sys.stderr.write(f"KEY: {key}\n")
    sys.stderr.write(f"VALUE: {value}\n")   

    with open(run_context["output-file"], "a+") as f:
        f.write("Results for: " + key + "\n\n")
        pprint(value, f) 
        f.write("\n\n---------------------------------\n\n")   


def visualize_final_timing(jobs, options, run_context, job_timings, job_profiles, lb_decisions):
    if "visualize-timing" not in run_context or not run_context["visualize-timing"]:    
        return
    
    link_loads, cross_rack_jobs = get_link_loads(jobs, options, run_context, job_profiles)

    deltas = {}
    throttle_rates = {} 
    for job_timing in job_timings:
        deltas[job_timing["job_id"]] = job_timing["deltas"]
        throttle_rates[job_timing["job_id"]] = job_timing["throttle_rates"]
    
    link_logical_bandwidth = options["ft-core-count"] * options["ft-agg-core-link-capacity-mult"]
    
    visualize_link_loads(link_loads, 
                         run_context, 
                         deltas=deltas, 
                         throttle_rates=throttle_rates,
                         link_logical_bandwidth=link_logical_bandwidth, 
                         suffix="_final")

# all the workloads will be starting at the same time, at time 0.
# this is technically the worst case scenario.
def zero_timing(jobs, options, run_context, timing_scheme, job_profiles):
    job_timings = [] 
    
    for job in jobs:
        job_timings.append({
            "deltas": [0] * job["iter_count"],
            "throttle_rates": [1.0] * job["iter_count"],    
            "job_id": job["job_id"]
        })     
        
    return job_timings 


# trying to spread the jobs out a bit in time. The number 400 is arbitrary.
# it's chosen based on the curren numbers. 
# TODO: make this number something that could be found based on the job profiles.
# in some way, a much much simpler version of the cassini timing.
def inc_timing(jobs, options, run_context, timing_scheme, job_profiles):
    job_timings = [] 
    
    timing_scheme_split = timing_scheme.split("_") 
    
    if len(timing_scheme_split) == 1:
        job_timing_increment = 400
    else: 
        job_timing_increment = int(timing_scheme_split[1])
    
    for job in jobs:
        job_timing = job_timing_increment * (job["job_id"] - 1)
        
        deltas = [job_timing] 
        for i in range(1, job["iter_count"]):
            deltas.append(0)
            
        job_timings.append({
            "deltas": deltas,
            "throttle_rates": [1.0] * job["iter_count"],    
            "job_id": job["job_id"]
        })     
    
    return job_timings


# all the jobs will start at a random time, somewhere between 0 and the period of the job.
# this is what we should assume to be happening in the real world, where the jobs are not
# synchronized in any sense. 
def random_timing(jobs, options, run_context, timing_scheme, job_profiles):
    job_timings = [] 

    for job in jobs:
        job_id = job["job_id"] 
        base_job_period = job["base_period"]
        job_timing = random.randint(0, base_job_period - 1)

        deltas = [job_timing]
        for i in range(1, job["iter_count"]):
            deltas.append(0)
            
        job_timings.append({
            "deltas": deltas,
            "throttle_rates": [1.0] * job["iter_count"],    
            "job_id": job_id
        })     
    
    return job_timings 
    

################################################################################################
################ CASINI TIMING #################################################################
################################################################################################

# we will run each job in isolation, in a network that wouldn't be bottlenecked by the core part 
# of the network. We will get the flow progress history for each job, process it and get the period.

# def get_delta_for_job_in_decisions(decisions, id):
#     for decision in decisions:
#         if decision[0] == id:
#             return decision[1]
#     return None 


def set_value_for_job_in_decisions(decisions, id, value):
    for i in range(len(decisions)):
        if decisions[i][0] == id:
            decisions[i] = (id, value)
            return
    decisions.append((id, value))


def lcm(numbers):
    def gcd(a, b):
        while b:
            a, b = b, a % b
        return a

    def lcm(a, b):
        return a * b // gcd(a, b)

    result = numbers[0]
    for i in range(1, len(numbers)):
        result = lcm(result, numbers[i])

    return result


def get_job_load_length(job_load, deltas, throttle_rates):
    job_id = job_load["job_id"]

    job_periods = [job_load["profiles"][throttle_rate]["period"] for throttle_rate in throttle_rates[job_id]]
    sum_job_periods = sum(job_periods)

    sum_job_deltas = sum(deltas[job_id])

    job_length = sum_job_deltas + sum_job_periods

    return job_length

def evaluate_candidate(job_loads, deltas, throttle_rates, run_context, 
                       link_logical_bandwidth, compat_score_mode):
    
    if len(job_loads) == 0:
        return 1
    
    # periods = [job["period"] * job["iter_count"] for job in job_loads]
    # hyperperiod = max(periods)  + max([d[1] for d in deltas])
    # eval_length = min(hyperperiod, run_context["sim-length"]) 

    job_lengths = [get_job_load_length(job_load, deltas, throttle_rates) for job_load in job_loads]   
    eval_length = max(job_lengths)  
    
    if EVAL_MODE == "cpp":
        return evaluate_candidate_cpp(job_loads, deltas, throttle_rates, run_context, 
                                      link_logical_bandwidth, compat_score_mode, 
                                      eval_length)
    
    else:
        return evaluate_candidate_python_2(job_loads, deltas, throttle_rates, run_context,    
                                             link_logical_bandwidth, compat_score_mode, 
                                             eval_length)
        
    

def evaluate_candidate_cpp(job_loads, deltas, throttle_rates, run_context, 
                           link_logical_bandwidth, compat_score_mode, eval_length):
    
    rage_quit("This function is not working. It's not returning the right values.")
    return 

    # Prepare JSON input
    input_data = {
        "job_loads": job_loads,
        "deltas": deltas,
    }
        
    # Convert the input data to JSON string
    json_input = json.dumps(input_data)
    sim_length = eval_length
    
    # TODO: it probably shouldn't be like this? 
    exec_path = './algo/evaluate/solve'
    
    # Run the C++ executable using subprocess
    process = subprocess.Popen(
        [   
            exec_path, 
            str(sim_length), 
            str(link_logical_bandwidth),
            compat_score_mode, 
        ],  
        stdin=subprocess.PIPE,       # Pipe the input
        stdout=subprocess.PIPE,      # Capture the output
        stderr=subprocess.PIPE,      # Capture errors
        text=True                    # Use text mode for strings
    )
    
    # Communicate with the process
    stdout, stderr = process.communicate(input=json_input)
    
    # Check for errors
    if process.returncode != 0:
        print(f"Error running C++ evaluator: {stderr}")
        return None
    
    # Parse and return the output
    # max_util_score, compat_score = map(float, stdout.split())
    compat_score = float(stdout) 
    
    return compat_score

def get_full_jobs_signals(this_link_loads, deltas, throttle_rates):     
    repeated_job_loads = []
    
    for job_load in this_link_loads:
        job_id = job_load["job_id"] 
        job_iter_count = job_load["iter_count"]
        
        job_total_load = np.zeros(0)
        
        for iter_id in range(job_iter_count):
            iter_time_shift = deltas[job_id][iter_id]
            iter_throttle_rate = throttle_rates[job_id][iter_id]
            
            this_job_load = job_load["profiles"][iter_throttle_rate]["load"] 
            
            job_total_load = np.append(job_total_load, np.zeros(iter_time_shift))
            job_total_load = np.append(job_total_load, this_job_load)
            
        repeated_job_loads.append(job_total_load)

    max_length = max(len(job_load) for job_load in repeated_job_loads)

    # Pad repeated and shifted job loads with zeros to make them all the same length
    padded_job_loads = [
        np.pad(job_load, (0, max_length - len(job_load)), mode='constant')
        for job_load in repeated_job_loads
    ]
    
    return padded_job_loads, max_length


def evaluate_candidate_python_2(job_loads, deltas, throttle_rates, 
                                run_context, link_logical_bandwidth, 
                                compat_score_mode, eval_length):
    
    sim_length = eval_length
    
    sum_signal = np.zeros(sim_length, dtype=np.float64)
    
    padded_job_loads, _ = get_full_jobs_signals(job_loads, deltas, throttle_rates) 
    job_loads_array = np.array(padded_job_loads)
    sum_signal = np.sum(job_loads_array, axis=0)
        
    max_util = np.max(sum_signal)
    
    compat_score = 0
    if compat_score_mode == "under-cap":
        compat_score = np.mean(sum_signal <= link_logical_bandwidth)

    elif compat_score_mode == "time-no-coll":
        
        if max_util <= link_logical_bandwidth:  
            compat_score = 1.0   
        else: 
            first_overload_index = np.argmax(sum_signal > link_logical_bandwidth)
            compat_score = first_overload_index / sim_length
            
        solution_cost = 0 
        for job_load in job_loads:
            job_id = job_load["job_id"]
            job_cost = get_solution_cost_job_load(job_load, deltas, throttle_rates)
            job_length = job_load["profiles"][1.0]["period"] * job_load["iter_count"]
            solution_cost += (job_cost / job_length)
        solution_cost = solution_cost / len(job_loads)
        
        compat_score = compat_score - solution_cost  
        
    elif compat_score_mode == "max-util-left":
        compat_score = (link_logical_bandwidth - max_util) / link_logical_bandwidth

    return compat_score
    
    
def solve_for_link(job_loads, link_logical_bandwidth, run_context, 
                   compat_score_mode, starting_iterations, 
                   current_deltas, current_throttle_rates, 
                   weights, resolved_deltas_set):  
    
    ls_candidates = run_context["cassini-parameters"]["link-solution-candidate-count"]
    ls_rand_quantum = run_context["cassini-parameters"]["link-solution-random-quantum"]
    ls_top_candidates = run_context["cassini-parameters"]["link-solution-top-candidates"]    
    
    if len(job_loads) == 0 or len(job_loads) == 1:
        same_deltas = copy.deepcopy(current_deltas)
        same_throttle_rates = copy.deepcopy(current_throttle_rates)
        return (same_deltas, same_throttle_rates)
        
    solution_scores = [] 
    involved_jobs = set([job["job_id"] for job in job_loads])
    
    for candidate_id in range(ls_candidates):
        new_deltas = copy.deepcopy(current_deltas) 
        new_throttle_rates = copy.deepcopy(current_throttle_rates)  
        
        # find a bunch of random deltas. 
        random_deltas = [] 
        random_throttle_rates = [] 
        
        max_job_period = max([job["profiles"][1.0]["period"] for job in job_loads]) 
        
        for job in job_loads: 
            job_id = job["job_id"]
            
            if run_context["timing-scheme"] == "cassini":
                throttle_rate = 1.0   
            elif run_context["timing-scheme"] == "farid":
                if "throttle-search" in run_context and run_context["throttle-search"]:
                    throttle_rate = random.choice(run_context["profiled-throttle-factors"])
                else:
                    throttle_rate = 1.0
                
            random_throttle_rates.append((job["job_id"], throttle_rate))
            
            # ####################################################
            # trying to see if we should use the max job period
            # ####################################################
            # job_period = job["profiles"][throttle_rate]["period"]
            job_period = max_job_period
            # ####################################################
             
            rand_max = job_period + weights[job_id]
            if rand_max < 0: 
                rand_max = 0
            
            quantized_rand_max = int(math.ceil((rand_max) / ls_rand_quantum))
            random_delta = random.randint(0, quantized_rand_max) * ls_rand_quantum        
            random_deltas.append((job["job_id"], random_delta)) 
            
        min_delta = min([x[1] for x in random_deltas])
        random_deltas = [(x[0], x[1] - min_delta) for x in random_deltas]
        
        # if some of the jobs are already resolved, we will discard the random deltas for them.        
        number_of_fixed_decisions = 0 
        for job_id in involved_jobs:     
            if job_id in resolved_deltas_set:
                
                number_of_fixed_decisions += 1
                iter = starting_iterations[job_id]
        
                if iter < len(new_deltas[job_id]):
                    set_value_for_job_in_decisions(random_deltas, job_id, 
                                                   new_deltas[job_id][iter])
                    
                    set_value_for_job_in_decisions(random_throttle_rates, job_id, 
                                                   new_throttle_rates[job_id][iter])
                    
        if number_of_fixed_decisions == len(involved_jobs):
            same_deltas = copy.deepcopy(current_deltas)
            same_throttle_rates = copy.deepcopy(current_throttle_rates) 
            return (same_deltas, same_throttle_rates) # no need to evaluate this.
        
        for job_id, delta in random_deltas:
            iter = starting_iterations[job_id]
            if iter < len(new_deltas[job_id]):
                new_deltas[job_id][iter] = delta
        
        for job_id, throttle_rate in random_throttle_rates: 
            iter = starting_iterations[job_id]
            if iter < len(new_throttle_rates[job_id]):
                new_throttle_rates[job_id][iter] = throttle_rate
        
        compat_score = evaluate_candidate(job_loads, new_deltas, new_throttle_rates, 
                                          run_context, link_logical_bandwidth, 
                                          compat_score_mode)

        # print("new_deltas: ", new_deltas, "compat_score: ", compat_score)   
        solution_scores.append((new_deltas, new_throttle_rates, compat_score))

    good_solutions = sorted(solution_scores, key=lambda x: x[2], reverse=True)
    top_candidates = good_solutions[:ls_top_candidates]
    r = random.randint(0, len(top_candidates) - 1)
    top_solution = top_candidates[r]
    
    return top_solution[0], top_solution[1]

def get_link_loads(jobs, options, run_context, job_profiles):
    servers_per_rack = options["ft-server-per-rack"]
    rack_count = options["machine-count"] // servers_per_rack   
    link_bandwidth = options["link-bandwidth"]  
    
    link_loads = [] 
    cross_rack_jobs_set = set() 
    cross_rack_jobs = []    
    
    def add_signal_to_sum(signal, sum_signal):
        for i in range(len(signal)):
            if i >= len(sum_signal):
                sum_signal.append(0)
            sum_signal[i] += signal[i]
            
    for i in range(rack_count):
        this_rack = {"up": [], "down": []}
        link_loads.append(this_rack)

        for dir in ["up", "down"]:
            
            for job in jobs:
                job_id = job["job_id"]
                throttled_job_profiles = {}
                any_flow_added = False
                
                for throttle_factor in run_context["profiled-throttle-factors"]:
                    if job_id not in job_profiles:
                        continue    
                    
                    job_profile = job_profiles[job_id][throttle_factor]
                    if len(job_profile["flows"]) == 0:
                        continue 

                    job_period = job_profile["period"]  
                    # for flow in job_profile["flows"]:   
                    #     for i in range(len(flow["progress_history"])):
                    #         flow["progress_history"][i] /= link_bandwidth
                    
                    # this job will add some load to each of the links. 
                    # all the flows for this job will be added up. 
                    link_job_load_combined = [] 
                    
                    for flow in job_profile["flows"]:
                        flow_src_rack = flow["srcrack"]
                        flow_dst_rack = flow["dstrack"]
                        
                        flow_progress_history = flow["progress_history"].copy()    
                        
                        for t in range(len(flow_progress_history)): 
                            flow_progress_history[t] /= link_bandwidth

                        # print flow start and end and src and dst 
                        if ((dir == "up" and flow_src_rack == i) or 
                            (dir == "down" and flow_dst_rack == i)):    
                            
                            any_flow_added = True 
                            if len(link_job_load_combined) > 0:
                                assert len(link_job_load_combined) == len(flow_progress_history)
                                
                            # this is a flow that goes through this link
                            add_signal_to_sum(flow_progress_history,
                                              link_job_load_combined)
                            
                    if any_flow_added:
                        throttled_job_profiles[throttle_factor] = {
                            "load": link_job_load_combined, 
                            "period": job_period,
                            "max": max(link_job_load_combined),
                        }
                    else:                         
                        # if there were no flows for this throttle factor, 
                        # there won't be any flows for the other throttle factors.
                        # so we can break out of the loop.
                        break 
                    
                if any_flow_added:
                    cross_rack_jobs_set.add(job_id)
                    
                    link_loads[i][dir].append({
                        "link_id": i * 2 + (1 if dir == "up" else 0),   
                        "job_id": job_id,
                        "iter_count": job["iter_count"],  
                        "profiles": throttled_job_profiles
                    })
    
    cross_rack_jobs = list(cross_rack_jobs_set) 
    return link_loads, cross_rack_jobs


def get_link_loads_runtime(jobs, options, run_context, summarized_job_profiles):
    servers_per_rack = options["ft-server-per-rack"]
    rack_count = options["machine-count"] // servers_per_rack   
    link_bandwidth = options["link-bandwidth"]  
    
    link_loads = [] 
    cross_rack_jobs_set = set() 
    cross_rack_jobs = []    
    
    def add_signal_to_sum(signal, sum_signal):
        for i in range(len(signal)):
            if i >= len(sum_signal):
                sum_signal.append(0)
            sum_signal[i] += signal[i]
            
    for i in range(rack_count):
        this_rack = {"up": [], "down": []}
        link_loads.append(this_rack)
        for dir in ["up", "down"]:
            for job in jobs:
                job_id = job["job_id"]
                any_flow_added = False
                
                if job_id not in summarized_job_profiles:
                    continue    
                job_profile = summarized_job_profiles[job_id]
                
                if len(job_profile["flows"]) == 0:
                    continue 

                job_period = job_profile["period"]  

                link_job_load_combined = [] 
                
                for flow in job_profile["flows"]:
                    flow_src_rack = flow["srcrack"]
                    flow_dst_rack = flow["dstrack"]
                    
                    # history is a list of tuples: (rate, count)
                    flow_progress_history_summary = []
                    
                    for rate, count in flow["progress_history_summarized"]: 
                        rate /= link_bandwidth  
                        flow_progress_history_summary.append((rate, count))
                    
                    # print flow start and end and src and dst 
                    if ((dir == "up" and flow_src_rack == i) or 
                        (dir == "down" and flow_dst_rack == i)):    
                        
                        any_flow_added = True 
                        
                        current_begin = 0
                        for history_item in flow_progress_history_summary:
                            rate = history_item[0]
                            count = history_item[1]
                                
                            if len(link_job_load_combined) < current_begin + count:
                                link_job_load_combined += [0] * (current_begin + count - len(link_job_load_combined))
                            
                            if rate > 0:
                                for j in range(count):  
                                    link_job_load_combined[current_begin + j] += rate
                            
                            current_begin += count                              
                        
                if any_flow_added:   
                    cross_rack_jobs_set.add(job_id)
                    
                    link_loads[i][dir].append({
                        "link_id": i * 2 + (1 if dir == "up" else 0),   
                        "job_id": job_id,
                        "load": link_job_load_combined, 
                        "period": job_period,
                        "max": max(link_job_load_combined),
                    })
    
    cross_rack_jobs = list(cross_rack_jobs_set) 
    return link_loads, cross_rack_jobs


job_colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k']
job_color_index = 0 

def get_job_color(job_id, assigned_job_colors): 
    global job_color_index
    
    if job_id in assigned_job_colors:
        return assigned_job_colors[job_id]
    else: 
        assigned_job_colors[job_id] = job_colors[job_color_index]
        job_color_index += 1
        job_color_index = job_color_index % len(job_colors) 
        return assigned_job_colors[job_id] 
    
    
def visualize_link_loads(link_loads, run_context, 
                         deltas, throttle_rates,
                         link_logical_bandwidth = None, 
                         suffix=""): 
    
    if "visualize-timing" not in run_context or not run_context["visualize-timing"]:    
        return  

    import matplotlib.pyplot as plt
    import numpy as np
    import os

    num_racks = len(link_loads)
    num_directions = 2  # "up" and "down"

    # Create a figure and subplots
    fig, axes = plt.subplots(num_racks, num_directions, figsize=(10, 3 * num_racks), squeeze=False, sharex=True)

    global job_color_index
    job_color_index = 0
    assigned_job_colors = {} 

    min_over_capacity_time = 1e9 
    
    for rack in range(num_racks):
        for i, direction in enumerate(["up", "down"]):
            ax = axes[rack][i]
            ax.set_title(f"Rack: {rack}, Direction: {direction}")
            ax.set_xlabel("Time")
            ax.set_ylabel("Load")
            
            if len(link_loads[rack][direction]) == 0:
                ax.text(0.5, 0.5, "No jobs", horizontalalignment='center', verticalalignment='center', transform=ax.transAxes)  
                continue
            
            padded_job_loads, max_length = get_full_jobs_signals(link_loads[rack][direction], deltas, throttle_rates)
            job_ids = [job_load["job_id"] for job_load in link_loads[rack][direction]]
            
            sum_max_job_load = 0    
            for job_load in link_loads[rack][direction]:
                max_job_load = 0 
                for throttle_rate in run_context["profiled-throttle-factors"]:
                    max_job_load = max(max_job_load, job_load["profiles"][throttle_rate]["max"])
                sum_max_job_load += max_job_load    
                
            # Convert the padded job loads to a 2D array
            job_loads_array = np.array(padded_job_loads)

            ax.stackplot(range(max_length), job_loads_array, 
                         labels=[f"Job: {job_id}" for job_id in job_ids], 
                         colors=[get_job_color(job_id, assigned_job_colors) for job_id in job_ids])

            ax.set_ylim(0, sum_max_job_load * 1.1)

            if link_logical_bandwidth is not None:  
                ax.axhline(y=link_logical_bandwidth, color='r', linestyle='--') 
                
                # find the first place that the link goes over the logical bandwidth.
                sum_signal = np.sum(job_loads_array, axis=0)
                first_overload_index = np.argmax(sum_signal > link_logical_bandwidth)
                if max(sum_signal) > link_logical_bandwidth:
                    min_over_capacity_time = min(min_over_capacity_time, first_overload_index) 
                    
                    ax.axvline(x=first_overload_index, color='black', linestyle='--', linewidth=1)
            
            max_value_in_stack = np.max(np.sum(job_loads_array, axis=0)) 
            
            ax.axhline(y=max_value_in_stack, color='blue', linestyle='--') 

            # ax.legend(loc='upper left')

    # create one legend for all the subplots. with the contents of the color assignment to jobs.
    if len(assigned_job_colors) > 0:
        handles = [plt.Line2D([0], [0], marker='o', color='w', markerfacecolor=assigned_job_colors[job_id], label=f"Job: {job_id}") for job_id in assigned_job_colors]
        fig.legend(handles=handles, loc='upper right')
    
    if min_over_capacity_time < 1e9:
        for rack in range(num_racks):
            for i, direction in enumerate(["up", "down"]):
                ax = axes[rack][i]
                ax.axvline(x=min_over_capacity_time, color='black', linestyle=':', linewidth=3)     
            
    plt.tight_layout()
    timing_plots_dir = f"{run_context['timings-dir']}/"
    os.makedirs(timing_plots_dir, exist_ok=True)
    plot_path = f"{timing_plots_dir}/demand{suffix}.png"  
    plt.savefig(plot_path, bbox_inches='tight', dpi=300)    
    plt.close(fig)



def visualize_link_loads_runtime(link_loads, run_context, 
                                 suffix="", plot_dir=None, 
                                 smoothing_window=1):      
        
    if "visualize-timing" not in run_context or not run_context["visualize-timing"]:    
        return  

    import matplotlib.pyplot as plt
    import numpy as np
    import os

    num_racks = len(link_loads)
    num_directions = 2  # "up" and "down"


    # Create a figure and subplots
    fig, axes = plt.subplots(num_racks, num_directions, figsize=(10, 3 * num_racks), 
                             squeeze=False, sharex=True)

    global job_color_index
    job_color_index = 0
    assigned_job_colors = {} 
    min_over_capacity_time = 1e9 
    
    for rack in range(num_racks):
        for i, direction in enumerate(["up", "down"]):
            ax = axes[rack][i]
            ax.set_title(f"Rack: {rack}, Direction: {direction}")
            ax.set_xlabel("Time")
            ax.set_ylabel("Load")
            
            if len(link_loads[rack][direction]) == 0:
                ax.text(0.5, 0.5, "No jobs", horizontalalignment='center', 
                        verticalalignment='center', transform=ax.transAxes)  
                continue
            
            repeated_job_loads = []
            for job_load in link_loads[rack][direction]:
                job_id = job_load["job_id"] 
                job_total_load = job_load["load"]
                repeated_job_loads.append(job_total_load)

            max_length = max(len(job_load) for job_load in repeated_job_loads)

            # Pad repeated and shifted job loads with zeros to make them all the same length
            padded_job_loads = [
                np.pad(job_load, (0, max_length - len(job_load)), mode='constant')
                for job_load in repeated_job_loads
            ]
            
            job_ids = [job_load["job_id"] for job_load in link_loads[rack][direction]]
            
            sum_max_job_load = 0
            for job_load in link_loads[rack][direction]:
                max_job_load = max(job_load["load"])
                sum_max_job_load += max_job_load
                
            # Convert the padded job loads to a 2D array
            job_loads_array = np.array(padded_job_loads)

            # smooth the signal
            if smoothing_window > 1:
                smoothed_job_loads = []  
                for job_load in job_loads_array:
                    # np.convolve 
                    smoothed = np.convolve(job_load, np.ones(smoothing_window) / smoothing_window, mode='same') 
                    smoothed_job_loads.append(smoothed)
                
                job_loads_array = np.array(smoothed_job_loads)
                            
            ax.stackplot(range(max_length), job_loads_array, 
                         labels=[f"Job: {job_id}" for job_id in job_ids], 
                         colors=[get_job_color(job_id, assigned_job_colors) for job_id in job_ids])

            ax.set_ylim(0, sum_max_job_load * 1.1)
            max_value_in_stack = np.max(np.sum(job_loads_array, axis=0)) 
            ax.axhline(y=max_value_in_stack, color='blue', linestyle='--') 
            # ax.legend(loc='upper left')

    # create one legend for all the subplots. with the contents of the color assignment to jobs.
    if len(assigned_job_colors) > 0:
        handles = [plt.Line2D([0], [0], marker='o', color='w', markerfacecolor=assigned_job_colors[job_id], label=f"Job: {job_id}") for job_id in assigned_job_colors]
        fig.legend(handles=handles, loc='upper right')
    
    if min_over_capacity_time < 1e9:
        for rack in range(num_racks):
            for i, direction in enumerate(["up", "down"]):
                ax = axes[rack][i]
                ax.axvline(x=min_over_capacity_time, color='black', linestyle=':', linewidth=3)     
            
    plt.tight_layout()
    plot_path = f"{plot_dir}/demand{suffix}.png"
    plt.savefig(plot_path, bbox_inches='tight', dpi=300)    
    plt.close(fig)
    
    
def get_good_until(jobs, link_loads_list, run_context, deltas, throttle_rates, link_logical_bandwidth):    
    min_first_overload_index = 1e9  
    max_length_across_links = 0 
    
    for link_load in link_loads_list: 
        if len(link_load) == 0:
            continue
          
        padded_job_loads, max_length = get_full_jobs_signals(link_load, deltas, throttle_rates)
        max_length_across_links = max(max_length_across_links, max_length)
        # Convert the padded job loads to a 2D array
        job_loads_array = np.array(padded_job_loads)
        sum_signal = np.sum(job_loads_array, axis=0)
        first_overload_index = np.argmax(sum_signal > link_logical_bandwidth)
        if max(sum_signal) > link_logical_bandwidth:
            min_first_overload_index = min(min_first_overload_index, first_overload_index)  
                 
    if min_first_overload_index == 1e9:
        min_first_overload_index = max_length_across_links  

    # so everything is good until the min_first_overload_index.
    # everything is good until the min_first_overload_index.
    # what's the iteration that corresponds to this index?
    
    # print(f"min_first_overload_index: {min_first_overload_index}")  
    
    good_until = {} 
    # print("min_first_overload_index: ", min_first_overload_index)   
    
    for job in jobs: 
        good_until[job["job_id"]] = -1
        job_id = job["job_id"] 
        job_iter_count = job["iter_count"] 

        # the job will be delta, period, delta, period, delta, period, ...  
        current_time = 0    
        
        for iter_id in range(job_iter_count):
            iter_throttle_rate = throttle_rates[job_id][iter_id] 
            
            job_iter_period = job["period"][str(iter_throttle_rate)]
            current_time += deltas[job_id][iter_id]
            current_time += job_iter_period 
            
            if current_time > min_first_overload_index:
                break
            
            good_until[job_id] = iter_id 
        
        # print(f"job_id: {job_id}, job_period: {job_period}, job_iter_count: {job_iter_count}, good_until: {good_until[job_id]}")      
    
    return good_until
    
def evaluate_candidate_all_links(jobs, link_loads, deltas, throttle_rates, run_context, 
                                 link_logical_bandwidth, compat_score_mode, 
                                 cross_rack_jobs): 
    # pprint(deltas)  
    
    # now we have a candidate solution. We should evaluate it. 
    if compat_score_mode == "time-no-coll":
        candidate_score = 1 
    elif compat_score_mode == "max-util-left":    
        candidate_score = 0
    elif compat_score_mode == "under-cap":
        candidate_score = 0
                
    for link_load in link_loads:    
        compat_score = evaluate_candidate(job_loads=link_load, 
                                            deltas=deltas, 
                                            throttle_rates=throttle_rates,
                                            run_context=run_context,
                                            compat_score_mode=compat_score_mode,
                                            link_logical_bandwidth=link_logical_bandwidth)

        if compat_score_mode == "max-util-left": 
            candidate_score += compat_score
        elif compat_score_mode == "under-cap": 
            candidate_score += compat_score 
        elif compat_score_mode == "time-no-coll":
            # print("candidate_score: ", candidate_score, "compat_score: ", compat_score) 
            candidate_score = min(compat_score, candidate_score)   

    
    # if compat_score_mode == "time-no-coll":
    #     total_cost = 0 
    #     for job in jobs:
    #         job_id = job["job_id"]
    #         if job_id in cross_rack_jobs:   
    #             job_cost = get_solution_cost(job, deltas, throttle_rates)
    #             job_length = job["base_period"] * job["iter_count"]
                
    #             total_cost += job_cost / job_length 
                
    #     delay_penalty = total_cost / len(cross_rack_jobs)
        
    #     print("candidate_score: ", candidate_score, 
    #           "delay_penalty: ", delay_penalty, 
    #           "candidate_score - delay_penalty: ", candidate_score - delay_penalty)
        
    #     candidate_score -= delay_penalty
    
    return candidate_score  

def get_solution_cost_job(job, deltas, throttle_rates):    
    job_id = job["job_id"]
    
    total_deltas_cost = sum(deltas[job_id]) 
    
    total_throttle_cost = 0
    for throttle_rate in throttle_rates[job_id]:    
        period = job["period"][str(throttle_rate)]
        base_period = job["base_period"]       
        
        throttle_cost = period - base_period    
        total_deltas_cost += throttle_cost
        
    job_cost = total_deltas_cost + total_throttle_cost 
    
    return job_cost

def get_solution_cost_job_load(job_load, deltas, throttle_rates):   
    job_id = job_load["job_id"]
    
    total_deltas_cost = sum(deltas[job_id]) 
    
    total_throttle_cost = 0
    for throttle_rate in throttle_rates[job_id]:    
        period = job_load["profiles"][throttle_rate]["period"]
        base_period = job_load["profiles"][1.0]["period"]          
        
        throttle_cost = period - base_period    
        total_deltas_cost += throttle_cost
        
    job_cost = total_deltas_cost + total_throttle_cost 
    
    return job_cost
    
def get_timeshifts(jobs, options, run_context, job_profiles, 
                   starting_iterations=None, base_deltas=None, 
                   weights=None, base_throttle_rates=None, round=0):       

    # check if the required parameters are there.    
    if (starting_iterations is None or 
            base_deltas is None or 
            weights is None or 
            base_throttle_rates is None): 
         
        base_deltas = {}
        base_throttle_rates = {}
        starting_iterations = {}    
        weights = {} 

        for job in jobs:
            job_id = job["job_id"]
    
            base_deltas[job_id] = [0] * job["iter_count"]
            base_throttle_rates[job_id] = [1.0] * job["iter_count"]
            starting_iterations[job_id] = 0
            weights[job_id] = 0
    
    if "compat-score-mode" not in run_context:
        rage_quit("compat-score-mode is required in run_context")
    
    compat_score_mode = run_context["compat-score-mode"]
    overall_solution_candidate_count = run_context["cassini-parameters"]["overall-solution-candidate-count"]
    servers_per_rack = options["ft-server-per-rack"]
    rack_count = options["machine-count"] // servers_per_rack       
    link_logical_bandwidth = options["ft-core-count"] * options["ft-agg-core-link-capacity-mult"]
    
    start_time = time.time()
    
    link_loads, cross_rack_jobs = get_link_loads(jobs, options, run_context, job_profiles)   

    # visualize_link_loads(link_loads, run_context, base_deltas, base_throttle_rates,
    #                      link_logical_bandwidth=link_logical_bandwidth, 
    #                      suffix=f"_round_{round}_base")
    
    best_candidate_score = -1e9 
    best_candidate_deltas = None
    best_candidate_throttle_rates = None 
    best_candidate_good_until = None     
    
    all_scores = [] 
     
    link_loads_list = [] 
    for rack in range(rack_count):
        for direction in ["up", "down"]:
            link_loads_list.append(link_loads[rack][direction])
    
    for i in range(overall_solution_candidate_count):
        resolved_deltas_set = set() 
    
        for job in jobs: 
            job_id = job["job_id"]
            if job_id in cross_rack_jobs:
                if starting_iterations[job_id] == job["iter_count"]:
                    resolved_deltas_set.add(job_id)
        
        current_deltas = copy.deepcopy(base_deltas)
        current_throttle_rates = copy.deepcopy(base_throttle_rates)   

        # shuffle the link_solutions. No real difference beetwen the links.
        random.shuffle(link_loads_list)
        
        # go through the rest of the links. 
        for j in range(0, len(link_loads_list)):
            this_link_loads = link_loads_list[j]
            
            solution = solve_for_link(this_link_loads, link_logical_bandwidth, 
                                      run_context, compat_score_mode,
                                      starting_iterations=starting_iterations, 
                                      current_deltas=current_deltas, 
                                      current_throttle_rates=current_throttle_rates,
                                      weights=weights, 
                                      resolved_deltas_set=resolved_deltas_set)    
             
            current_deltas, current_throttle_rates = solution
            
            
            for job_load in this_link_loads:
                resolved_deltas_set.add(job_load["job_id"]) 
                
            if len(resolved_deltas_set) > len(cross_rack_jobs): 
                rage_quit("resolved_deltas_set is bigger than cross_rack_jobs. what's going on?")
                   
            if len(resolved_deltas_set) == len(cross_rack_jobs):
                break
            
            # log_results(run_context, f"link_solutions_{j}_candidate_{i}", current_deltas)
        
        
        # visualize_link_loads(link_loads, run_context, current_deltas, current_throttle_rates,   
        #                      link_logical_bandwidth=link_logical_bandwidth, 
        #                      suffix=f"_{round}_{i}")    
        candidate_score = evaluate_candidate_all_links(jobs, link_loads_list, current_deltas, current_throttle_rates, 
                                                       run_context, link_logical_bandwidth, 
                                                       compat_score_mode, cross_rack_jobs)
    
        
        # log_results(run_context, "candidate", (current_deltas, candidate_score))
        good_until = get_good_until(jobs, link_loads_list, run_context, 
                                    current_deltas, current_throttle_rates,   
                                    link_logical_bandwidth=link_logical_bandwidth)
        
        all_scores.append(candidate_score)
        
        if candidate_score > best_candidate_score:
            best_candidate_score = candidate_score
            best_candidate_deltas = current_deltas
            best_candidate_throttle_rates = current_throttle_rates
            best_candidate_good_until = good_until

    visualize_link_loads(link_loads, run_context, best_candidate_deltas, 
                         best_candidate_throttle_rates,    
                         link_logical_bandwidth=link_logical_bandwidth, 
                         suffix=f"_round_{round}_best")

    job_timings = [] 
    log_results(run_context, "best_candidate", (best_candidate_deltas, 
                                                best_candidate_throttle_rates, 
                                                best_candidate_score))
    
    for job in jobs:
        job_id = job["job_id"]
        if job_id in best_candidate_deltas:
            deltas = best_candidate_deltas[job_id]
            throttle_rates = best_candidate_throttle_rates[job_id] 
        else:
            deltas = [0] * job["iter_count"]
            throttle_rates = [1.0] * job["iter_count"]      
            
        job_timings.append({
            "deltas": deltas,
            "throttle_rates": throttle_rates,   
            "job_id": job_id
        })           

    end_time = time.time() 
    time_taken = end_time - start_time 
    log_results(run_context, "time_taken", time_taken)  
        
    return job_timings, best_candidate_good_until  

def load_job_profiles(jobs, run_context): 
    job_profiles = {} 
    
    for job in jobs: 
        job_id = job["job_id"]
        job_profiles[job_id] = {}

        for throttle_factor in run_context["profiled-throttle-factors"]:
            profiles_dir = run_context["profiles-dir"]
            path = f"{profiles_dir}/{job_id}_{throttle_factor}.pkl"
            
            # check if the file exists.
            if not os.path.exists(path):
                job_profiles[job_id][throttle_factor] = None
                continue 
                    
            with open(path, "rb") as f:  
                job_profiles[job_id][throttle_factor] = pkl.load(f)
    
    return job_profiles
        

def cassini_timing(jobs, options, run_context, timing_scheme, job_profiles):
    job_timings, good_until = get_timeshifts(jobs, options, run_context, job_profiles)

    return job_timings

################################################################################################
################ Farid TIMING #################################################################
################################################################################################

def get_job_iter_finish(period, deltas, iter_id):
    finish = 0
    for i in range(iter_id + 1):
        finish += period + deltas[i]
    return finish

def get_extended_time_shifts(jobs, options, run_context, job_profiles):
    base_deltas = {}
    base_throttle_rates = {}
    starting_iterations = {}    
    weights = {} 
    
    for job in jobs:
        job_id = job["job_id"]

        base_deltas[job_id] = [0] * job["iter_count"]
        base_throttle_rates[job_id] = [1.0] * job["iter_count"]
        weights[job_id] = 0
        starting_iterations[job_id] = 0
            
    rounds_no_progress = 0 
    for i in range(run_context["farid-rounds"]):    
        sys.stderr.write("starting round: {}".format(i))
        any_progress = False
        
        job_timings, good_until = get_timeshifts(jobs, options, run_context, job_profiles, 
                                                 starting_iterations=starting_iterations, 
                                                 base_deltas=base_deltas, weights=weights, 
                                                 base_throttle_rates=base_throttle_rates, round=i) 
        
        for job in jobs:
            job_id = job["job_id"]
            
            if starting_iterations[job_id] < good_until[job_id] + 1:
                any_progress = True
                
            starting_iterations[job_id] = good_until[job_id] + 1    
            
            for timing in job_timings:
                if timing["job_id"] == job_id:
                    base_deltas[job_id] = timing["deltas"]
                    base_throttle_rates[job_id] = timing["throttle_rates"]
                    break  
            
            # print("job_id: {}, good until: {}, sum_deltas: {}, period: {}, missed iters: {}".format(
            #         job_id, good_until[job_id], sum_deltas[job_id], job["period"], 
            #         int(math.ceil(sum_deltas[job_id] / job["period"]))))
        
        if not any_progress:
            rounds_no_progress += 1
            log_results(run_context, "round_no_progress", rounds_no_progress)
        else:   
            rounds_no_progress = 0
        
        job_costs = {} 
        for job in jobs:
            job_id = job["job_id"]  
            job_costs[job_id] = get_solution_cost_job(job, base_deltas, base_throttle_rates)
            sys.stderr.write("job_id: {}, job_cost: {}, job_period: {}, job_iter_count: {}".format(
                job_id, job_costs[job_id], job["period"], job["iter_count"]))
            
        avg_job_cost = int(sum(job_costs.values()) / len(jobs))   
                
        for job in jobs:
            job_id = job["job_id"]
            weights[job_id] = avg_job_cost - job_costs[job_id]
            
        log_results(run_context, "job_costs", job_costs)
        log_results(run_context, "weights", weights)    
        
        are_we_done = True 
        for job in jobs:    
            if starting_iterations[job["job_id"]] < job["iter_count"]:
                are_we_done = False 
                break
        if are_we_done:
            break

    log_results(run_context, "job_timings", job_timings)
    # pprint(job_timings)
    
    return job_timings 

def farid_timing(jobs, options, run_context, timing_scheme, job_profiles):
    # step 2: run cassini timing with the job profiles, find some timings for the jobs.  
    job_timings = get_extended_time_shifts(jobs, options, run_context, job_profiles)
    
    # step 4: return the full schedule.  
    return job_timings

############################################################################
################ MAIN FUCTION  #############################################
############################################################################

def get_job_timings(jobs, options, run_context, job_profiles, ):
    if "timing-scheme" not in run_context:
        raise ValueError("timing-scheme option is required")

    timing_scheme = run_context["timing-scheme"]
    # call the right function to do the timing schedule.
    timing_funcions = {
        "inc": inc_timing,
        "random": random_timing,
        "zero": zero_timing, 
        "cassini": cassini_timing, 
        "farid": farid_timing, 
    }
    if timing_scheme.split("_")[0] not in timing_funcions:
        raise ValueError(f"Invalid timing-scheme: {timing_scheme}")
    timing_func = timing_funcions[timing_scheme.split("_")[0]] 
    job_timings = timing_func(jobs, options, run_context, timing_scheme, job_profiles)

    return job_timings

def get_job_routings(jobs, options, run_context, job_profiles, job_timings):    
    lb_scheme = options["lb-scheme"]
     
    if lb_scheme == "readprotocol":
        lb_decisions = route_flows(jobs, options, run_context, job_profiles, job_timings)
    else: 
        lb_decisions = None
        
    return lb_decisions
    
def generate_timing_file(timing_file_path, routing_file_path, placement_seed, 
                         jobs, options, run_context):

    random.seed(run_context["experiment-seed"] + placement_seed)

    # load the job profiles. Might be a bit unnecassary in some cases, but anyway. 
    job_profiles = load_job_profiles(jobs, run_context)
    
    # do the timing.
    job_timings = get_job_timings(jobs, options, run_context, job_profiles)
    
    # do the routing.   
    lb_decisions = get_job_routings(jobs, options, run_context, job_profiles, job_timings)   

    # visualize
    visualize_final_timing(jobs, options, run_context, job_timings, job_profiles, lb_decisions) 
        
    # writing the results to the files. 
    with open(timing_file_path, "w") as f:
        json.dump(job_timings, f, indent=4)
        f.flush() 
    
    with open(routing_file_path, "w") as f:
        if lb_decisions is not None:    
            json.dump(lb_decisions, f, indent=4)
            f.flush()   
        else: 
            f.write("[]")
            f.flush()   
        
    # returning the results just in case as well. 
    return job_timings, lb_decisions    

if __name__ == "__main__":
    
    input_data = json.load(sys.stdin)
    # input_data = {"timing_file_path": "results/sweep/2384-nethint_LB+random_TS+_R+_2_67/custom_files//timings/random-random/12/farid/first/time-no-coll//timing.txt", "routing_file_path": "results/sweep/2384-nethint_LB+random_TS+_R+_2_67/custom_files//routings/random-random/12/farid/first/time-no-coll/routing.txt", "placement_seed": 12, "jobs": [{"job_id": 1, "machine_count": 14, "comm_size": 2000, "comp_size": 200, "layer_count": 1, "iter_count": 16, "machines": [22, 12, 48, 25, 41, 34, 26, 49, 10, 9, 55, 18, 6, 24], "period": 452}, {"job_id": 2, "machine_count": 15, "comm_size": 4000, "comp_size": 100, "layer_count": 1, "iter_count": 27, "machines": [40, 59, 5, 51, 47, 58, 39, 8, 11, 43, 33, 60, 45, 38, 56], "period": 284}, {"job_id": 3, "machine_count": 13, "comm_size": 4000, "comp_size": 100, "layer_count": 2, "iter_count": 15, "machines": [63, 37, 20, 28, 44, 42, 23, 54, 19, 32, 1, 46, 27], "period": 496}, {"job_id": 4, "machine_count": 16, "comm_size": 4000, "comp_size": 400, "layer_count": 2, "iter_count": 3, "machines": [3, 31, 35, 0, 61, 52, 4, 29, 57, 53, 30, 17, 13, 50, 16, 2], "period": 1690}, {"job_id": 5, "machine_count": 6, "comm_size": 8000, "comp_size": 200, "layer_count": 2, "iter_count": 7, "machines": [36, 14, 7, 21, 15, 62], "period": 940}], "options": {"step-size": 1, "core-status-profiling-interval": 100000, "rep-count": 1, "console-log-level": 4, "file-log-level": 1, "initial-rate": 100, "min-rate": 100, "drop-chance-multiplier": 0, "rate-increase": 1, "priority-allocator": "maxmin", "network-type": "leafspine", "link-bandwidth": 100, "ft-rack-per-pod": 1, "ft-agg-per-pod": 1, "ft-pod-count": -1, "ft-server-tor-link-capacity-mult": 1, "ft-tor-agg-link-capacity-mult": 1, "ft-agg-core-link-capacity-mult": 1, "shuffle-device-map": False, "regret-mode": "none", "machine-count": 64, "ft-server-per-rack": 16, "simulation-seed": 67, "print-flow-progress-history": True, "protocol-file-name": "nethint-test", "lb-scheme": "readprotocol", "subflows": 1, "ft-core-count": 8, "workers-dir": "/home/faridzandi/git/psim/run/workers/", "load-metric": "flowsize", "placement-file": "results/sweep/2384-nethint_LB+random_TS+_R+_2_67/custom_files//placements/random-random/12//placement.txt"}, "run_context": {"sim-length": 8000, "visualize-timing": False, "visualize-routing": False, "random-rep-count": 1, "interesting-metrics": ["avg_ar_time", "avg_iter_time"], "all-placement-modes": ["random"], "experiment-seed": 67, "oversub": 2, "cassini-parameters": {"link-solution-candidate-count": 50, "link-solution-random-quantum": 10, "link-solution-top-candidates": 3, "overall-solution-candidate-count": 10, "save-profiles": True}, "routing-parameters": {}, "selected-setting": {"machine-count": 64, "ft-server-per-rack": 16, "jobs-machine-count-low": 12, "jobs-machine-count-high": 16, "placement-seed-range": 40, "comm-size": [8000, 4000, 2000], "comp-size": [200, 100, 400], "layer-count": [1, 2], "iter-count": [30]}, "comparison-base": {"timing-scheme": "random", "ring-mode": "random", "lb-scheme": "random"}, "comparisons": [["farid", {"timing-scheme": "farid", "lb-scheme": "random"}], ["ideal", {"lb-scheme": "ideal", "timing-scheme": "random"}]], "exp-uuid": 1, "worker-id-for-profiling": 0, "output-file": "results/sweep/2384-nethint_LB+random_TS+_R+_2_67/exp_outputs/output-1.txt", "perfect_lb": False, "ideal_network": False, "farid_timing": True , "original_mult": 1, "original_core_count": 8, "original_lb_scheme": "random", "original_ring_mode": "random", "original_timing_scheme": "farid", "routing-fit-strategy": "first", "compat-score-mode": "time-no-coll", "placement-mode": "random", "ring-mode": "random", "placement-seed": 12, "timing-scheme": "farid", "placements_dir": "results/sweep/2384-nethint_LB+random_TS+_R+_2_67/custom_files//placements/random-random/12/", "profiles-dir": "results/sweep/2384-nethint_LB+random_TS+_R+_2_67/custom_files//profiles/random-random/12/", "timings-dir": "results/sweep/2384-nethint_LB+random_TS+_R+_2_67/custom_files//timings/random-random/12/farid/first/time-no-coll/", "routings-dir": "results/sweep/2384-nethint_LB+random_TS+_R+_2_67/custom_files//routings/random-random/12/farid/first/time-no-coll"}}
    # call the main function
    job_timings, lb_decisions = generate_timing_file(**input_data)
    
    dumped_data = {
        "job_timings": job_timings, 
        "lb_decisions": lb_decisions
    } 
    
    # write the output to stdout
    print(json.dumps(dumped_data))
    
    
    