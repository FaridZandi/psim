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
import matplotlib.pyplot as plt
from datetime import datetime

from algo.newtiming import LegoSolver, LegoV2Solver   


####################################################################################
##################  HELPER FUNCTIONS  ##############################################
####################################################################################

def log_results(run_context, key, value):
    # print to stderr first  
    sys.stderr.write(f"KEY: {key}\n")
    sys.stderr.write(f"VALUE: {value}\n")   

    with open(run_context["output-file"], "a+") as f:
        f.write("Results for: " + key )
        pprint(value, f) 
        f.write("\n---------------------------------\n")   


def visualize_workload_timing(jobs, options, run_context, 
                              job_timings, job_profiles, lb_decisions, 
                              mode):
    
    link_loads, cross_rack_jobs = get_link_loads(jobs, options, run_context, job_profiles)
    deltas = {}
    throttle_rates = {} 
    suffix = ""
    
    if mode == "final":
        suffix = "_final"
        for job_timing in job_timings:
            deltas[job_timing["job_id"]] = job_timing["deltas"]
            throttle_rates[job_timing["job_id"]] = job_timing["throttle_rates"]
    elif mode == "initial":
        suffix = "_initial"
        # job_timings is None. we want to visualize with all zeros. 
        for job in jobs:
            deltas[job["job_id"]] = [0] * job["iter_count"]
            throttle_rates[job["job_id"]] = [1.0] * job["iter_count"]   
    
    link_logical_bandwidth = options["ft-core-count"] * options["ft-agg-core-link-capacity-mult"]
    
    for sep in [True, False]:    
        visualize_link_loads(link_loads, run_context, deltas=deltas, 
                            throttle_rates=throttle_rates,
                            link_logical_bandwidth=link_logical_bandwidth, 
                            suffix=suffix, separate_plots=sep)

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
    
    job_lengths = [get_job_load_length(job_load, deltas, throttle_rates) for job_load in job_loads]   
    eval_length = max(job_lengths)  
    
    return evaluate_candidate_python(job_loads, deltas, throttle_rates, run_context,    
                                       link_logical_bandwidth, compat_score_mode, 
                                       eval_length)
        
    

def get_full_jobs_signals(this_link_loads, deltas, throttle_rates, pref_iter_count=None):     
    repeated_job_loads = []
    
    for job_load in this_link_loads:
        job_id = job_load["job_id"] 
        
        if pref_iter_count is not None: 
            job_iter_count = pref_iter_count 
        else: 
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


def evaluate_candidate_python(job_loads, deltas, throttle_rates, 
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
    
    if len(job_loads) == 0: # or len(job_loads) == 1:
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
        
        accum = 0 
        
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
            # job_period = max_job_period * len(job_loads)
            # ####################################################
             
            rand_max = max_job_period + weights[job_id]
            if rand_max < 0: 
                rand_max = 0
            
            if candidate_id == 0: 
                # if it's the first time, we want to do something drastic
                random_deltas.append((job["job_id"], accum))
                accum += job["profiles"][throttle_rate]["period"]
            else: 
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
        
        # print("new_deltas: ", new_deltas, "compat_score: ", compat_score, file=sys.stderr)

        # print("new_deltas: ", new_deltas, "compat_score: ", compat_score)   
        solution_scores.append((new_deltas, new_throttle_rates, compat_score))

    good_solutions = sorted(solution_scores, key=lambda x: x[2], reverse=True)
    top_candidates = good_solutions[:ls_top_candidates]
    r = random.randint(0, len(top_candidates) - 1)
    top_solution = top_candidates[r]
    
    return top_solution[0], top_solution[1]



def get_link_loads(jobs, options, run_context, job_profiles):
    """
    receives profiles for a single iteration of a job, find the flows 
    going through each virtual link, combines them into one signal.  
    """
    
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

####################################################################################################
####################################################################################################
####################################################################################################

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


# visualize the link loads based on the runtime. 
def visualize_link_loads_runtime(link_loads, run_context, 
                                 logical_capacity,
                                 suffix="", plot_dir=None, 
                                 smoothing_window=1, separate_plots=False):      

    import matplotlib.pyplot as plt
    import numpy as np
    import os

    num_racks = len(link_loads)
    num_directions = 2  # "up" and "down"


    if not separate_plots: 
        # Create a figure and subplots
        fig, axes = plt.subplots(num_racks, num_directions, figsize=(10, 3 * num_racks), 
                                squeeze=False, sharex=True)

    global job_color_index
    job_color_index = 0
    assigned_job_colors = {} 
    min_over_capacity_time = 1e9 
    
    for rack in range(num_racks):
        for i, direction in enumerate(["up", "down"]):
            if not separate_plots:
                ax = axes[rack][i]
            else: 
                fig, ax = plt.subplots(figsize=(5, 3))
                
            ax.set_title(f"Rack: {rack}, Direction: {direction}")
            ax.set_xlabel("Time (ms)")
            ax.set_ylabel("Load (link capacity units)")
            
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

                    # gaussian filter
                    # smoothed = gaussian_filter1d(job_load, sigma=10)
                    
                    smoothed_job_loads.append(smoothed)
                
                job_loads_array = np.array(smoothed_job_loads)
                            
            ax.stackplot(range(max_length), job_loads_array, 
                         edgecolor = 'black',
                         linewidth = 1,
                         
                         labels=[f"Job: {job_id}" for job_id in job_ids], 
                         colors=[get_job_color(job_id) for job_id in job_ids])

            
            max_value_in_stack = np.max(np.sum(job_loads_array, axis=0)) 
            ax.axhline(y=max_value_in_stack, color='blue', linestyle='--') 
            ax.text(max_length, max_value_in_stack, " max load",
                        verticalalignment='bottom', horizontalalignment='right', color='blue')


            y_max = max(4, max_value_in_stack) * 1.1    
                        
            if logical_capacity is not None:  
                ax.axhline(y=logical_capacity, color='r', linestyle='--') 
                # add an annotation for the logical capacity, right next to the line
                if int(logical_capacity) != int(max_value_in_stack): 
                    ax.text(max_length, logical_capacity, " capacity",
                            verticalalignment='bottom', horizontalalignment='right', color='red')
                
                y_max = max(y_max, logical_capacity * 1.1)  
                # y_max = max_value_in_stack * 1.1 
                
            for i in range(1, math.ceil(y_max) + 1):
                ax.axhline(y=i, color='black', linestyle='-', linewidth=0.5)
                        
            ax.set_ylim(0, y_max)
            
            ax.legend(loc='upper left')

            ax.set_xlim(0, 700)
              
            if separate_plots: 
                plt.tight_layout()
                plot_path = f"{plot_dir}/demand{suffix}_{rack}_{direction}.png"
                plt.savefig(plot_path, bbox_inches='tight', dpi=300)    
                plt.close(fig)

    # create one legend for all the subplots. with the contents of the color assignment to jobs.
    if len(assigned_job_colors) > 0:
        handles = [plt.Line2D([0], [0], marker='o', color='w', markerfacecolor=get_job_color(job_id), label=f"Job: {job_id}") for job_id in assigned_job_colors]
        fig.legend(handles=handles, loc='upper right')
    
    if min_over_capacity_time < 1e9:
        for rack in range(num_racks):
            for i, direction in enumerate(["up", "down"]):
                ax = axes[rack][i]
                ax.axvline(x=min_over_capacity_time, color='black', linestyle=':', linewidth=3)     
            
    if not separate_plots:
        plt.tight_layout()
        plot_path = f"{plot_dir}/demand{suffix}.png"
        plt.savefig(plot_path, bbox_inches='tight', dpi=100)    
        plt.close(fig)
    
####################################################################################################
####################################################################################################
####################################################################################################
    
def get_job_color(job_id): 
    return plt.cm.tab20.colors[job_id % 20] 

    
def visualize_link_loads(link_loads, run_context, 
                         deltas, throttle_rates,
                         link_logical_bandwidth = None, 
                         suffix="", separate_plots=False): 
    
    import matplotlib.pyplot as plt
    import numpy as np
    import os

    num_racks = len(link_loads)
    num_directions = 2  # "up" and "down"

    # Create a figure and subplots
    if not separate_plots:
        fig, axes = plt.subplots(num_racks, num_directions, figsize=(10, 3 * num_racks), squeeze=False, sharex=True)

    global job_color_index
    job_color_index = 0
    assigned_job_colors = {} 

    min_over_capacity_time = 1e9 
    
    for rack in range(num_racks):
        for i, direction in enumerate(["up", "down"]):
            if not separate_plots:
                ax = axes[rack][i]
            else: 
                fig, ax = plt.subplots(figsize=(5, 3))  
                
            ax.set_title(f"Rack: {rack}, Direction: {direction}")
            ax.set_xlabel("Time (ms)")
            ax.set_ylabel("Load (link capacity units)")
            
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
                         edgecolor = 'black',
                         linewidth = 1,
                         
                         labels=[f"Job: {job_id}" for job_id in job_ids], 
                         colors=[get_job_color(job_id) for job_id in job_ids])

                        
            max_value_in_stack = np.max(np.sum(job_loads_array, axis=0)) 
            ax.axhline(y=max_value_in_stack, color='blue', linestyle='--') 
            ax.text(max_length, max_value_in_stack, " max load",
                        verticalalignment='bottom', horizontalalignment='right', color='blue')
            
            y_max = max(4, max_value_in_stack) * 1.1    
            # y_max = max_value_in_stack * 1.1 

            if link_logical_bandwidth is not None:  
                ax.axhline(y=link_logical_bandwidth, color='r', linestyle='--') 
                if int(link_logical_bandwidth) != int(max_value_in_stack): 
                    ax.text(max_length, link_logical_bandwidth, " capacity",
                        verticalalignment='bottom', horizontalalignment='right', color='red')
                                
                y_max = max(y_max, link_logical_bandwidth * 1.1)  
                    
                if not separate_plots:
                    # find the first place that the link goes over the logical bandwidth.
                    sum_signal = np.sum(job_loads_array, axis=0)
                    first_overload_index = np.argmax(sum_signal > link_logical_bandwidth)
                    if max(sum_signal) > link_logical_bandwidth:
                        min_over_capacity_time = min(min_over_capacity_time, first_overload_index) 
                        ax.axvline(x=first_overload_index, color='black', linestyle='--', linewidth=1)
            
            for i in range(1, math.ceil(y_max) + 1):
                ax.axhline(y=i, color='black', linestyle='-', linewidth=0.5) 

            ax.set_ylim(0, y_max)

            ax.legend(loc='upper left')
            ax.set_xlim(0, 700)
            
            if separate_plots: 
                plt.tight_layout()
                plot_path = f"{run_context['timings-dir']}/demand{suffix}_{rack}_{direction}.png"
                plt.savefig(plot_path, bbox_inches='tight', dpi=300)    
                plt.close(fig)

    # create one legend for all the subplots. with the contents of the color assignment to jobs.
    if len(assigned_job_colors) > 0:
        handles = [plt.Line2D([0], [0], marker='o', color='w', markerfacecolor=get_job_color(job_id), label=f"Job: {job_id}") for job_id in assigned_job_colors]
        fig.legend(handles=handles, loc='upper right')
    
    if not separate_plots: 
        if min_over_capacity_time < 1e9:
            for rack in range(num_racks):
                for i, direction in enumerate(["up", "down"]):
                    ax = axes[rack][i]
                    ax.axvline(x=min_over_capacity_time, color='black', linestyle=':', linewidth=3)     
        
    if not separate_plots:
        plt.tight_layout()
        timing_plots_dir = f"{run_context['timings-dir']}/"
        os.makedirs(timing_plots_dir, exist_ok=True)
        plot_path = f"{timing_plots_dir}/demand{suffix}.png"  
        plt.savefig(plot_path, bbox_inches='tight', dpi=100)    
        plt.close(fig)

    
def get_good_until(jobs, link_loads_list, run_context, 
                   deltas, throttle_rates, link_logical_bandwidth,
                   cross_rack_jobs):   
     
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
        job_id = job["job_id"] 
        
        if job_id not in cross_rack_jobs:  
            good_until[job["job_id"]] = job["iter_count"] - 1   
        else: 
            good_until[job["job_id"]] = -1
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
        
    
    return good_until
    
def evaluate_candidate_all_links(jobs, link_loads, deltas, throttle_rates, run_context, 
                                 link_logical_bandwidth, compat_score_mode, 
                                 cross_rack_jobs): 
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
            candidate_score = min(compat_score, candidate_score)   
    
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
    

# this is how the job timings are generated 
    # def get_job_timings(self):  
    #     job_timings = []    
    #     for job_id, job in self.job_map.items():
    #         job_timings.append({
    #             "deltas": self.deltas[job_id],  
    #             "throttle_rates": self.throttle_rates[job_id],     
    #             "job_id": job_id
    #         })   
    #     return job_timings
    
def get_avg_job_cost(job_id, jobs, job_timings):
    job = None
    for j in jobs:
        if j["job_id"] == job_id:
            job = j
            break
        
    for job_timing in job_timings:
        if job_timing["job_id"] == job_id:
            job_cost = 0 
            
            job_cost = sum(job_timing["deltas"])
            
            for throttle_rate in job_timing["throttle_rates"]:
                period = job["period"][str(throttle_rate)]
                base_period = job["base_period"]       
                
                throttle_cost = period - base_period    
                job_cost += throttle_cost   
                
            return job_cost / job["iter_count"]

    

    
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


    # visualize_link_loads(link_loads, run_context, base_deltas, 
    #                      base_throttle_rates,    
    #                      link_logical_bandwidth=link_logical_bandwidth, 
    #                      suffix=f"_round_base")
    
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
                                    link_logical_bandwidth=link_logical_bandwidth, 
                                    cross_rack_jobs=cross_rack_jobs)    
        
        all_scores.append(candidate_score)
        
        if candidate_score > best_candidate_score:
            best_candidate_score = candidate_score
            best_candidate_deltas = current_deltas
            best_candidate_throttle_rates = current_throttle_rates
            best_candidate_good_until = good_until

    if run_context["plot-intermediate-timing"]: 
        visualize_link_loads(link_loads, run_context, best_candidate_deltas, 
                             best_candidate_throttle_rates,    
                             link_logical_bandwidth=link_logical_bandwidth, 
                             suffix=f"_round_{round + 1}_best")
    
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
                
            starting_iterations[job_id] = max(good_until[job_id] + 1, starting_iterations[job_id])  
            
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
        
        
        sys.stderr.write("jobs: {}\n".format(jobs)) 
        sys.stderr.write("round: {}, avg_job_cost: {}\n".format(i, avg_job_cost))
        # write the good untils to the stderr 
        sys.stderr.write("good_until: {}\n".format(good_until))
        sys.stderr.write("starting_iterations: {}\n".format(starting_iterations))
        
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


def farid_timing_v2(jobs, options, run_context, timing_scheme, job_profiles):

    solver = LegoV2Solver(jobs, run_context, options, job_profiles, timing_scheme)
    job_timings, solution = solver.solve()    
    
    # step 4: return the full schedule.  
    return job_timings


# doing the timing and routing together.    
# general idea is to do the timing first, then do the routing.
# the check which time_ranges are problematic in the routing process
# and then go back and fix the timing.
# then do the routing again.
# repeat until the routing is good.
# then return the job_timings and lb_decisions.


def get_bad_range_ratio(new_bad_ranges, prev_bad_ranges, sim_length):   
    sum_bad_ranges = 0
    for bad_range in new_bad_ranges:
        sum_bad_ranges += (bad_range[1] - bad_range[0])
    for bad_range in prev_bad_ranges:
        sum_bad_ranges += (bad_range[1] - bad_range[0])
        
    bad_range_ratio = sum_bad_ranges / sim_length
    
    return bad_range_ratio

def log_bad_ranges(run_context, current_round, new_bad_ranges, prev_bad_ranges):   
    bad_ranges_dir = f"{run_context['timings-dir']}/bad_ranges/"
    os.makedirs(bad_ranges_dir, exist_ok=True)
    
    path = f"{bad_ranges_dir}/{current_round}.txt"
    
    bad_range_ratio = get_bad_range_ratio(new_bad_ranges, prev_bad_ranges, run_context["sim-length"])
    
    with open(path, "w") as f:
        f.write(f"bad range ratio: {bad_range_ratio}\n")
        
        f.write("new bad ranges:\n")
        json.dump(new_bad_ranges, f, indent=4)
        f.write("\n")
        f.write("\n")
        f.write("\n")
        f.write("prev bad ranges:\n")
        json.dump(prev_bad_ranges, f, indent=4)    
        f.flush()

def log_progress(run_context, message): 
    with open(run_context["output-file"], "a+") as f:   
        f.write(f"{message}\n")
        f.flush()
        
def append_to_bad_ranges(bad_ranges, new_bad_ranges):
    # we just do it one at a time.
    new_bad_ranges.sort() 
    bad_range_to_add = new_bad_ranges[0]
    
    # reset the bad_ranges, and we will rebuild it. 
    bad_banges_copy = copy.deepcopy(bad_ranges)
    bad_ranges.clear()
    
    # we want to remove any bad ranges that happen completely after the new bad range.
    for i in range(len(bad_banges_copy)):
        current = bad_banges_copy[i]
        
        if current[0] < bad_range_to_add[1]:
            bad_ranges.append(current)
    
    # we want to add the new bad range to the bad_ranges.
    bad_ranges.append(bad_range_to_add)            




def faridv3_scheduling(jobs, options, run_context, job_profiles):
    # the only supported mode for now 
    timing_scheme = run_context["timing-scheme"]
    assert timing_scheme == "faridv3" 


    solver = LegoSolver(jobs, run_context, options, job_profiles, timing_scheme)
    current_round = 0
    # step 1: do the timing first.
    job_timings, solution = solver.solve()
    lb_decisions, new_bad_ranges = route_flows(jobs, options, run_context, 
                                                   job_profiles, job_timings, 
                                                   current_round, highlighted_ranges=[], 
                                                   early_return=False)
    
    log_bad_ranges(run_context, current_round, new_bad_ranges, [])

    prev_bad_ranges = [] 
    current_round = 1
    max_attempts = run_context["farid-rounds"]
    
    # step 2: if the routing is good, return the results.
    while len(new_bad_ranges) > 0 and current_round < max_attempts:
        append_to_bad_ranges(prev_bad_ranges, new_bad_ranges)

        # step 3: fix the timing.
        with open(run_context["output-file"], "a+") as f:
            f.write(f"timing round {current_round}, starting at {datetime.now()}\n")    
            
        job_timings, solution = solver.solve(prev_bad_ranges)
        # step 4: do the routing again.
        
        with open(run_context["output-file"], "a+") as f:
            f.write(f"routing round {current_round}, starting at {datetime.now()}\n")    
        
        early_return = True
        if current_round == max_attempts - 1:
            early_return = False
        
        lb_decisions, new_bad_ranges = route_flows(jobs, options, run_context, 
                                                   job_profiles, job_timings, 
                                                   current_round, 
                                                   highlighted_ranges=prev_bad_ranges, 
                                                   early_return=early_return)   

        log_bad_ranges(run_context, current_round, new_bad_ranges, prev_bad_ranges)
            
        current_round += 1 
        
    return job_timings, lb_decisions

             
       
       

def faridv4_scheduling(jobs, options, run_context, job_profiles):
    # the only supported mode for now 
    timing_scheme = run_context["timing-scheme"]
    assert timing_scheme == "faridv4" 

    solver = LegoV2Solver(jobs, run_context, options, job_profiles, timing_scheme)
    current_round = 0
    
    
    # step 1: do the vanilla timing first.
    log_progress(run_context, "starting vanilla timing")    
    
    job_timings, solution = solver.solve()
    lb_decisions, new_bad_ranges = route_flows(jobs, options, run_context, 
                                               job_profiles, job_timings, 
                                               suffix=current_round, 
                                               highlighted_ranges=[])
    
    log_bad_ranges(run_context, "1.0_vanilla", new_bad_ranges, [])

    # step 1.5: if the routing is good, return the results.
    if len(new_bad_ranges) == 0:
        return job_timings, lb_decisions
    
    
    ################################################################################
    # step 2: if the routing is bad, then trying patching it up a little bit. 
    ################################################################################

    for inflate in [1.0, 1.1, 1.2, 1.3]:

        max_attempts = run_context["farid-rounds"]
        current_round = 1
        prev_bad_ranges = [] 

        while len(new_bad_ranges) > 0 and current_round < max_attempts:
            append_to_bad_ranges(prev_bad_ranges, new_bad_ranges)

            # step 2.1: fix the timing.
            log_progress(run_context, "starting timing fix, round {}".format(current_round))    
            
            job_timings, solution = solver.solve_with_bad_ranges_and_inflation(prev_bad_ranges, inflate)
            # step 2.2: do the routing again.
            lb_decisions, new_bad_ranges = route_flows(jobs, options, run_context, 
                                                       job_profiles, job_timings, 
                                                       suffix=f"{inflate}_{current_round}", 
                                                       highlighted_ranges=prev_bad_ranges)   

            log_bad_ranges(run_context, f"inflation_{inflate}_round_{current_round}", 
                           new_bad_ranges, prev_bad_ranges)
            
            current_round += 1
            
        if len(new_bad_ranges) == 0:
            break 
        
    # step 2.5: if the routing is good, return the results.
    bad_range_ratio = get_bad_range_ratio(new_bad_ranges, prev_bad_ranges, run_context["sim-length"])
    if len(new_bad_ranges) == 0 and bad_range_ratio < run_context["fallback-threshold"]:
        return job_timings, lb_decisions


    ################################################################################
    # step 3: if routing is still bad, then do the zero timing + v3 routing.
    ################################################################################
    bad_range_ratio = get_bad_range_ratio(new_bad_ranges, prev_bad_ranges, run_context["sim-length"])
    average_job_cost = solution.get_average_job_cost() / run_context["sim-length"]

    log_progress(run_context, "starting zero timing, bad range ratio: {}, average job cost: {}".format(bad_range_ratio, average_job_cost))
        
    job_timings, solution = solver.get_zero_solution()
    lb_decisions, new_bad_ranges = route_flows(jobs, options, run_context, 
                                                job_profiles, job_timings, 
                                                suffix=current_round, 
                                                highlighted_ranges=[], 
                                                early_return=False, 
                                                override_routing_strategy="graph-coloring-v3")
        
    return job_timings, lb_decisions

        
        

    
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
        "faridv2": farid_timing_v2, 
    }
    
    if timing_scheme.split("_")[0] not in timing_funcions:
        raise ValueError(f"Invalid timing-scheme: {timing_scheme}")
    timing_func = timing_funcions[timing_scheme.split("_")[0]] 
    job_timings = timing_func(jobs, options, run_context, timing_scheme, job_profiles)

    # visualize

        
    return job_timings

def get_job_routings(jobs, options, run_context, job_profiles, job_timings):    
    lb_scheme = options["lb-scheme"]
     
    if lb_scheme == "readprotocol":
        lb_decisions, bad_ranges = route_flows(jobs, options, run_context, job_profiles, job_timings)
    else: 
        lb_decisions, bad_ranges = None, None
        
    return lb_decisions
    
def dump_scheduling_results(job_timings, lb_decisions,  
                            timing_file_path, routing_file_path):
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

def generate_timing_file(timing_file_path, routing_file_path, placement_seed, 
                         jobs, options, run_context):

    random.seed(run_context["experiment-seed"] + placement_seed)

    # load the job profiles. Might be a bit unnecassary in some cases, but anyway. 
    job_profiles = load_job_profiles(jobs, run_context)

    if run_context["plot-initial-timing"]: 
        visualize_workload_timing(jobs, options, run_context, None, 
                                  job_profiles, None, mode="initial") 
        
    timing_scheme = run_context["timing-scheme"]
    lb_scheme = options["lb-scheme"]    
    
    if timing_scheme == "faridv3":
        job_timings, lb_decisions = faridv3_scheduling(jobs, options, 
                                                       run_context, job_profiles)
    elif timing_scheme == "faridv4":
        job_timings, lb_decisions = faridv4_scheduling(jobs, options, 
                                                       run_context, job_profiles)   
    else:
        # do the timing first.
        job_timings = get_job_timings(jobs, options, run_context, 
                                    job_profiles)
        
        # do the routing next.
        lb_decisions = get_job_routings(jobs, options, run_context, 
                                        job_profiles, job_timings)   
            
    if run_context["plot-final-timing"]: 
        visualize_workload_timing(jobs, options, run_context, job_timings, 
                                  job_profiles, None, mode="final") 
        
    dump_scheduling_results(job_timings, lb_decisions, 
                            timing_file_path, routing_file_path)    

    job_ids = [job["job_id"] for job in jobs] 
    job_ids.sort()
    
    add_to_context = {
        "job_costs": [get_avg_job_cost(job_id, jobs, job_timings) for job_id in job_ids],
    }
    # returning the results just in case as well. 
    return job_timings, lb_decisions, add_to_context     


if __name__ == "__main__":
    
    input_data = json.load(sys.stdin)
    # call the main function
    job_timings, lb_decisions, add_to_context = generate_timing_file(**input_data)
    
    dumped_data = {
        "job_timings": job_timings, 
        "lb_decisions": lb_decisions,
        "add_to_context": add_to_context,
    } 
    
    # write the output to stdout
    print(json.dumps(dumped_data))
    
    
    