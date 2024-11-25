# cython: language_level=3

import random
import json
from pprint import pprint 
import math 
import time 
import sys 
import copy 
from processing.flowprogress import get_job_profiles
from algo.routing import route_flows
import subprocess
import os 
import pickle as pkl 
import numpy as np 
import traceback
from utils.util import rage_quit

####################################################################################
##################  HELPER FUNCTIONS  ##############################################
####################################################################################

EVAL_MODE = "cpp"
EVAL_MODE = "python"

def log_results(run_context, key, value):
    # print to stderr first  
    sys.stderr.write(f"KEY: {key}\n")
    sys.stderr.write(f"VALUE: {value}\n")   

    with open(run_context["output-file"], "a+") as f:
        f.write("Results for: " + key + "\n\n")
        pprint(value, f) 
        f.write("\n\n---------------------------------\n\n")   


# all the workloads will be starting at the same time, at time 0.
# this is technically the worst case scenario.
def zero_timing(jobs, options, run_context, config_sweeper, timing_scheme):
    job_timings = [] 
    
    for job in jobs:
        job_timing = 0
    
        job_timings.append({
            "initial_wait": job_timing,
            "job_id": job["job_id"]
        })     
        
    return job_timings, None 


# trying to spread the jobs out a bit in time. The number 400 is arbitrary.
# it's chosen based on the curren numbers. 
# TODO: make this number something that could be found based on the job profiles.
# in some way, a much much simpler version of the cassini timing.
def inc_timing(jobs, options, run_context, config_sweeper, timing_scheme):
    job_timings = [] 
    
    timing_scheme_split = timing_scheme.split("_") 
    
    if len(timing_scheme_split) == 1:
        job_timing_increment = 400
    else: 
        job_timing_increment = int(timing_scheme_split[1])
    
    
    
    for job in jobs:
        job_timing = job_timing_increment * (job["job_id"] - 1)
    
        job_timings.append({
            "initial_wait": job_timing,
            "job_id": job["job_id"]
        })     
        
    return job_timings, None


# all the jobs will start at a random time, somewhere between 0 and the period of the job.
# this is what we should assume to be happening in the real world, where the jobs are not
# synchronized in any sense. 
def random_timing(jobs, options, run_context, config_sweeper, timing_scheme):
    job_timings = [] 

    for job in jobs:
        job_id = job["job_id"] 
        job_timing = random.randint(0, job["period"] - 1)
    
        job_timings.append({
            "initial_wait": job_timing,
            "job_id": job_id
        })     
        
    return job_timings, None 
    

################################################################################################
################ CASINI TIMING #################################################################
################################################################################################

# we will run each job in isolation, in a network that wouldn't be bottlenecked by the core part 
# of the network. We will get the flow progress history for each job, process it and get the period.

def get_delta_for_job_in_decisions(decisions, id):
    for decision in decisions:
        if decision[0] == id:
            return decision[1]

    return None 


def set_delta_for_job_in_decisions(decisions, id, delta):
    for i in range(len(decisions)):
        if decisions[i][0] == id:
            decisions[i] = (id, delta)
            return
        
    decisions.append((id, delta))


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

    
def evaluate_candidate(job_loads, deltas, run_context, link_logical_bandwidth, compat_score_mode):
    if len(job_loads) == 0:
        return 1
    
    # periods = [job["period"] * job["iter_count"] for job in job_loads]
    # hyperperiod = max(periods)  + max([d[1] for d in deltas])
    # eval_length = min(hyperperiod, run_context["sim-length"]) 
    max_base_job_length = 0 
    eval_length = 0 

    for job_load in job_loads:
        job_id = job_load["job_id"] 
        deltas_for_job = deltas[job_id] 
        max_base_job_length = max(max_base_job_length, len(job_load["load"]))
        job_length_all_iter = len(job_load["load"]) * job_load["iter_count"]
        job_length = job_length_all_iter + sum(deltas_for_job)
        
        eval_length = max(eval_length, job_length)  

    eval_length = eval_length + max_base_job_length 
    
    if EVAL_MODE == "cpp":
        return evaluate_candidate_cpp(job_loads, deltas, run_context, 
                                      link_logical_bandwidth, compat_score_mode, 
                                      eval_length)
    else:
        score_2 = evaluate_candidate_python_2(job_loads, deltas, run_context,    
                                             link_logical_bandwidth, compat_score_mode, 
                                             eval_length)

        # print(f"CPP: {score_cpp}, Python: {score_1}, Python 2: {score_2}")

        return score_2 

def evaluate_candidate_cpp(job_loads, deltas, run_context, 
                           link_logical_bandwidth, compat_score_mode, eval_length):
    
    rage_quit("This function is not working. It's not returning the right values.")
    
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

def get_full_jobs_signals(this_link_loads, deltas, max_length=None):     
    
    job_ids = [job_load["job_id"] for job_load in this_link_loads]
    job_loads = [job_load["load"] for job_load in this_link_loads]
    job_iter_counts = [job_load["iter_count"] for job_load in this_link_loads]  
    
    repeated_job_loads = []
    for job_id, job_load, job_iter_count in zip(job_ids, job_loads, job_iter_counts):   
        job_total_load = np.zeros(0)
        for iter_id in range (job_iter_count):
            iter_time_shift = deltas[job_id][iter_id]
            job_total_load = np.append(job_total_load, np.zeros(iter_time_shift))
            job_total_load = np.append(job_total_load, job_load)
        repeated_job_loads.append(job_total_load)

    # Find the maximum length of the repeated and shifted job loads
    if max_length is None:  
        max_length = max(len(job_load) for job_load in repeated_job_loads)

    # Pad repeated and shifted job loads with zeros to make them all the same length
    padded_job_loads = [
        np.pad(job_load, (0, max_length - len(job_load)), mode='constant')
        for job_load in repeated_job_loads
    ]
    
    return padded_job_loads, max_length


def evaluate_candidate_python_2(job_loads, deltas, run_context, link_logical_bandwidth, 
                              compat_score_mode, eval_length):
    sim_length = eval_length
    
    sum_signal = np.zeros(sim_length, dtype=np.float64)
    
    padded_job_loads, _ = get_full_jobs_signals(job_loads, deltas, sim_length) 
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
        
    elif compat_score_mode == "max-util-left":
        compat_score = (link_logical_bandwidth - max_util) / link_logical_bandwidth

    return compat_score
    
    
def solve_for_link(job_loads, link_logical_bandwidth, run_context, 
                   compat_score_mode, starting_iterations, base_deltas, 
                   fixed_prefs=None, resolved_deltas_set=None):
    
    ls_candidates = run_context["cassini-parameters"]["link-solution-candidate-count"]
    ls_rand_quantum = run_context["cassini-parameters"]["link-solution-random-quantum"]
    ls_top_candidates = run_context["cassini-parameters"]["link-solution-top-candidates"]    
    
    if len(job_loads) == 0: 
        return [([], 0)] * ls_top_candidates
    
    if len(job_loads) == 1:
        one_job_id = job_loads[0]["job_id"] 
        new_solution = base_deltas[one_job_id].copy()
        return [([(job_loads[0]["job_id"], new_solution)], 0)]
        
    delta_scores = [] 
    involved_jobs = set([job["job_id"] for job in job_loads])
    
    for i in range(ls_candidates):
        new_deltas = copy.deepcopy(base_deltas) 
        
        random_deltas = [] 
        for job in job_loads: 
            rand_options = int(math.ceil(job["period"] / ls_rand_quantum))
            r = random.randint(0, rand_options) * ls_rand_quantum        
            random_deltas.append((job["job_id"], r)) 
            
        min_delta = min([x[1] for x in random_deltas])
        random_deltas = [(x[0], x[1] - min_delta) for x in random_deltas]
        
        number_of_fixed_decisions = 0 
        
        if fixed_prefs is not None and resolved_deltas_set is not None:
            
            # iterate over the set 
            for job_id, deltas in fixed_prefs.items():  
                if job_id in resolved_deltas_set:   
                    number_of_fixed_decisions += 1
                    set_delta_for_job_in_decisions(random_deltas, job_id, deltas[starting_iterations[job_id]])  
        
        if number_of_fixed_decisions == len(involved_jobs):
            return [(new_deltas, 0)]
        
        for job_id, delta in random_deltas:
            # set_delta_for_job_in_decisions(new_deltas, job_id, delta)
            iter = starting_iterations[job_id]
            new_deltas[job_id][iter] = delta
            
        compat_score = evaluate_candidate(job_loads, new_deltas, 
                                          run_context, 
                                          link_logical_bandwidth, 
                                          compat_score_mode)
        
        
        delta_scores.append((new_deltas, compat_score))

    good_deltas = sorted(delta_scores, key=lambda x: x[1], reverse=True)
    results = good_deltas[:ls_top_candidates]
    
    return results

def get_link_loads(jobs, options, run_context, job_profiles):
    servers_per_rack = options["ft-server-per-rack"]
    rack_count = options["machine-count"] // servers_per_rack   
    link_bandwidth = options["link-bandwidth"]  
    
    link_loads = [] 
    cross_rack_jobs_set = set() 
    cross_rack_jobs = []    
    
    for i in range(rack_count):
        this_rack = {"up": [], "down": []}
        link_loads.append(this_rack)

    for job in jobs:
        job_id = job["job_id"]
        if job_id not in job_profiles:
            continue    
        
        job_profile = job_profiles[job_id]
        if len(job_profile["flows"]) == 0:
            continue 

        job_period = job_profile["period"]  
        
        # for flow in job_profile["flows"]:   
        #     for i in range(len(flow["progress_history"])):
        #         flow["progress_history"][i] /= link_bandwidth
        
        # this job will add some load to each of the links. 
        # all the flows for this job will be added up. 
        for i in range(rack_count):
            for dir in ["up", "down"]:
                def add_signal_to_sum(signal, sum_signal):
                    for i in range(len(signal)):
                        if i >= len(sum_signal):
                            sum_signal.append(0)
                        sum_signal[i] += signal[i]

                link_job_load_combined = [] 
                any_flow_added = False
                
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
                    cross_rack_jobs_set.add(job_id) 
                    
                    link_loads[i][dir].append({
                        "job_id": job_id,
                        "iter_count": job["iter_count"],    
                        "load": link_job_load_combined,
                        "period": job_period
                    })
                    
    cross_rack_jobs = list(cross_rack_jobs_set) 
    return link_loads, cross_rack_jobs



def visualize_link_loads(link_loads, run_context, deltas, 
                         link_logical_bandwidth = None, suffix=""): 
    
    if "visualize-timing" not in run_context or not run_context["visualize-timing"]:    
        return  

    import matplotlib.pyplot as plt
    import numpy as np
    import os

    num_racks = len(link_loads)
    num_directions = 2  # "up" and "down"

    # Create a figure and subplots
    fig, axes = plt.subplots(num_racks, num_directions, figsize=(10, 3 * num_racks), squeeze=False)

    for rack in range(num_racks):
        for i, direction in enumerate(["up", "down"]):
            ax = axes[rack][i]
            ax.set_title(f"Rack: {rack}, Direction: {direction}")
            ax.set_xlabel("Time")
            ax.set_ylabel("Load")
            
            if len(link_loads[rack][direction]) == 0:
                ax.text(0.5, 0.5, "No jobs", horizontalalignment='center', verticalalignment='center', transform=ax.transAxes)  
                continue
            
            padded_job_loads, max_length = get_full_jobs_signals(link_loads[rack][direction], deltas)
            job_ids = [job_load["job_id"] for job_load in link_loads[rack][direction]]  
            
            # Convert the padded job loads to a 2D array
            job_loads_array = np.array(padded_job_loads)

            ax.stackplot(range(max_length), job_loads_array, labels=[f"Job: {job_id}" for job_id in job_ids])

            if link_logical_bandwidth is not None:  
                ax.axhline(y=link_logical_bandwidth, color='r', linestyle='--') 
                
                # find the first place that the link goes over the logical bandwidth.
                sum_signal = np.sum(job_loads_array, axis=0)
                first_overload_index = np.argmax(sum_signal > link_logical_bandwidth)
                if max(sum_signal) > link_logical_bandwidth:
                    ax.axvline(x=first_overload_index, color='r', linestyle='--')
            
            max_value_in_stack = np.max(np.sum(job_loads_array, axis=0)) 
            
            ax.axhline(y=max_value_in_stack, color='blue', linestyle='--') 

            # ax.legend(loc='upper left')

    plt.tight_layout()

    timing_plots_dir = f"{run_context['timings-dir']}/timing/"
    os.makedirs(timing_plots_dir, exist_ok=True)
    plot_path = f"{timing_plots_dir}/demand{suffix}.png"  
    plt.savefig(plot_path, bbox_inches='tight', dpi=300)    
    plt.close(fig)

def get_good_until(jobs, link_loads, run_context, deltas, link_logical_bandwidth):    
    num_racks = len(link_loads)

    min_first_overload_index = 1e9  
    
    for rack in range(num_racks):
        for i, direction in enumerate(["up", "down"]):
            padded_job_loads, max_length = get_full_jobs_signals(link_loads[rack][direction], deltas)
            # Convert the padded job loads to a 2D array
            job_loads_array = np.array(padded_job_loads)
            sum_signal = np.sum(job_loads_array, axis=0)
            first_overload_index = np.argmax(sum_signal > link_logical_bandwidth)
            if max(sum_signal) <= link_logical_bandwidth:
                first_overload_index = max_length   
            
            min_first_overload_index = min(min_first_overload_index, first_overload_index)  

    # so everything is good until the min_first_overload_index.
    # everything is good until the min_first_overload_index.
    # what's the iteration that corresponds to this index?
    good_until = {} 
    for job in jobs: 
        good_until[job["job_id"]] = -1
        
        job_id = job["job_id"] 
        job_period = job["period"] 
        job_iter_count = job["iter_count"] 

        # the job will be delta, period, delta, period, delta, period, ...  
        current_time = 0    
        
        for iter_id in range(job_iter_count):
            current_time += deltas[job_id][iter_id]
            current_time += job_period 
            if current_time > min_first_overload_index:
                break
            
            good_until[job_id] = iter_id
    return good_until
    
def evaluate_candidate_all_links(link_loads, deltas, run_context, 
                                 link_logical_bandwidth, compat_score_mode, 
                                 rack_count):
    # now we have a candidate solution. We should evaluate it. 
    if compat_score_mode == "time-no-coll":
        candidate_score = 1 
    elif compat_score_mode == "max-util-left":    
        candidate_score = 0
    elif compat_score_mode == "under-cap":
        candidate_score = 0
                
    for rack in range(rack_count):
        for direction in ["up", "down"]:
            compat_score = evaluate_candidate(job_loads=link_loads[rack][direction], 
                                              deltas=deltas, 
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

    
def get_timeshifts(jobs, options, run_context, config_sweeper, job_profiles, 
                   starting_iterations=None, base_deltas=None, round=0):       
    
    if starting_iterations is None or base_deltas is None:
        rage_quit("starting_iteration or deltas is None")
     
    start_time = time.time()    
    
    if "compat-score-mode" not in run_context:
        rage_quit("compat-score-mode is required in run_context")
        
    compat_score_mode = run_context["compat-score-mode"]
    overall_solution_candidate_count = run_context["cassini-parameters"]["overall-solution-candidate-count"]
    
    servers_per_rack = options["ft-server-per-rack"]
    rack_count = options["machine-count"] // servers_per_rack       
    link_logical_bandwidth = options["ft-core-count"]
    
    # log_results(run_context, "jobs", jobs)
    link_loads, cross_rack_jobs = get_link_loads(jobs, options, run_context, job_profiles)   

    visualize_link_loads(link_loads, run_context, base_deltas, link_logical_bandwidth=link_logical_bandwidth)
    
    best_candidate_score = -1e9 
    best_candidate = None
    best_candidate_good_until = None     
    
    all_scores = [] 
     
    link_loads_list = [] 
    for rack in range(rack_count):
        for direction in ["up", "down"]:
            link_loads_list.append(link_loads[rack][direction])
    
    resolved_deltas_set = set() 
    
    for i in range(overall_solution_candidate_count):
        # shuffle the link_solutions. No real difference beetwen the links.
        random.shuffle(link_loads_list)

        # pick the top solution for the first link (which is an arbitrary choice)
        first_link_loads = link_loads_list[0]
        solutions = solve_for_link(first_link_loads, link_logical_bandwidth, 
                                   run_context, compat_score_mode, 
                                   starting_iterations=starting_iterations, 
                                   base_deltas=base_deltas)
        
        
        r = random.randint(0, len(solutions) - 1)
        top_solution = solutions[r]
        current_decisions = top_solution[0]
        # log_results(run_context, f"link_solutions_0_candidate_{i}", current_decisions)

        this_link_jobs_ids = [job["job_id"] for job in first_link_loads]
        for this_link_jobs_id in this_link_jobs_ids:
            resolved_deltas_set.add(this_link_jobs_id)

        # go through the rest of the links. 
        for j in range(1, len(link_loads_list)):
            this_link_loads = link_loads_list[j]
            
            link_solutions = solve_for_link(this_link_loads, link_logical_bandwidth, 
                                            run_context, compat_score_mode,
                                            starting_iterations=starting_iterations, 
                                            base_deltas=base_deltas,
                                            fixed_prefs=current_decisions, 
                                            resolved_deltas_set=resolved_deltas_set)    
            
            r = random.randint(0, len(link_solutions) - 1)
            top_solution = link_solutions[r]
            
            top_solution_timing = top_solution[0]
            
            # update the current decisions.
            for job_id, delta in top_solution_timing.items(): 
                this_link_jobs_id = set([job["job_id"] for job in this_link_loads]) 
                if job_id in this_link_jobs_id:
                    starting_iter = starting_iterations[job_id]  
                    current_decisions[job_id][starting_iter] = delta[starting_iter]
                    resolved_deltas_set.add(job_id)
            # log_results(run_context, f"link_solutions_{j}_candidate_{i}", current_decisions)
            
            if len(resolved_deltas_set) == len(cross_rack_jobs):
                break
        
        visualize_link_loads(link_loads, run_context, current_decisions,    
                             link_logical_bandwidth=link_logical_bandwidth, 
                             suffix=f"_{i}_{round}")    
        
        candidate_score = evaluate_candidate_all_links(link_loads, current_decisions, run_context, 
                                                       link_logical_bandwidth, compat_score_mode, 
                                                       rack_count) 
        

        
        log_results(run_context, "candidate", (current_decisions, candidate_score))
        
        good_until = get_good_until(jobs, link_loads, run_context, current_decisions,    
                                    link_logical_bandwidth=link_logical_bandwidth)
        
        all_scores.append(candidate_score)
        
        if candidate_score > best_candidate_score:
            best_candidate_score = candidate_score
            best_candidate = current_decisions
            best_candidate_good_until = good_until


    job_timings = [] 
    log_results(run_context, "best_candidate", (best_candidate, best_candidate_score))
    for job in jobs:
        job_id = job["job_id"]
        # timing = get_delta_for_job_in_decisions(best_candidate, job_id)
        if job_id in best_candidate:
            timing = best_candidate[job_id]
        else:
            timing = [0] * job["iter_count"]    
            
        job_timings.append({
            "initial_wait": timing,
            "job_id": job_id
        })           

            
    end_time = time.time() 
    time_taken = end_time - start_time 
    log_results(run_context, "time_taken", time_taken)  
        
    return job_timings, best_candidate_good_until  

def load_job_profiles(jobs, run_context): 
    job_profiles = {} 
    
    for job in jobs: 
        profiles_dir = run_context["profiles-dir"]
        job_id = job["job_id"]
        path = f"{profiles_dir}/{job_id}.pkl"
        
        # check if the file exists.
        if not os.path.exists(path):
            continue 
                
        with open(path, "rb") as f:  
            job_profiles[job_id] = pkl.load(f)
    
    return job_profiles
        

def cassini_timing(jobs, options, run_context, config_sweeper, timing_scheme):
    # job_profiles = profile_all_jobs(jobs, options, run_context, config_sweeper)
    job_profiles = load_job_profiles(jobs, run_context) 

    # run cassini timing with the job profiles, find some timings for the jobs.
    # TODO: add the other argument. 
    job_timings = get_timeshifts(jobs, options, run_context, config_sweeper, job_profiles)

    return job_timings, None

################################################################################################
################ Farid TIMING #################################################################
################################################################################################

def farid_timing(jobs, options, run_context, config_sweeper, timing_scheme):
    # step 1: profile the jobs 
    job_profiles = load_job_profiles(jobs, run_context)

    # step 2: run cassini timing with the job profiles, find some timings for the jobs.  
    
    
    base_deltas = {}
    starting_iterations = {}    
    
    for job in jobs:
        job_id = job["job_id"]
        base_deltas[job_id] = [0] * job["iter_count"]
        starting_iterations[job_id] = 0
        
    for i in range(3): 
        
        print("starting round {}".format(i))    
        print("starting_iterations:")
        pprint(starting_iterations)
        
        print("base_deltas:")
        pprint(base_deltas)
        
        job_timings, good_until = get_timeshifts(jobs, options, run_context, config_sweeper, job_profiles, 
                                                starting_iterations=starting_iterations, base_deltas=base_deltas, round=i) 
        
        for job in jobs:
            job_id = job["job_id"]
            starting_iterations[job_id] = good_until[job_id] + 1    
            
            for timing in job_timings:
                if timing["job_id"] == job_id:
                    base_deltas[job_id] = timing["initial_wait"]
                    break   
                
    # step 3: do the routing for the flows. 
    lb_decisions = route_flows(jobs, options, run_context, config_sweeper, job_profiles, job_timings)
    
    # step 4: return the full schedule.  
    return job_timings, lb_decisions

############################################################################
################ MAIN FUCTION  #############################################
############################################################################

def generate_timing_file(timing_file_path, routing_file_path, placement_seed, 
                         jobs, options, run_context, config_sweeper):

    random.seed(run_context["experiment-seed"] + placement_seed)
    
    if "timing-scheme" not in run_context:
        raise ValueError("timing-scheme option is required")

    timing_scheme = run_context["timing-scheme"]    
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
    job_timings, lb_decisions = timing_func(jobs, options, run_context, config_sweeper, timing_scheme)
    
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
            
    return job_timings, lb_decisions    

if __name__ == "__main__":
    if len(sys.argv) == 2:
        if sys.argv[1] == "--test":
            input_data = '{"timing_file_path": "results/sweep/1223-nethint_LB+leastloaded_TS+cassini_R+optimal_4_58/custom_files//timings/random-optimal/2//cassini.txt", "placement_seed": 2, "jobs": [{"job_id": 1, "machine_count": 6, "comm_size": 10000, "comp_size": 1000, "layer_count": 1, "iter_count": 20, "machines": [2, 4, 7, 8, 17, 22]}, {"job_id": 2, "machine_count": 7, "comm_size": 10000, "comp_size": 500, "layer_count": 1, "iter_count": 20, "machines": [5, 10, 21, 24, 26, 29, 31]}, {"job_id": 3, "machine_count": 5, "comm_size": 20000, "comp_size": 500, "layer_count": 1, "iter_count": 20, "machines": [1, 16, 18, 19, 25]}, {"job_id": 4, "machine_count": 4, "comm_size": 10000, "comp_size": 1000, "layer_count": 1, "iter_count": 20, "machines": [3, 15, 23, 30]}, {"job_id": 5, "machine_count": 8, "comm_size": 20000, "comp_size": 1000, "layer_count": 1, "iter_count": 20, "machines": [0, 6, 9, 11, 12, 20, 27, 28]}, {"job_id": 6, "machine_count": 2, "comm_size": 20000, "comp_size": 1000, "layer_count": 1, "iter_count": 20, "machines": [13, 14]}], "options": {"step-size": 1, "core-status-profiling-interval": 100000, "rep-count": 1, "console-log-level": 4, "file-log-level": 3, "initial-rate": 100, "min-rate": 100, "drop-chance-multiplier": 0, "rate-increase": 1, "priority-allocator": "maxmin", "network-type": "leafspine", "link-bandwidth": 100, "ft-rack-per-pod": 1, "ft-agg-per-pod": 1, "ft-pod-count": -1, "ft-server-tor-link-capacity-mult": 1, "ft-tor-agg-link-capacity-mult": 1, "ft-agg-core-link-capacity-mult": 1, "shuffle-device-map": false, "regret-mode": "none", "machine-count": 32, "ft-server-per-rack": 8, "general-param-1": 4, "general-param-3": 8, "simulation-seed": 58, "protocol-file-name": "nethint-test", "lb-scheme": "random", "timing-scheme": "cassini", "ring-mode": "optimal", "placement-mode": "random", "ft-core-count": 2, "placement-seed": 2, "load-metric": "utilization", "placement-file": "results/sweep/1223-nethint_LB+leastloaded_TS+cassini_R+optimal_4_58/custom_files//placements/random-optimal//seed-2.txt"}, "run_context": {"base-lb-scheme": "random", "base-timing-scheme": "random", "base-ring-mode": "random", "compared-lb-scheme": "leastloaded", "compared-timing-scheme": "cassini", "compared-ring-mode": "optimal", "random-rep-count": 1, "interesting-metrics": ["avg_ar_time", "avg_iter_time"], "experiment-seed": 58, "oversub": 4, "exp-uuid": 5, "output-file": "results/sweep/1223-nethint_LB+leastloaded_TS+cassini_R+optimal_4_58/exp_outputs/output-5.txt", "perfect_lb": false, "ideal_network": false, "original_mult": 1, "original_core_count": 2, "original_ring_mode": "optimal", "original_timing_scheme": "cassini"}}'
        else: 
            input_data = json.load(sys.stdin)

    else: 
        input_data = json.load(sys.stdin)
            
    # call the main function
    job_timings, lb_decisions = generate_timing_file(**input_data)
    
    dumped_data = {
        "job_timings": job_timings, 
        "lb_decisions": lb_decisions
    } 
    
    # write the output to stdout
    print(json.dumps(dumped_data))
    
    
    