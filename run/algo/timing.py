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

####################################################################################
##################  HELPER FUNCTIONS  ##############################################
####################################################################################

EVAL_MODE = "cpp"


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
    job_profiles = profile_all_jobs(jobs, options, run_context, config_sweeper)

    pprint(jobs)
    
    for job in jobs:
        job_id = job["job_id"] 
        
        pprint(job_profiles.keys()) 
        job_timing = random.randint(0, job_profiles[job_id]["period"] - 1)
    
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

# TODO: it might be a good idea to be able to store the flow progress history in a file, so that
# I can later write the C++ code to read from that file and do the whole schudling thing in C++.

def profile_all_jobs(jobs, options, run_context, config_sweeper, stretch_factor=1):
    job_profiles = {}
    
    for job in jobs:
        profiling_job_options = copy.deepcopy(options)  
        profiling_job_options["isolate-job-id"] = job["job_id"]
        profiling_job_options["print-flow-progress-history"] = True
        profiling_job_options["timing-file"] = None  
        profiling_job_options["ft-core-count"] = 1  
        profiling_job_options["ft-agg-core-link-capacity-mult"] = 100
        profiling_job_options["lb-scheme"] = "random"   
        profiling_job_options["worker-id"] = run_context["worker-id-for-profiling"]
        profiling_job_options["stretch-factor"] = stretch_factor 

        output = config_sweeper.only_run_command_with_options(run_context, profiling_job_options)
        
        path = "{}/worker-{}/run-1/flow-info.txt".format(config_sweeper.workers_dir, 
                                                         run_context["worker-id-for-profiling"]) 
        
        
        job_prof, _, _ = get_job_profiles(path)
        
        # job_prof might be empty.
        job_id = job["job_id"]
        if job_id in job_prof:
            print("job_prof period: ", job_prof[job_id]["period"])
            job_profiles[job_id] = job_prof[job_id]
            job["period"] = job_prof[job_id]["period"]
            
            if "save-profiles" in run_context["cassini-parameters"]:
                if run_context["cassini-parameters"]["save-profiles"]:  
                    with open(f"{run_context['timing-extra-files-dir']}/job-{job_id}-flows.json", "w") as f:
                        copied_job_prof = copy.deepcopy(job_prof[job_id])
                        for flow in copied_job_prof["flows"]:
                            del flow["progress_history"]
                        json.dump(copied_job_prof, f, indent=4)
                        
        else:
            job_profiles[job_id] = {"period": 1000, "flows": []}
            job["period"] = 1000               

    return job_profiles 


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

    
def evaluate_candidate(job_loads, deltas, run_context, link_logical_bandwidth):
    if len(job_loads) == 0:
        return (1, 1)
    
    periods = [job["period"] * job["iter_count"] for job in job_loads] 
    hyperperiod = max(periods)  + max([d[1] for d in deltas])
    eval_length = min(hyperperiod, run_context["sim-length"]) 
    
    if EVAL_MODE == "cpp":
        return evaluate_candidate_cpp(job_loads, deltas, run_context, link_logical_bandwidth, eval_length)
    else:
        return evaluate_candidate_python(job_loads, deltas, run_context, link_logical_bandwidth, eval_length)
        

def evaluate_candidate_cpp(job_loads, deltas, run_context, link_logical_bandwidth, eval_length):
    # Prepare JSON input
    input_data = {
        "job_loads": job_loads,
        "deltas": deltas,
    }
        
    # Convert the input data to JSON string
    json_input = json.dumps(input_data)
    # sim_length = run_context["cassini-parameters"]["sim-length"] 
    sim_length = eval_length
    
    # Run the C++ executable using subprocess
    process = subprocess.Popen(
        ['./algo/evaluate/solve', str(sim_length), str(link_logical_bandwidth)],  # Path to your compiled C++ executable
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
    max_util_score, compat_score = map(float, stdout.split())
    
    return max_util_score, compat_score


# counter = 0 
def evaluate_candidate_python(job_loads, deltas, run_context, link_logical_bandwidth, eval_length):
    # sim_length = run_context["cassini-parameters"]["sim-length"]
    sim_length = eval_length    
     
    sum_signal = [0] * sim_length
            
    for job_load in job_loads:
        job_id = job_load["job_id"]
        current_time = get_delta_for_job_in_decisions(deltas, job_id)   

        while current_time < sim_length:
            for j in range(len(job_load["load"])):
                sum_signal[current_time] += job_load["load"][j]

                current_time += 1
                if current_time >= sim_length:
                    break

    # max_util is the maximum utilization of the link.
    # the higher the score, the better.
    max_util = max(sum_signal) 
    max_util_score = (link_logical_bandwidth - max_util) / link_logical_bandwidth
    
    # compat_score is the fraction of the time the link is not saturated.
    # the higher the score, the better. 
    compat_score = 0 
    for i in range(sim_length): 
        if sum_signal[i] <= link_logical_bandwidth:
            compat_score += 1
    compat_score = compat_score / sim_length
    
    # global counter
    # counter += 1
    # import matplotlib.pyplot as plt 
    # plt.plot(sum_signal)
    # plt.savefig(f"plots/wtf/sum_signal_{counter}.png")
    # plt.clf()    

    print(f"PYTHON: max_util_score: {max_util_score}, compat_score: {compat_score}")
    return (max_util_score, compat_score)
    
    
def solve_for_link(job_loads, link_logical_bandwidth, run_context, fixed_prefs=None):
    ls_candidates = run_context["cassini-parameters"]["link-solution-candidate-count"]
    ls_rand_quantum = run_context["cassini-parameters"]["link-solution-random-quantum"]
    ls_top_candidates = run_context["cassini-parameters"]["link-solution-top-candidates"]    
    
    if len(job_loads) == 0: 
        return [([], 0, 0)] * ls_top_candidates
    
    if len(job_loads) == 1:
        return [([(job_loads[0]["job_id"], 0)], 0, 0)]
        
    delta_scores = [] 
    involved_jobs = set([job["job_id"] for job in job_loads])
    
    for i in range(ls_candidates):
        deltas = [] 
        
        for job in job_loads: 
            rand_options = int(math.ceil(job["period"] / ls_rand_quantum))
            r = random.randint(0, rand_options) * ls_rand_quantum        
            deltas.append((job["job_id"], r)) 
            
        min_delta = min([x[1] for x in deltas])
        deltas = [(x[0], x[1] - min_delta) for x in deltas]
        
        number_of_fixed_decisions = 0 
        
        if fixed_prefs is not None:
            for job_id, delta in fixed_prefs:
                if job_id in involved_jobs:
                    number_of_fixed_decisions += 1
                    set_delta_for_job_in_decisions(deltas, job_id, delta)
        
        if number_of_fixed_decisions == len(involved_jobs):
            # print("All fixed decisions")
            return [(deltas, 0, 0)]
        
        max_util_score, compat_score = evaluate_candidate(job_loads, deltas, 
                                                          run_context, 
                                                          link_logical_bandwidth)
        
        delta_scores.append((deltas, max_util_score, compat_score))

    def this_sort(x):
        # best compat score, among those, lowest max_util
        return (x[2], x[1])

    good_deltas = sorted(delta_scores, key=this_sort, reverse=True)
    results = good_deltas[:ls_top_candidates]

    # pprint(results)
    
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

def visualize_link_loads(link_loads, run_context, deltas=None, repeat_iterations=None, suffix=""): 
    import matplotlib.pyplot as plt
    import numpy as np
    import os

    # Ensure the directory exists
    os.makedirs(run_context['timing-extra-files-dir'], exist_ok=True)

    num_racks = len(link_loads)
    num_directions = 2  # "up" and "down"

    # Create a figure and subplots
    fig, axes = plt.subplots(num_racks, num_directions, figsize=(10, 3 * num_racks), squeeze=False)

    # Convert deltas to a dictionary for easy lookup if deltas is not None
    delta_dict = dict(deltas) if deltas is not None else {}

    for rack in range(num_racks):
        for i, direction in enumerate(["up", "down"]):
            ax = axes[rack][i]
            ax.set_title(f"Rack: {rack}, Direction: {direction}")

            job_ids = [job_load["job_id"] for job_load in link_loads[rack][direction]]
            job_loads = [job_load["load"] for job_load in link_loads[rack][direction]]
            job_iter_counts = [job_load["iter_count"] for job_load in link_loads[rack][direction]]  
            
            if job_loads:  # Check if there are any loads to plot
                repeated_job_loads = []
                for job_id, job_load, job_iter_count in zip(job_ids, job_loads, job_iter_counts):   
                    # Repeat the load
                    
                    repeat_count_final = None 
                    if repeat_iterations is not None:
                        repeat_count_final = repeat_iterations
                    else:
                        repeat_count_final = job_iter_count
                        
                    repeated_load = np.tile(job_load, repeat_count_final)
                    # Shift the load by the delta amount if deltas are provided
                    shift_amount = delta_dict.get(job_id, 0)
                    shifted_load = np.roll(repeated_load, shift_amount)
                    # Zero out the initial values based on the shift
                    if shift_amount > 0:
                        shifted_load[:shift_amount] = 0
                    repeated_job_loads.append(shifted_load)

                # Find the maximum length of the repeated and shifted job loads
                max_length = max(len(job_load) for job_load in repeated_job_loads)

                # Pad repeated and shifted job loads with zeros to make them all the same length
                padded_job_loads = [
                    np.pad(job_load, (0, max_length - len(job_load)), mode='constant')
                    for job_load in repeated_job_loads
                ]

                # Convert the padded job loads to a 2D array
                job_loads_array = np.array(padded_job_loads)

                ax.stackplot(range(max_length), job_loads_array, labels=[f"Job: {job_id}" for job_id in job_ids])

            ax.set_xlabel("Time")
            ax.set_ylabel("Load")
            ax.legend(loc='upper left')

    plt.tight_layout()

    # Save the entire figure
    plt.savefig(f"{run_context['timing-extra-files-dir']}/stacked_racks_directions{suffix}.png")
    plt.close(fig)

    
def get_timeshifts(jobs, options, run_context, config_sweeper, job_profiles): 
    start_time = time.time()    
    
    overall_solution_candidate_count = run_context["cassini-parameters"]["overall-solution-candidate-count"]
    
    servers_per_rack = options["ft-server-per-rack"]
    rack_count = options["machine-count"] // servers_per_rack       
    link_logical_bandwidth = options["ft-core-count"]
    
    # log_results(run_context, "jobs", jobs)
    link_loads, cross_rack_jobs = get_link_loads(jobs, options, run_context, job_profiles)   

    visualize_link_loads(link_loads, run_context)
    
    best_candidate_score = -1e9 
    best_candidate = None

    link_loads_list = [] 
    for rack in range(rack_count):
        for direction in ["up", "down"]:
            link_loads_list.append(link_loads[rack][direction])
                
    for i in range(overall_solution_candidate_count):
        # shuffle the link_solutions. No real difference beetwen the links.
        random.shuffle(link_loads_list)

        # pick the top solution for the first link (which is an arbitrary choice)
        first_link_loads = link_loads_list[0]
        solutions = solve_for_link(first_link_loads, link_logical_bandwidth, run_context)
        
        r = random.randint(0, len(solutions) - 1)
        top_solution = solutions[r]
        current_decisions = top_solution[0]
        
        # log_results(run_context, f"link_solutions_0_candidate_{i}", current_decisions)

        # go through the rest of the links. 
        for j in range(1, len(link_loads_list)):
            this_link_loads = link_loads_list[j]
            
            link_solutions = solve_for_link(this_link_loads, link_logical_bandwidth, run_context, 
                                                  fixed_prefs=current_decisions)
            
            r = random.randint(0, len(link_solutions) - 1)
            top_solution = link_solutions[r]
            top_solution_timing = top_solution[0]
            
            # update the current decisions.
            for job_id, delta in top_solution_timing:
                set_delta_for_job_in_decisions(current_decisions, job_id, delta)

            # log_results(run_context, f"link_solutions_{j}_candidate_{i}", current_decisions)
            if len(current_decisions) == len(cross_rack_jobs):
                break
        
        visualize_link_loads(link_loads, run_context, current_decisions, suffix=f"_{i}")
        
        # now we have a candidate solution. We should evaluate it. 
        candidate_score = 0 
        for rack in range(rack_count):
            for direction in ["up", "down"]:
                max_util_score, compat_score = evaluate_candidate(job_loads=link_loads[rack][direction], 
                                                                  deltas=current_decisions, 
                                                                  run_context=run_context,
                                                                  link_logical_bandwidth=link_logical_bandwidth)

                candidate_score += compat_score
                # candidate_score += max_util_score
                
                # print(f"Rack: {rack}, Direction: {direction}, max_util_score: {max_util_score}, compat_score: {compat_score}")  
                
        log_results(run_context, "candidate", (current_decisions, candidate_score))
        
        if candidate_score > best_candidate_score:
            best_candidate_score = candidate_score
            best_candidate = current_decisions

    perfection_solution_found = False 
    max_possible_score = rack_count * 2 
    if best_candidate_score == max_possible_score: 
        perfection_solution_found = True
        
    # some job timings are negative now. 
    
    job_timings = [] 
    
    log_results(run_context, "best_candidate", (best_candidate, best_candidate_score))
    
    for job in jobs:
        job_id = job["job_id"]
        timing = get_delta_for_job_in_decisions(best_candidate, job_id)
        if timing is None:
            # it could be that the job has no intra-rack communication.
            # therefore, it's doesn't appear anywhere in the decisions. 
            # we'll just give it a zero. 
            timing = 0
            
        job_timings.append({
            "initial_wait": timing,
            "job_id": job_id
        })           
    
    
    end_time = time.time() 
    time_taken = end_time - start_time 
    log_results(run_context, "time_taken", time_taken)  
        
    return job_timings, perfection_solution_found



def cassini_timing(jobs, options, run_context, config_sweeper, timing_scheme):
    job_profiles = profile_all_jobs(jobs, options, run_context, config_sweeper)
    
    job_timings, perfect = get_timeshifts(jobs, options, run_context, config_sweeper, job_profiles)

    return job_timings, None

################################################################################################
################ Farid TIMING #################################################################
################################################################################################

def farid_timing(jobs, options, run_context, config_sweeper, timing_scheme):
    # step 1: profile the jobs 
    job_profiles = profile_all_jobs(jobs, options, run_context, config_sweeper)    
    
    # step 2: run cassini timing with the job profiles, find some timings for the jobs.  
    job_timings, perfect = get_timeshifts(jobs, options, run_context, config_sweeper, job_profiles)
    
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


    # get the base dir for the timing_file_path
    timing_file_dir = os.path.dirname(timing_file_path)
    timing_extra_files_dir_name = run_context["timing-scheme"] + "-" + "files"
    os.makedirs(f"{timing_file_dir}/{timing_extra_files_dir_name}", exist_ok=True)  
    run_context["timing-extra-files-dir"] = f"{timing_file_dir}/{timing_extra_files_dir_name}"

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
    
    
    