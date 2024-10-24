# cython: language_level=3

import random
import json
from pprint import pprint 
import math 
import time 
import sys 
import copy 
from processing.flowprogress import get_job_profiles
import subprocess

####################################################################################
##################  HELPER FUNCTIONS  ##############################################
####################################################################################

def log_results(run_context, key, value):
    # print to stderr first  
    sys.stderr.write(f"KEY: {key}\n")
    sys.stderr.write(f"VALUE: {value}\n")   

    with open(run_context["output-file"], "a+") as f:
        f.write("Results for: " + key + "\n\n")
        pprint(value, f) 
        f.write("\n\n---------------------------------\n\n")   
        
      
def get_job_signal_info(jobs, options, run_context):
    job_signal_infos = {}
    
    for job in jobs: 
        machines = job["machines"]
        job_id = job["job_id"] 
        job_machine_count = len(machines) 

        all_reduce_flow_size = job["comm_size"] / (job_machine_count)
        fct = int(math.ceil(all_reduce_flow_size / options["link-bandwidth"])) 
        all_reduce_length = fct * (job_machine_count - 1) * 2 # number of stages in an all-reduce. 
        duration = all_reduce_length
        period = job["comp_size"] * job["layer_count"] * 2 + all_reduce_length
        
        job_signal_infos[job_id] = {
            "duration": duration,
            "period": period
        }
        
    log_results(run_context, "job_signal_infos", job_signal_infos)
    
    return job_signal_infos


def inc_timing(jobs, options, run_context, config_sweeper):
    job_timings = [] 
    
    for job in jobs:
        job_timing = 400 * (job["job_id"] - 1)
    
        job_timings.append({
            "initial_wait": job_timing,
            "job_id": job["job_id"]
        })     
        
    return job_timings


def random_timing(jobs, options, run_context, config_sweeper):
    job_timings = [] 
    job_infos = get_job_signal_info(jobs, options, run_context)

    for job in jobs:
        job_id = job["job_id"] 
        job_timing = random.randint(0, job_infos[job_id]["period"] - 1)
    
        job_timings.append({
            "initial_wait": job_timing,
            "job_id": job_id
        })     
        
    return job_timings
    
    
def zero_timing(jobs, options, run_context, config_sweeper):
    job_timings = [] 
    
    for job in jobs:
        job_timing = 0
    
        job_timings.append({
            "initial_wait": job_timing,
            "job_id": job["job_id"]
        })     
        
    return job_timings


################################################################################################
################ CASINI TIMING #################################################################
################################################################################################



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
    
        
def evaluate_candidate(job_loads, deltas, run_context, link_logical_bandwidth):
    
    sim_length = run_context["cassini-parameters"]["sim-length"] 
    
    sum_signal = [0] * sim_length
            
    for job_load in job_loads:
        job_id = job_load["job_id"]
        current_time = get_delta_for_job_in_decisions(deltas, job_id)   

        while current_time < sim_length:
            on_length = job_load["duration"]
            off_length = job_load["period"] - on_length
            
            for j in range(on_length):
                sum_signal[current_time] += job_load["load"]
                                   
                current_time += 1
                if current_time >= sim_length:
                    break
            
            current_time += off_length  


    # max_util is the maximum utilization of the link.
    # the higher the score, the better.
    max_util = max(sum_signal) 
    max_util_score = (link_logical_bandwidth - max_util) / link_logical_bandwidth

    # compat_score is the fraction of the time the link is not saturated.
    # the higher the score, the better. 
    compat_score = 0 
    for i in range(sim_length): 
        if sum_signal[i] < link_logical_bandwidth:
            compat_score += 1
    compat_score = compat_score / sim_length

    
    return (max_util_score, compat_score)
    
def solve_for_link(job_loads, link_logical_bandwidth, run_context, fixed_prefs=None):
    ls_candidates = run_context["cassini-parameters"]["link-solution-candidate-count"]
    ls_rand_quantum = run_context["cassini-parameters"]["link-solution-random-quantum"]
    ls_top_candidates = run_context["cassini-parameters"]["link-solution-top-candidates"]    
    
    if len(job_loads) == 0: 
        return [([], 0, 0)] * ls_top_candidates
    
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
        
        # untested code -> fixed_prefs
        if fixed_prefs is not None:
            for job_id, delta in fixed_prefs:
                if job_id in involved_jobs:
                    set_delta_for_job_in_decisions(deltas, job_id, delta)
        
        max_util_score, compat_score = evaluate_candidate(job_loads, deltas, run_context, link_logical_bandwidth)
        
        delta_scores.append((deltas, max_util_score, compat_score))

    def this_sort(x):
        # best compat score, among those, lowest max_util
        return (x[2], x[1])

    good_deltas = sorted(delta_scores, key=this_sort, reverse=True)
    results = good_deltas[:ls_top_candidates]

    return results

    
    
    
def get_link_loads(jobs, options, job_signal_infos, run_context):
    
    servers_per_rack = options["ft-server-per-rack"]
    rack_count = options["machine-count"] // servers_per_rack   
    
    link_loads = []
    
    for i in range(rack_count):
        this_rack = {"up": [], "down": []}
        link_loads.append(this_rack)

    
    cross_rack_jobs = [] 
    
    for job in jobs: 
        job_has_cross_rack = False
        
        machines = job["machines"]
        job_id = job["job_id"] 

        # there's a ring through these machines 
        this_job_loads = []
        for i in range(rack_count): 
            this_rack = {"up": 0, "down": 0}
            this_job_loads.append(this_rack)
            
        for i in range(len(machines)):
            src_machine = machines[i] 
            dest_machine = machines[(i + 1) % len(machines)]
            
            src_rack = src_machine // servers_per_rack
            dest_rack = dest_machine // servers_per_rack

            if src_rack == dest_rack:
                continue
                
            job_has_cross_rack = True
            
            this_job_loads[src_rack]["up"] += 1 
            this_job_loads[dest_rack]["down"] += 1
        
        
        if job_has_cross_rack:
            cross_rack_jobs.append(job_id)
            
        duration = job_signal_infos[job_id]["duration"] 
        period = job_signal_infos[job_id]["period"]
                
        for i in range(rack_count):
            if this_job_loads[i]["up"] > 0:            
                link_loads[i]["up"].append({
                    "load": this_job_loads[i]["up"],
                    "job_id": job_id, 
                    "duration": duration, 
                    "period": period,
                }) 
                
            if this_job_loads[i]["down"] > 0:
                link_loads[i]["down"].append({
                    "load": this_job_loads[i]["down"],
                    "job_id": job_id, 
                    "duration": duration, 
                    "period": period,
                })  
                
    log_results(run_context, "link_loads", link_loads)
                
    return link_loads, cross_rack_jobs


def cassini_timing_old(jobs, options, run_context, config_sweeper):
    overall_solution_candidate_count = run_context["cassini-parameters"]["overall-solution-candidate-count"]
    
    start_time = time.time()
    
    servers_per_rack = options["ft-server-per-rack"]
    rack_count = options["machine-count"] // servers_per_rack       
    link_logical_bandwidth = options["ft-core-count"]
    
    log_results(run_context, "jobs", jobs)
        
    job_signal_infos = get_job_signal_info(jobs, options, run_context)
    link_loads, cross_rack_jobs = get_link_loads(jobs, options, job_signal_infos, run_context)   

    link_solutions = [] 
    for rack in range(rack_count):
        for direction in ["up", "down"]:
            link_solution_candidates = solve_for_link(link_loads[rack][direction], link_logical_bandwidth, run_context)
            log_title = f"solutions for rack {rack} {direction}"
            log_results(run_context, log_title, link_solution_candidates)
            link_solutions.append(link_solution_candidates)            
            
    best_candidate_score = -1e9 
    best_candidate = None

    for i in range(overall_solution_candidate_count):

        # shuffle the link_solutions. No real difference beetwen the links.
        random.shuffle(link_solutions)
        solutions_count = len(link_solutions)  

        # pick the top solution for the first link (which is an arbitrary choice)
        current_decisions = link_solutions[0][0][0]
        
        # go through the rest of the links. 
        for j in range(1, solutions_count):
            # now we are looking for the candidate solutions for one of the links. 
            # it doesn't really matter which link. 
            
            # go through the solutions for this link, find the one that's closest to the current decisions
            this_link_best_score = -1e9
            this_link_best_solution = None
            
            for solution in link_solutions[j]:
                this_link_this_solution_score = 0
                timings = solution[0] 
                
                for timing in timings:
                    job_id = timing[0] 
                    job_delta = timing[1] 

                    current = get_delta_for_job_in_decisions(current_decisions, job_id)
                    
                    if current is None:
                        diff = 0 
                    else:                        
                        diff = abs(job_delta - current)
                        
                    this_link_this_solution_score += diff
                                        
                if this_link_this_solution_score > this_link_best_score:
                    this_link_best_score = this_link_this_solution_score
                    this_link_best_solution = solution
            
            
            for timing in this_link_best_solution[0]:
                job_id = timing[0]
                delta = timing[1]
                
                job_period = job_signal_infos[job_id]["period"]
                
                current_decision = get_delta_for_job_in_decisions(current_decisions, job_id)
                
                if current_decision is None:
                    new_decision = delta
                else:                
                    # new_decision = (current_decision + delta) // 2
                    new_decision = current_decision
                
                new_decision = new_decision % job_period 
                
                set_delta_for_job_in_decisions(current_decisions, job_id, new_decision)


        for i in range(len(current_decisions)):
            job_id = current_decisions[i][0]
            timing = current_decisions[i][1]
            job_period = job_signal_infos[job_id]["period"] 

            current_decisions[i] = (job_id, timing % job_period)       
        
        
        # now we have a candidate solution. We should evaluate it. 
        candidate_score = 0 
        for rack in range(rack_count):
            for direction in ["up", "down"]:
                max_util_score, compat_score = evaluate_candidate(job_loads=link_loads[rack][direction], 
                                                                  deltas=current_decisions, 
                                                                  run_context=run_context,
                                                                  link_logical_bandwidth=link_logical_bandwidth)

                candidate_score += compat_score
                candidate_score += max_util_score
                
        log_results(run_context, "candidate", (current_decisions, candidate_score))
        
        if candidate_score > best_candidate_score:
            best_candidate_score = candidate_score
            best_candidate = current_decisions

    # some job timings are negative now. 
    
    job_timings = [] 
    
    log_results(run_context, "best_candidate", (best_candidate, best_candidate_score))
    
    for job in jobs:
        job_id = job["job_id"]
        
        timing = get_delta_for_job_in_decisions(best_candidate, job_id)
        
        if timing is None:
            # it could be that the job has no intra-rack communication.
            # therefore, it's doesn't appear anywhere in the decisions. 
            timing = 0
            
        job_timings.append({
            "initial_wait": timing,
            "job_id": job_id
        })           
    
    
    end_time = time.time() 
    time_taken = end_time - start_time 
    log_results(run_context, "time_taken", time_taken)  
        
    return job_timings 
        
        

def cassini_timing(jobs, options, run_context, config_sweeper): 
    overall_solution_candidate_count = run_context["cassini-parameters"]["overall-solution-candidate-count"]

    start_time = time.time()
    
    servers_per_rack = options["ft-server-per-rack"]
    rack_count = options["machine-count"] // servers_per_rack       
    link_logical_bandwidth = options["ft-core-count"]
    
    log_results(run_context, "jobs", jobs)
        
    job_signal_infos = get_job_signal_info(jobs, options, run_context)
    link_loads, cross_rack_jobs = get_link_loads(jobs, options, job_signal_infos, run_context)   

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
        
        log_results(run_context, f"link_solutions_0_candidate_{i}", current_decisions)

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

            log_results(run_context, f"link_solutions_{j}_candidate_{i}", current_decisions)
            
            if len(current_decisions) == len(cross_rack_jobs):
                break
        
        
        # now we have a candidate solution. We should evaluate it. 
        candidate_score = 0 
        for rack in range(rack_count):
            for direction in ["up", "down"]:
                max_util_score, compat_score = evaluate_candidate(job_loads=link_loads[rack][direction], 
                                                                  deltas=current_decisions, 
                                                                  run_context=run_context,
                                                                  link_logical_bandwidth=link_logical_bandwidth)

                candidate_score += compat_score
                candidate_score += max_util_score
                
        log_results(run_context, "candidate", (current_decisions, candidate_score))
        
        if candidate_score > best_candidate_score:
            best_candidate_score = candidate_score
            best_candidate = current_decisions

    # some job timings are negative now. 
    
    job_timings = [] 
    
    log_results(run_context, "best_candidate", (best_candidate, best_candidate_score))
    
    for job in jobs:
        job_id = job["job_id"]
        
        timing = get_delta_for_job_in_decisions(best_candidate, job_id)
        
        if timing is None:
            # it could be that the job has no intra-rack communication.
            # therefore, it's doesn't appear anywhere in the decisions. 
            timing = 0
            
        job_timings.append({
            "initial_wait": timing,
            "job_id": job_id
        })           
    
    
    end_time = time.time() 
    time_taken = end_time - start_time 
    log_results(run_context, "time_taken", time_taken)  
        
    return job_timings 
        
        
################################################################################################
################ Farid TIMING #################################################################
################################################################################################


def evaluate_candidate_cpp(job_loads, deltas, run_context, link_logical_bandwidth):
    # Prepare JSON input
    input_data = {
        "job_loads": job_loads,
        "deltas": deltas,
    }
        
    # Convert the input data to JSON string
    json_input = json.dumps(input_data)
    sim_length = run_context["cassini-parameters"]["sim-length"] 

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
    
    print(f"max_util_score: {max_util_score}, compat_score: {compat_score}")
    
    return max_util_score, compat_score


def evaluate_candidate_farid(job_loads, deltas, run_context, link_logical_bandwidth):
    
    sim_length = run_context["cassini-parameters"]["sim-length"] 
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
        if sum_signal[i] < link_logical_bandwidth:
            compat_score += 1
    compat_score = compat_score / sim_length

    print(f"max_util_score: {max_util_score}, compat_score: {compat_score}")
    return (max_util_score, compat_score)
    
    
def solve_for_link_farid(job_loads, link_logical_bandwidth, run_context, fixed_prefs=None):
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
            print("All fixed decisions")
            return [(deltas, 0, 0)]
        
        max_util_score, compat_score = evaluate_candidate_cpp(job_loads, deltas, 
                                                              run_context, 
                                                              link_logical_bandwidth)
        
        delta_scores.append((deltas, max_util_score, compat_score))

    def this_sort(x):
        # best compat score, among those, lowest max_util
        return (x[2], x[1])

    good_deltas = sorted(delta_scores, key=this_sort, reverse=True)
    results = good_deltas[:ls_top_candidates]

    pprint(results)
    
    return results

def get_link_loads_farid(jobs, options, run_context, job_profiles):
    
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
        job_period = job_profile["period"]  
        
        for flow in job_profile["flows"]:   
            for i in range(len(flow["progress_history"])):
                flow["progress_history"][i] /= link_bandwidth
        
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
                    flow_progress_history = flow["progress_history"]
                    
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
                        "load": link_job_load_combined,
                        "period": job_period
                    })
                    
                    
    cross_rack_jobs = list(cross_rack_jobs_set) 
    return link_loads, cross_rack_jobs



def farid_cassini_helper(jobs, options, run_context, config_sweeper, job_profiles): 
    
    start_time = time.time()    
    
    overall_solution_candidate_count = run_context["cassini-parameters"]["overall-solution-candidate-count"]
    
    servers_per_rack = options["ft-server-per-rack"]
    rack_count = options["machine-count"] // servers_per_rack       
    link_logical_bandwidth = options["ft-core-count"]
    
    log_results(run_context, "jobs", jobs)
    
    link_loads, cross_rack_jobs = get_link_loads_farid(jobs, options, run_context, job_profiles)   

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
        solutions = solve_for_link_farid(first_link_loads, link_logical_bandwidth, run_context)
        r = random.randint(0, len(solutions) - 1)
        top_solution = solutions[r]
        current_decisions = top_solution[0]
        
        log_results(run_context, f"link_solutions_0_candidate_{i}", current_decisions)

        # go through the rest of the links. 
        for j in range(1, len(link_loads_list)):
            this_link_loads = link_loads_list[j]
            
            link_solutions = solve_for_link_farid(this_link_loads, link_logical_bandwidth, run_context, 
                                                  fixed_prefs=current_decisions)
            
            r = random.randint(0, len(link_solutions) - 1)
            top_solution = link_solutions[r]
            top_solution_timing = top_solution[0]
            
            # update the current decisions.
            for job_id, delta in top_solution_timing:
                set_delta_for_job_in_decisions(current_decisions, job_id, delta)

            log_results(run_context, f"link_solutions_{j}_candidate_{i}", current_decisions)
            
            if len(current_decisions) == len(cross_rack_jobs):
                break
        
        
        # now we have a candidate solution. We should evaluate it. 
        candidate_score = 0 
        for rack in range(rack_count):
            for direction in ["up", "down"]:
                max_util_score, compat_score = evaluate_candidate_cpp(job_loads=link_loads[rack][direction], 
                                                                      deltas=current_decisions, 
                                                                      run_context=run_context,
                                                                      link_logical_bandwidth=link_logical_bandwidth)

                candidate_score += compat_score
                candidate_score += max_util_score
                
        log_results(run_context, "candidate", (current_decisions, candidate_score))
        
        if candidate_score > best_candidate_score:
            best_candidate_score = candidate_score
            best_candidate = current_decisions

    # some job timings are negative now. 
    
    job_timings = [] 
    
    log_results(run_context, "best_candidate", (best_candidate, best_candidate_score))
    
    for job in jobs:
        job_id = job["job_id"]
        
        timing = get_delta_for_job_in_decisions(best_candidate, job_id)
        
        if timing is None:
            # it could be that the job has no intra-rack communication.
            # therefore, it's doesn't appear anywhere in the decisions. 
            timing = 0
            
        job_timings.append({
            "initial_wait": timing,
            "job_id": job_id
        })           
    
    
    end_time = time.time() 
    time_taken = end_time - start_time 
    log_results(run_context, "time_taken", time_taken)  
        
    return job_timings 



def farid_timing(jobs, options, run_context, config_sweeper):
    # step 1: profile the jobs. Run them in isolation and get their flows and periods.
    job_profiles = {}
    
    for job in jobs:
        profiling_job_options = copy.deepcopy(options)  
        profiling_job_options["isolate-job-id"] = job["job_id"]
        profiling_job_options["print-flow-progress-history"] = True
        profiling_job_options["timing-file"] = None  
        profiling_job_options["ft-core-count"] = 1  
        profiling_job_options["ft-agg-core-link-capacity-mult"] = 100

        output = config_sweeper.only_run_command_with_options(profiling_job_options)
        path = "{}/worker-{}/run-1/flow-info.txt".format(config_sweeper.workers_dir, 
                                                         run_context["worker-id-for-profiling"]) 
        job_prof, _, _ = get_job_profiles(path)
        
        # job_prof might be empty.
        job_id = job["job_id"]
        if job_id in job_prof:
            print("job_prof period: ", job_prof[job_id]["period"])
            job_profiles[job_id] = job_prof[job_id]
            job["period"] = job_prof[job_id]["period"]
            
    # step 2: run cassini timing with the job profiles, find some timings for the jobs.  
    farid_cassini_helper(jobs, options, run_context, config_sweeper, job_profiles)
    
    # step 3: do the routing for the flows. 
    
    # step 4: return the full schedule.  
    job_timings = [] 
    return job_timings

############################################################################
################ MAIN FUCTION  #############################################
############################################################################

def generate_timing_file(timing_file_path, placement_seed, 
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
    
    if timing_scheme not in timing_funcions:
        raise ValueError(f"Invalid timing-scheme: {timing_scheme}")
    
    timing_func = timing_funcions[timing_scheme] 
    job_timings = timing_func(jobs, options, run_context, config_sweeper)
    
    with open(timing_file_path, "w") as f:
        json.dump(job_timings, f, indent=4)
        f.flush() 

    return job_timings

if __name__ == "__main__":
    if len(sys.argv) == 2:
        if sys.argv[1] == "--test":
            input_data = '{"timing_file_path": "results/sweep/1223-nethint_LB+leastloaded_TS+cassini_R+optimal_4_58/custom_files//timings/random-optimal/2//cassini.txt", "placement_seed": 2, "jobs": [{"job_id": 1, "machine_count": 6, "comm_size": 10000, "comp_size": 1000, "layer_count": 1, "iter_count": 20, "machines": [2, 4, 7, 8, 17, 22]}, {"job_id": 2, "machine_count": 7, "comm_size": 10000, "comp_size": 500, "layer_count": 1, "iter_count": 20, "machines": [5, 10, 21, 24, 26, 29, 31]}, {"job_id": 3, "machine_count": 5, "comm_size": 20000, "comp_size": 500, "layer_count": 1, "iter_count": 20, "machines": [1, 16, 18, 19, 25]}, {"job_id": 4, "machine_count": 4, "comm_size": 10000, "comp_size": 1000, "layer_count": 1, "iter_count": 20, "machines": [3, 15, 23, 30]}, {"job_id": 5, "machine_count": 8, "comm_size": 20000, "comp_size": 1000, "layer_count": 1, "iter_count": 20, "machines": [0, 6, 9, 11, 12, 20, 27, 28]}, {"job_id": 6, "machine_count": 2, "comm_size": 20000, "comp_size": 1000, "layer_count": 1, "iter_count": 20, "machines": [13, 14]}], "options": {"step-size": 1, "core-status-profiling-interval": 100000, "rep-count": 1, "console-log-level": 4, "file-log-level": 3, "initial-rate": 100, "min-rate": 100, "drop-chance-multiplier": 0, "rate-increase": 1, "priority-allocator": "maxmin", "network-type": "leafspine", "link-bandwidth": 100, "ft-rack-per-pod": 1, "ft-agg-per-pod": 1, "ft-pod-count": -1, "ft-server-tor-link-capacity-mult": 1, "ft-tor-agg-link-capacity-mult": 1, "ft-agg-core-link-capacity-mult": 1, "shuffle-device-map": false, "regret-mode": "none", "machine-count": 32, "ft-server-per-rack": 8, "general-param-1": 4, "general-param-3": 8, "simulation-seed": 58, "protocol-file-name": "nethint-test", "lb-scheme": "random", "timing-scheme": "cassini", "ring-mode": "optimal", "placement-mode": "random", "ft-core-count": 2, "placement-seed": 2, "load-metric": "utilization", "placement-file": "results/sweep/1223-nethint_LB+leastloaded_TS+cassini_R+optimal_4_58/custom_files//placements/random-optimal//seed-2.txt"}, "run_context": {"base-lb-scheme": "random", "base-timing-scheme": "random", "base-ring-mode": "random", "compared-lb-scheme": "leastloaded", "compared-timing-scheme": "cassini", "compared-ring-mode": "optimal", "random-rep-count": 1, "interesting-metrics": ["avg_ar_time", "avg_iter_time"], "experiment-seed": 58, "oversub": 4, "exp-uuid": 5, "output-file": "results/sweep/1223-nethint_LB+leastloaded_TS+cassini_R+optimal_4_58/exp_outputs/output-5.txt", "perfect_lb": false, "ideal_network": false, "original_mult": 1, "original_core_count": 2, "original_ring_mode": "optimal", "original_timing_scheme": "cassini"}}'
        else: 
            input_data = json.load(sys.stdin)

    else: 
        input_data = json.load(sys.stdin)
            
    # call the main function
    job_timings = generate_timing_file(**input_data)
    
    # write the output to stdout
    print(json.dumps(job_timings))
    
    
    