
import random
import json
from pprint import pprint 
import math 
import time 


def inc_timing(jobs, options, run_context):
    job_timings = [] 
    
    for job in jobs:
        job_timing = 400 * (job["job_id"] - 1)
    
        job_timings.append({
            "initial_wait": job_timing,
            "job_id": job["job_id"]
        })     
        
    return job_timings


def random_timing(jobs, options, run_context):
    job_timings = [] 
    
    for job in jobs:
        job_timing = random.randint(0, 8400)
    
        job_timings.append({
            "initial_wait": job_timing,
            "job_id": job["job_id"]
        })     
        
    return job_timings
    
    
def zero_timing(jobs, options, run_context):
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

sim_length = 100000
link_solution_candidate_count = 100
link_solution_random_quantum = 100
link_solution_top_candidates = 10    
overall_solution_candidate_count = 100

def get_delta_for_job_in_decisions(decisions, id):
    for decision in decisions:
        if decision[1] == id:
            return decision[0]

    return None 
def set_delta_for_job_in_decisions(decisions, id, delta):
    for i in range(len(decisions)):
        if decisions[i][1] == id:
            decisions[i] = (delta, id)
            return
    decisions.append((delta, id))
    
        
def evaluate_candidate(job_loads, deltas, link_logical_bandwidth): 
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

    max_util = max(sum_signal) 

    compat_score = 0 
    for i in range(sim_length): 
        if sum_signal[i] < link_logical_bandwidth:
            compat_score += 1
    compat_score = compat_score / sim_length
    
    return (max_util, compat_score)
    
def solve_for_link(job_loads, link_logical_bandwidth, fixed_prefs=None):
    if len(job_loads) == 0: 
        return [([], 0, 0)] * link_solution_top_candidates
    
    delta_scores = [] 
    
    for i in range(link_solution_candidate_count):
        
        deltas = [] 
        
        for job_load in job_loads: 
            rand_options = int(math.ceil(job_load["period"] / link_solution_random_quantum))
            r = random.randint(0, rand_options) * link_solution_random_quantum        
            deltas.append((r, job_load["job_id"])) 
            
        min_delta = min([x[0] for x in deltas])
        deltas = [(x[0] - min_delta, x[1]) for x in deltas]
        
        # untested code -> fixed_prefs
        if fixed_prefs is not None:
            for pref, delta in fixed_prefs.items():
                set_delta_for_job_in_decisions(deltas, pref, delta)
        
        max_util, compat_score = evaluate_candidate(job_loads, deltas, link_logical_bandwidth)
        
        delta_scores.append((deltas, max_util, compat_score))

    def this_sort(x):
        # best compat score, among those, lowest max_util
        return (x[2], -x[1])

    good_deltas = sorted(delta_scores, key=this_sort, reverse=True)
    results = good_deltas[:link_solution_top_candidates]

    return results

def log_results(run_context, key, value):
    with open(run_context["output-file"], "a+") as f:
        f.write("Results for: " + key + "\n\n")
        pprint(value, f) 
        f.write("\n\n---------------------------------\n\n")   
        
def cassini_timing(jobs, options, run_context):
    log_results(run_context, "jobs", jobs)
        
    servers_per_rack = options["ft-server-per-rack"]
    rack_count = options["machine-count"] // servers_per_rack   
    
    job_periods = {} 
    
    link_loads = []
    for i in range(rack_count):
        this_rack = {"up": [], "down": []}
        link_loads.append(this_rack)

    for job in jobs: 
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
                
            this_job_loads[src_rack]["up"] += 1 
            this_job_loads[dest_rack]["down"] += 1
            
        job_machine_count = len(machines) 

        all_reduce_flow_size = job["comm_size"] / (job_machine_count)
        fct = int(math.ceil(all_reduce_flow_size / options["link-bandwidth"])) 
        all_reduce_length = fct * (job_machine_count - 1) * 2 # number of stages in an all-reduce. 
        duration = all_reduce_length
        period = job["comp_size"] * job["layer_count"] * 2 + all_reduce_length
        job_periods[job_id] = period
        
        
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
    
    # now we have 2 * rack_count links for up and down directions. 
    
    log_results(run_context, "link_loads", link_loads)
        
    link_logical_bandwidth = options["ft-core-count"]
    
    link_solutions = [] 
    for rack in range(rack_count):
        for direction in ["up", "down"]:
            link_solution_candidates = solve_for_link(link_loads[rack][direction], link_logical_bandwidth)
            link_solutions.append(link_solution_candidates)            
            
    best_candidate_score = 0 
    best_candidate = None


    log_results(run_context, "link_solutions", link_solutions)
        
    
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
            this_link_best_score = -1
            this_link_best_solution = None
            
            for solution in link_solutions[j]:
                this_link_this_solution_score = 0
                timings = solution[0] 
                
                for timing in timings:
                    job_delta = timing[0] 
                    job_id = timing[1] 

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
                delta = timing[0]
                job_id = timing[1]
                job_period = job_periods[job_id]
                
                current_decision = get_delta_for_job_in_decisions(current_decisions, job_id)
                
                if current_decision is None:
                    new_decision = delta
                else:                
                    new_decision = (current_decision + delta) // 2
                
                new_decision = new_decision % job_period 
                
                set_delta_for_job_in_decisions(current_decisions, job_id, new_decision)

        for i in range(len(current_decisions)):
            timing = current_decisions[i][0] 
            job_id = current_decisions[i][1]
            job_period = job_periods[job_id] 
            current_decisions[i] = (timing % job_period, current_decisions[i][1])       
        
        
        # now we have a candidate solution. We should evaluate it. 
        candidate_score = 0 
        for rack in range(rack_count):
            for direction in ["up", "down"]:
                max_util, compat_score = evaluate_candidate(job_loads=link_loads[rack][direction], 
                                                            deltas=current_decisions, 
                                                            link_logical_bandwidth=link_logical_bandwidth)

                candidate_score += compat_score
                candidate_score += ((link_logical_bandwidth - max_util) / link_logical_bandwidth) 
                
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
    
    return job_timings 
        
        
############################################################################
################ MAIN FUCTION  #############################################
############################################################################

def generate_timing_file(timing_file_path, placement_seed, 
                         jobs, options, run_context):
    
    random.seed(run_context["experiment-seed"] + placement_seed)
    
    
    if "timing-scheme" not in options:
        raise ValueError("timing-scheme option is required")

    timing_scheme = options["timing-scheme"]    
    
    timing_funcions = {
        "inc": inc_timing,
        "random": random_timing,
        "zero": zero_timing, 
        "cassini": cassini_timing
    }
    
    if timing_scheme not in timing_funcions:
        raise ValueError(f"Invalid timing-scheme: {timing_scheme}")
    
    job_timings = timing_funcions[timing_scheme](jobs, options, run_context)
    
    with open(timing_file_path, "w") as f:
        json.dump(job_timings, f, indent=4)
        f.flush() 

    return 