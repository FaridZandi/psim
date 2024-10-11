# cython: language_level=3

import random
import json
from pprint import pprint 
import math 
import time 
import sys 


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
    job_infos = get_job_signal_info(jobs, options, run_context)

    for job in jobs:
        job_id = job["job_id"] 
        job_timing = random.randint(0, job_infos[job_id]["period"] - 1)
    
        job_timings.append({
            "initial_wait": job_timing,
            "job_id": job_id
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
    link_solution_candidate_count = run_context["cassini-parameters"]["link-solution-candidate-count"]
    link_solution_random_quantum = run_context["cassini-parameters"]["link-solution-random-quantum"]
    link_solution_top_candidates = run_context["cassini-parameters"]["link-solution-top-candidates"]    
    
    if len(job_loads) == 0: 
        return [([], 0, 0)] * link_solution_top_candidates
    
    delta_scores = [] 
    involved_jobs = set([job["job_id"] for job in job_loads])
    
    for i in range(link_solution_candidate_count):
        deltas = [] 
        
        for job in job_loads: 
            rand_options = int(math.ceil(job["period"] / link_solution_random_quantum))
            r = random.randint(0, rand_options) * link_solution_random_quantum        
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
    results = good_deltas[:link_solution_top_candidates]

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


def cassini_timing_2(jobs, options, run_context):
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
        
        

def cassini_timing(jobs, options, run_context):
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
    
    
    