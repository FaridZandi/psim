import numpy as np  
from pprint import pprint
from functools import cached_property
# type
from typing import List, Dict, Tuple
from collections import defaultdict

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










########################################################################################    
########################################################################################    
########################################################################################    
########################################################################################    
######################################################################################## 

















class Solution(): 
    def __init__(self, jobs, job_profiles): 
        self.deltas = {}
        self.throttle_rates = {} 
        self.costs = {}
        self.job_profiles = job_profiles    
        self.jobs = jobs 
        
        for job in jobs:    
            iter_count = job["iter_count"] 
            
            self.deltas[job["job_id"]] = [0] * iter_count
            self.throttle_rates[job["job_id"]] = [1.0] * iter_count
            self.costs[job["job_id"]] = [0] * iter_count    
            
    def get_job_cost(self, job_id):    
        return sum(self.costs[job_id])  

    def get_job_timings(self):  
        job_timings = []    
        
        for job in self.jobs:
            job_id = job["job_id"]

            job_timings.append({
                "deltas": self.deltas[job_id],  
                "throttle_rates": self.throttle_rates[job_id],     
                "job_id": job_id
            })   
            
        return job_timings
    
    def get_job_iter_start_time(self, job_id, iter):    
        start_time = 0  
        
        for i in range(iter):
            delta = self.deltas[job_id][i]  
            throttle_rate = self.throttle_rates[job_id][i]  
            last_iter_period = self.job_profiles[job_id][throttle_rate]["period"]    
            start_time += delta + last_iter_period   

        start_time += self.deltas[job_id][iter] 
        
        return start_time
    
    def get_job_waiting_times(self, job_id):    
        # get the ranges of time that the job is waiting 
        waiting_ranges = []
        # for each iteration, add a tuple of (start, end) to the list
        for i in range(len(self.deltas[job_id])):
            start_time = self.get_job_iter_start_time(job_id, i)
            delta = self.deltas[job_id][i]  
            
            waiting_ranges.append((start_time - delta, start_time)) 
        
        return waiting_ranges
    
    # make this hashable
    def __hash__(self):
        return hash(str(self.deltas) + str(self.throttle_rates) + str(self.costs))
     
class LinkJobLoad():  
    # Profiles = 1 iteration of the job at different rates
    def __init__(self, job, profiles, iter_count):  
        self.job = job
        self.profiles = profiles
        self.iter_count = iter_count    
    
    def get_signal(self, solution: Solution, 
                   start_time = None, start_iter = None, end_iter = None) -> np.array:
        
        job_id = self.job["job_id"] 

        deltas = solution.deltas[job_id]
        throttle_rates = solution.throttle_rates[job_id]    

        if start_time is None:  
            start_time = 0
        # add zero padding for the start time   
        signal = np.zeros(start_time)
        
        if start_iter is None:  
            start_iter = 0
        if end_iter is None:    
            end_iter = self.iter_count
            
        # get the full signal for the job
        for iter in range(start_iter, end_iter):
            
            # add the delta padding for the iter            
            iter_delta = deltas[iter] 
            signal = np.append(signal, np.zeros(iter_delta))

            # add the signal for the iter   
            iter_throttle_rate = throttle_rates[iter]
            iter_signal = self.profiles[iter_throttle_rate]["load"]
            signal = np.append(signal, iter_signal)
            
        return signal 
            

class LinkLevelProblem(): 
    def __init__(self, link_id, max_length, score_mode):    
        self.link_id = link_id  
        # self.job_loads = [] # a list of LinkLevelJob objects
        # with the proper typing 
        self.job_loads: List[LinkJobLoad] = []  
        self.max_length = max_length
        self.score_mode = score_mode
    
    def get_total_load(self, solution: Solution) -> np.array:
        all_signals = []
        
        for job_load in self.job_loads:
            job_signal = job_load.get_signal(solution)
            all_signals.append(job_signal)
                
        # pad all signals to the same length
        max_signal_length = max(len(signal) for signal in all_signals)
        padded_signals = [
            np.pad(signal, (0, max_signal_length - len(signal)), mode='constant')
            for signal in all_signals
        ]
        
        sum_signal = np.sum(padded_signals, axis=0)
        
        return sum_signal
        
    def get_compat_score(self, 
                         solution: Solution, 
                         capacity: float) -> float: 
        
        sum_signal = self.get_total_load(solution)        
        max_util = np.max(sum_signal)
    
        compat_score = 0
        if self.score_mode == "under-cap":
            compat_score = np.mean(sum_signal <= capacity)

        elif self.score_mode == "time-no-coll":
            
            if max_util <= capacity:  
                compat_score = 1.0   
            else: 
                first_overload_index = np.argmax(sum_signal > capacity)
                compat_score = first_overload_index / self.max_length
                
            job_costs = [solution.get_job_cost(job_load.job["job_id"]) for job_load in self.job_loads]  
            solution_cost = sum(job_costs) / len(self.job_loads)

            compat_score = compat_score - solution_cost  
            
        elif self.score_mode == "max-util-left":
            compat_score = (capacity - max_util) / capacity

        return compat_score            


class TimingSolver(): 
    def __init__(self, jobs, run_context, options, job_profiles, scheme):
        self.run_context = run_context
        self.options = options
        self.scheme = scheme     
            
        # getting the parameters from the run context and options         
        self.score_mode = run_context["compat-score-mode"]
        self.rack_count = options["machine-count"] // options["ft-server-per-rack"] 
        self.link_bandwidth = options["link-bandwidth"]
        self.capacity = options["ft-core-count"] * options["ft-agg-core-link-capacity-mult"]
        
        self.jobs = jobs # list of jobs, each job is a dict
        self.job_map = {job["job_id"]: job for job in jobs}
        
        # example job = {
        #     "job_id": 1,
        #     "machine_count": 12,
        #     "comm_size": 32000,
        #     "comp_size": 200,
        #     "layer_count": 1,
        #     "iter_count": 5,
        #     "machines": [23,5,18,47,2,39,38,28,40,34,33,31],
        #     "period": { "1.0": 994, "0.5": 1100 },
        #     "base_period": 994
        # },
        
        
        self.job_profiles = job_profiles
        # two level dict like job_profiles[job_id][throttle_factor] = profile 
        # and each profile is a bunch of flows, and the period of the job
        # {
        #     'flows': [
        #         {
        #             'core': 0, 'dir': 'incoming',
        #             'dstrack': 0, 'end_time': 426, 'fct': 27, 'flow_id': 0,
        #             'flow_size': 2700.0, 'iteration': 0,
        #             'job_id': 1, 'label': 'chain_1_hop_1_subflow_1',
        #             'progress_history': [0.0, 0.0, 0.0, 0.0, 0.0, 0.0], #retracted
        #             'progress_history_summarized': [[0, 400], [100.0, 27], [0, 567]],
        #             'srcrack': 2, 'start_time': 400, 'subflow': 0
        #         }, ...
        #     ],
        #     'period': 994
        # }
        
        
        # sequential execution of the jobs, worst case scenario. 
        self.max_length = sum(job["base_period"] * job["iter_count"] for job in self.jobs)
        self.max_length *= 2 # let's be safe and double it, shouldn't matter much.
                
        self.links = {} 
        for i in range(self.rack_count):    
            for dir in ["up", "down"]:
                link_id = (i, dir)  
                self.links[link_id] = LinkLevelProblem(link_id, self.max_length, self.score_mode)    
        
    def get_sequentail_solution(self):  
        sol = Solution(self.jobs, self.job_profiles)  

        # create a sequential solution for the jobs
        max_iter_count = max(job["iter_count"] for job in self.jobs)                    
        job_ids = [job["job_id"] for job in self.jobs]  
        job_accum = {job_id: 0 for job_id in job_ids}   
        accum_time = 0  

        for i in range(max_iter_count):   
            for job_id in job_ids:
                
                job_period = self.job_map[job_id]["base_period"]    
                job_iter_count = self.job_map[job_id]["iter_count"]
                
                if i < job_iter_count:   
                    if i == 0: 
                        sol.deltas[job_id][i] = accum_time
                    else:
                        sol.deltas[job_id][i] = accum_time - job_accum[job_id] - job_period 
                    
                    job_accum[job_id] = accum_time                        
                    accum_time += job_period    
            
            pprint(sol.deltas)   
            
        return sol
    
    def get_lego_solution(self, links: List[LinkLevelProblem]):   
        sol = Solution(self.jobs, self.job_profiles)    
        
        
        job_max_load = {job["job_id"]: 0 for job in self.jobs}
        job_ids = [job["job_id"] for job in self.jobs] 
        
        for link in links:  
            for job_load in link.job_loads: 
                job_id = job_load.job["job_id"]
                job_max_load[job_id] = max(job_max_load[job_id], job_load.profiles[1.0]["max"])
        pprint(job_max_load)    
        
        rem = [self.capacity] * self.max_length
        
        service_attained = {job_id: 0 for job_id in job_ids}        
        current_iters = {job_id: 0 for job_id in job_ids} 
        not_done_jobs = set(job_ids) 
        
        while len(not_done_jobs) > 0:
            # pick a random job
            # job_id = np.random.choice(job_ids) 
            
            # pick the job with the least service attained among the not done jobs
            job_id = min(not_done_jobs, key=lambda x: service_attained[x])
            
            print(f"Job {job_id} at iter {current_iters[job_id]}, max load {job_max_load[job_id]}") 
            
            # assume the rate is 1.0 for now
            throttle_rate = 1 

            # find the first time that the job can be scheduled
            current_iter = current_iters[job_id]    
            
            earliest_iter_start = sol.get_job_iter_start_time(job_id, current_iter)
            earliest_iter_end = earliest_iter_start + self.job_profiles[job_id][throttle_rate]["period"]
            
            print(f"Earliest start {earliest_iter_start}, end {earliest_iter_end}")
            
            def find_earliest_available_time(earliest_iter_start, earliest_iter_end, rem, job_max_load):
                delay = 0
                current_window = sum(rem[t] < job_max_load for t in range(earliest_iter_start, earliest_iter_end))
                while current_window > 0:
                    if rem[earliest_iter_start + delay] < job_max_load:
                        current_window -= 1  
                    if rem[earliest_iter_end + delay] < job_max_load:
                        current_window += 1  
                    delay += 1
                return delay
             
            delay = find_earliest_available_time(earliest_iter_start, earliest_iter_end, rem, job_max_load[job_id]) 
            
            print(f"Job {job_id} with delay {delay} can be scheduled from {earliest_iter_start + delay} to {earliest_iter_end + delay}")
            # schedule the job at this time
            for t in range(earliest_iter_start + delay, earliest_iter_end + delay):
                rem[t] -= job_max_load[job_id]
                
            # update the solution
            sol.deltas[job_id][current_iter] = delay
            
            # update the current iter
            current_iters[job_id] += 1    
            service_attained[job_id] += self.job_profiles[job_id][throttle_rate]["period"]
            
            if current_iters[job_id] >= self.job_map[job_id]["iter_count"]: 
                not_done_jobs.remove(job_id)    
            
            print("---------------------------------------")
            
        
        # try compressing the solution, based on the actual job_signals 
        
        link_empty_spaces = defaultdict(list)
        
        for link in links:
            total_load = link.get_total_load(sol)
            
            # find the empty spaces in the signal   
            current_empty_space = None
            
            for i in range(len(total_load)):
                if total_load[i] == 0:
                    if current_empty_space is None:
                        current_empty_space = [i, i]
                    else:
                        current_empty_space[1] = i
                else:
                    if current_empty_space is not None:
                        link_empty_spaces[link.link_id].append(current_empty_space)
                        current_empty_space = None
            
            if current_empty_space is not None:
                link_empty_spaces[link.link_id].append(current_empty_space)
                            
        
        pprint(link_empty_spaces)
        
        # get the time ranges that every links is empty. create a list of these ranges
        # for each link, find the time ranges that are empty.
            
                
        job_waiting_times = {} 
        
        for job_id in job_ids:  
            job_waiting_times[job_id] = sol.get_job_waiting_times(job_id)   
            
        pprint(job_waiting_times)   
        
        return sol
            
            
    
    def solve(self):
        # step get the link level loads 
        link_loads, cross_rack_job = get_link_loads(self.jobs, self.options, self.run_context, self.job_profiles)    
        
        for rack_num, rack_loads in enumerate(link_loads):
            for dir in ["up", "down"]:
                for link_job_load in rack_loads[dir]:

                    link = self.links[(rack_num, dir)] 
                    job = self.job_map[link_job_load["job_id"]]  

                    link_job_load = LinkJobLoad(job, link_job_load["profiles"], link_job_load["iter_count"])
                    link.job_loads.append(link_job_load)
                    
        
        # base_solution = self.get_sequentail_solution()   
                
        base_solution = self.get_lego_solution(self.links.values())
        
        return base_solution.get_job_timings()
        
        
if __name__ == "__main__":
    jobs = []
    options = {}
    run_context = {}
    job_profiles = {}
    
    solver = TimingSolver(jobs, run_context, options, job_profiles)
    solver.solve()