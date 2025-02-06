import numpy as np  
from pprint import pprint
from functools import cached_property, lru_cache
from typing import List, Dict, Tuple
from collections import defaultdict
import matplotlib.pyplot as plt

import os 
import sys

# TODO: move this function in the main class. 
# TODO: let it create the link objects directly. Don't stick with legacy code.  
 
def get_link_loads(job_map, options, run_context):
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
            
            for job_id, job in job_map.items():
                throttled_job_profiles = {}
                any_flow_added = False
                
                for throttle_factor in run_context["profiled-throttle-factors"]:
                    if job.profiles is None:    
                        continue    
                    
                    job_profile = job.profiles[throttle_factor]
                    
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
                        "iter_count": job.iter_count,  
                        "profiles": throttled_job_profiles
                    })
    
    cross_rack_jobs = list(cross_rack_jobs_set) 
    return link_loads, cross_rack_jobs


def find_earliest_available_time(start, end, rem, max):
    delay = 0
    window_sum = sum(rem[t] < max for t in range(start, end))
    while window_sum > 0:
        if rem[start + delay] < max:
            window_sum -= 1  
        if rem[end + delay] < max:
            window_sum += 1  
        delay += 1
    return delay


def find_empty_ranges(signal):  
    empty_spaces = []
    current_space = None
    for i in range(len(signal)):
        if signal[i] == 0:
            if current_space is None:
                current_space = [i, i]
            else:
                current_space[1] = i
        else:
            if current_space is not None:
                empty_spaces.append(current_space)
                current_space = None
                
    if current_space is not None:
        empty_spaces.append(current_space)
        
    return empty_spaces



########################################################################################    
########################################################################################    
########################################################################################    
########################################################################################    
######################################################################################## 






# forward declaration of the solution class
class Solution: 
    pass

class Job: 
    def __init__(self, job_id, profiles, iter_count, periods, base_period): 
        self.job_id = job_id
        self.profiles = profiles
        self.iter_count = iter_count
        self.base_period = base_period
        
        # replace the str keys with float keys  
        self.periods = {float(k): v for k, v in periods.items()}
        
        self.link_loads: dict[tuple, LinkJobLoad] = {}
    
    def get_combined_signal(self, solution: Solution) -> np.array:
        signals = []
        
        for link_load in self.link_loads.values(): 
            signals.append(link_load.get_signal(solution))
        
        return np.sum(signals, axis=0)

    @lru_cache(maxsize=None)    
    def get_base_signal(self, throttle_rate = 1.0) -> np.array: 
        job_period = self.periods[throttle_rate]
        signal = np.zeros(job_period)
        
        # sum up the signals for all links 
        for link in self.link_loads.values():
            link_signal = link.get_base_signal(throttle_rate)
            signal = np.add(signal, link_signal)
            
        return signal            
        
        
    def get_active_range(self, throttle_rate, inflate = 1.0):
        start_time = 1e9 
        end_time = 0
        
        base_signal = self.get_base_signal(throttle_rate)    
                    
        first_non_zero = np.argmax(base_signal > 0)
        last_non_zero = len(base_signal) - np.argmax(base_signal[::-1] > 0)
        
        start_time = min(start_time, first_non_zero)    
        end_time = max(end_time, last_non_zero)
        
        if inflate > 1.0:
            # inflate the active range by the inflate factor
            active_range = end_time - start_time
            inflate_amount = int(active_range * (inflate - 1))
            start_time = max(0, start_time - inflate_amount)
            end_time = min(len(base_signal), end_time + inflate_amount)
            
        # print(f"Job {self.job_id} at rate {throttle_rate}, active range: {start_time} - {end_time}", file=sys.stderr)
            
        return (start_time, end_time) 

class Solution(): 
    def __init__(self, job_map): 
        self.deltas = {}
        self.throttle_rates = {} 
        self.job_map: dict[int, Job] = job_map
        
        for job_id, job in self.job_map.items():     
            iter_count = job.iter_count 
            
            self.deltas[job_id] = [0] * iter_count
            self.throttle_rates[job_id] = [1.0] * iter_count
            
    def get_job_cost(self, job_id):    
        # TODO: implement this  
        return 0


    def get_job_timings(self):  
        job_timings = []    
        
        for job_id, job in self.job_map.items():

            job_timings.append({
                "deltas": self.deltas[job_id],  
                "throttle_rates": self.throttle_rates[job_id],     
                "job_id": job_id
            })   
            
        return job_timings
    
    
    def get_job_iter_start_time(self, job_id, iter):    
        assert job_id in self.job_map, f"Job {job_id} not in {self.job_map.keys()}"    
        assert iter < len(self.deltas[job_id]), f"Job {job_id} has {len(self.deltas[job_id])} iters, not {iter}"
        
        job = self.job_map[job_id]

        start_time = 0  
        
        # for every iteration before this one, add the delta and the period
        for i in range(iter):
            delta = self.deltas[job_id][i]  
            throttle_rate = self.throttle_rates[job_id][i]  
            last_iter_period = job.profiles[throttle_rate]["period"]    
            start_time += delta + last_iter_period   

        # for this iteration, add the delta
        start_time += self.deltas[job_id][iter] 
        
        return start_time
    
    def get_job_iter_active_time(self, job_id, iter, iter_throttle_rate = 1.0, inflate = 1.0):
        assert job_id in self.job_map, f"Job {job_id} not in {self.job_map.keys()}"    
        assert iter < len(self.deltas[job_id]), f"Job {job_id} has {len(self.deltas[job_id])} iters, not {iter}"

        job = self.job_map[job_id]
        
        iter_start_time = self.get_job_iter_start_time(job_id, iter)
        active_range_start, active_range_end = job.get_active_range(throttle_rate=iter_throttle_rate,
                                                                    inflate=inflate)            
        
        return (iter_start_time + active_range_start, iter_start_time + active_range_end)        
        
    
    def get_job_waiting_times(self, job_id):    
        assert job_id in self.job_map, f"Job {job_id} not in {self.job_map.keys()}"        

        # get the ranges of time that the job is waiting 
        waiting_ranges = []
        # for each iteration, add a tuple of (start, end) to the list
        for i in range(len(self.deltas[job_id])):
            start_time = self.get_job_iter_start_time(job_id, i)
            delta = self.deltas[job_id][i]
            if delta > 0:  
                waiting_ranges.append((start_time - delta, start_time)) 
        
        return waiting_ranges
    
    # make this hashable
    def __hash__(self):
        return hash(str(self.deltas) + str(self.throttle_rates))
     
     
class LinkJobLoad():  
    # Profiles = 1 iteration of the job at different rates
    def __init__(self, job: Job, link_profiles):  
        self.job = job
        self.link_profiles = link_profiles
    
    def get_base_signal(self, throttle_rate = 1.0) -> np.array:  
        # get the signal for the job at the base rate
        return np.array(self.link_profiles[throttle_rate]["load"])
    
    def get_signal(self, solution: Solution, start_time = None, 
                   start_iter = None, end_iter = None) -> np.array:

        if start_iter is not None: 
            assert start_iter >= 0, f"start_iter {start_iter} must be >= 0"
            assert start_iter < self.job.iter_count, f"start_iter {start_iter} must be < {self.job.iter_count}"
        else:
            start_iter = 0
        
        if end_iter is not None:    
            assert end_iter >= 0, f"end_iter {end_iter} must be >= 0"
            assert end_iter <= self.job.iter_count, f"end_iter {end_iter} must be <= {self.job.iter_count}"
        else:
            end_iter = self.job.iter_count            
            
        assert start_iter < end_iter, f"start_iter {start_iter} must be < end_iter {end_iter}" 
        
        
        if start_time is not None:
            assert start_time >= 0, f"start_time {start_time} must be >= 0"
        else:
            start_time = 0
        
        job_id = self.job.job_id

        deltas = solution.deltas[job_id]
        throttle_rates = solution.throttle_rates[job_id]    

        # add zero padding for the start time   
        signal = np.zeros(start_time)
        
        # get the full signal for the job
        for iter in range(start_iter, end_iter):
            # add the delta padding for the iter            
            iter_delta = deltas[iter] 
            signal = np.append(signal, np.zeros(iter_delta))

            # add the signal for the iter   
            iter_throttle_rate = throttle_rates[iter]
            iter_signal = self.link_profiles[iter_throttle_rate]["load"]
            signal = np.append(signal, iter_signal)
            
        return signal 
            

class LinkLevelProblem(): 
    def __init__(self, link_id, max_length, score_mode):    
        self.link_id = link_id  
        self.job_loads: List[LinkJobLoad] = []  
        self.max_length: int = max_length
        self.score_mode: str = score_mode
    
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
        
    def get_compat_score(self, solution: Solution, 
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
        self.job_map = {} 

        for job in jobs:   
            job_id = job["job_id"] 
            
            if job_id not in job_profiles:
                this_job_profiles = None
            else:    
                this_job_profiles = job_profiles[job_id]
                  
            iter_count = job["iter_count"]
            base_period = job["base_period"]
            periods = job["period"]
            
            self.job_map[job_id] = Job(job_id, this_job_profiles, iter_count, periods, base_period) 
        
        # sequential execution of the jobs, worst case scenario. 
        self.max_length = sum(job.base_period * job.iter_count for job in self.job_map.values())  
        self.max_length *= 2 # let's be safe and double it, shouldn't matter much.
                
        self.links = {} 
        for i in range(self.rack_count):    
            for dir in ["up", "down"]:
                link_id = (i, dir)  
                self.links[link_id] = LinkLevelProblem(link_id, self.max_length, self.score_mode)    
        
        link_loads, cross_rack_job = get_link_loads(self.job_map, self.options, self.run_context)    
        
        for rack_num, rack_loads in enumerate(link_loads):
            for dir in ["up", "down"]:
                for link_job_load in rack_loads[dir]:
                    link = self.links[(rack_num, dir)] 
                    job: Job = self.job_map[link_job_load["job_id"]]  

                    link_job_load = LinkJobLoad(job, link_job_load["profiles"])

                    job.link_loads[link.link_id] = link_job_load
                    link.job_loads.append(link_job_load)
        
        self.cross_rack_jobs = cross_rack_job           
     
    def plot_empty_ranges(self, sol, plot_path=None):
        if plot_path is None:
            return 
           
        links = list(self.links.values())
        job_ids = list(self.job_map.keys()) 
        
        # try compressing the solution, based on the actual job_signals 
        link_empty_times = {}
        for link in links:
            link_total_load = link.get_total_load(sol)
            link_empty_times[link.link_id] = find_empty_ranges(link_total_load)    
        
        # merge the empty times of all the links 
        max_time = 0 
        for link in links:
            for empty_time in link_empty_times[link.link_id]:
                max_time = max(max_time, empty_time[1]) 
        
        empty_ranges = [len(links)] * (max_time + 1)
        
        for link in links:
            for empty_time in link_empty_times[link.link_id]:
                for t in range(empty_time[0], empty_time[1] + 1):
                    empty_ranges[t] -= 1
                    
        all_link_empty_ranges = find_empty_ranges(empty_ranges)  
        
        # get the time ranges that every links is empty. create a list of these ranges
        # for each link, find the time ranges that are empty.
            
        job_waiting_times = {} 
        job_empty_times = {} 
        
        for job_id in job_ids:  
            job = self.job_map[job_id]
            job_total_load = job.get_combined_signal(sol)    
                        
            job_waiting_times[job_id] = sol.get_job_waiting_times(job_id)   
            job_empty_times[job_id] = find_empty_ranges(job_total_load)
        
        # let's plot this mess: 
        fig, axes = plt.subplots(2, 1, figsize=(10, 5), sharex=True)   
        y = 0 
        
        for link in links:
            ranges = link_empty_times[link.link_id]
            for r in ranges:
                axes[0].plot([r[0], r[1]], [y, y], 'r-', label = f"{link.link_id}")
            y += 1
            
        ranges = all_link_empty_ranges
        for r in ranges:
            axes[0].plot([r[0], r[1]], [y, y], 'b-', label = "all") 
        y += 1  
           
        axes[0].set_title("Link empty times")
        axes[0].set_yticks(range(y))
        axes[0].set_yticklabels([f"{link.link_id}" for link in links] + ["all"])    
        
        y = 0
        for job_id in job_ids:
            ranges = job_empty_times[job_id]
            for r in ranges:
                axes[1].plot([r[0], r[1]], [y, y], 'r-', label = f"{job_id}")
            y += 1
            
        y = 0
        for job_id in job_ids:
            ranges = job_waiting_times[job_id]
            for r in ranges:
                axes[1].plot([r[0], r[1]], [y, y], 'b-', linewidth=2)   
            y += 1  
        
        axes[1].set_title("Job empty times")
        axes[1].set_yticks(range(y))
        axes[1].set_yticklabels(job_ids)
        
        plt.savefig(plot_path)                
    def get_sequentail_solution(self):  
        sol = Solution(self.job_map)  

        # create a sequential solution for the jobs
        max_iter_count = max(job.iter_count for job in self.job_map.values())
        job_ids = list(self.job_map.keys())   
        job_accum = {job_id: 0 for job_id in job_ids}   
        accum_time = 0  

        for i in range(max_iter_count):   
            for job_id, job in self.job_map.items():
                
                job_period = job.base_period        
                job_iter_count = job.iter_count 
                
                if i < job_iter_count:   
                    if i == 0: 
                        sol.deltas[job_id][i] = accum_time
                    else:
                        sol.deltas[job_id][i] = accum_time - job_accum[job_id] - job_period 
                    
                    job_accum[job_id] = accum_time                        
                    accum_time += job_period    
            
            print(sol.deltas, file=sys.stderr)     
            
        return sol
    
    
    
    
    def get_lego_solution(self):
        links = list(self.links.values())   
        
        sol = Solution(self.job_map)    
        
        job_ids = list(self.job_map.keys())    
        job_max_load = {}
        
        throttle_rates = [1.0]
        if "throttle-search" in self.run_context and self.run_context["throttle-search"]:
            throttle_rates = self.run_context["profiled-throttle-factors"]
            
        for throttle_rate in throttle_rates: 
            job_max_load[throttle_rate] = {job_id: 0 for job_id in job_ids}  
        
            for link in links:  
                for job_load in link.job_loads: 
                    job_id = job_load.job.job_id    

                    max_load = job_load.link_profiles[throttle_rate]["max"] 
                    current_val = job_max_load[throttle_rate][job_id]
                    job_max_load[throttle_rate][job_id] = max(current_val, max_load)    
                    
        print(job_max_load, file=sys.stderr)
        
        rem = [self.capacity] * self.max_length

        service_attained = {job_id: 0 for job_id in job_ids}        
        current_iters = {job_id: 0 for job_id in job_ids} 
        not_done_jobs = set(job_ids) 
        
        while len(not_done_jobs) > 0:
            # pick the job with the least service attained among the not done jobs
            job_id = min(not_done_jobs, key=lambda x: service_attained[x])
            job: Job = self.job_map[job_id]
            current_iter = current_iters[job_id]    

            best_finish_time = 1e9 
            best_throttle_rate = 1.0     
            best_delay = 0 
            best_active_start = 0 
            best_active_end = 0
            
            for throttle_rate in throttle_rates:
                inflate = 1.0 
                if "inflate" in self.run_context:   
                    inflate = self.run_context["inflate"]
                     
                active_start, active_end  = sol.get_job_iter_active_time(job_id, current_iter, throttle_rate, inflate)   
                max_load = job_max_load[throttle_rate][job_id]
                
                if max_load > self.capacity:                        
                    continue 
                
                delay = find_earliest_available_time(active_start, active_end, rem, max_load) 
                finish_time = active_end + delay     
                
                if finish_time < best_finish_time:  
                    best_finish_time = finish_time
                    best_throttle_rate = throttle_rate
                    best_delay = delay  
                    best_active_start = active_start    
                    best_active_end = active_end


            max_load = job_max_load[best_throttle_rate][job_id]  
            for t in range(best_active_start + best_delay, best_active_end + best_delay):
                rem[t] -= max_load
                
            # update the solution
            sol.deltas[job_id][current_iter] = best_delay
            sol.throttle_rates[job_id][current_iter] = best_throttle_rate   
            
            # update the current iter
            current_iters[job_id] += 1    
            service_attained[job_id] += job.base_period
            
            if current_iters[job_id] >= job.iter_count: 
                not_done_jobs.remove(job_id)    
            
            # print("---------------------------------------", file=sys.stderr)
        
        
        timing_plots_dir = f"{self.run_context['timings-dir']}/"
        os.makedirs(timing_plots_dir, exist_ok=True)
        plot_path = f"{timing_plots_dir}/link_empty_times.png"    
        self.plot_empty_ranges(sol, plot_path)  
        
        return sol
    
        
        
    def solve(self):
        base_solution = self.get_lego_solution()
        return base_solution.get_job_timings()
        