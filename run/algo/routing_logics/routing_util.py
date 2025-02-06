
import matplotlib.pyplot as plt
from collections import defaultdict
from algo.routing_logics.routing_plot_util import plot_time_ranges  

from copy import deepcopy   

import sys 

def update_time_range(start_time, end_time, flow, selected_spines, rem, usage, src_leaf, dst_leaf): 
    for t in range(start_time, end_time + 1):
        for s, mult in selected_spines:
            time_req = flow["progress_history"][t - flow["progress_shift"]] * mult
            rem[src_leaf][s]["up"][t]   -= time_req
            rem[dst_leaf][s]["down"][t] -= time_req    
            
            job_id = flow["job_id"]
            usage[job_id][src_leaf][s]["up"][t] += time_req 
            usage[job_id][dst_leaf][s]["down"][t] += time_req
            
            

def get_spine_availablity(flow, rem, num_spines, start_time, end_time, src_leaf, dst_leaf):
    spine_availablity = []  
    
    for s in range(num_spines): 
        spine_min_max_availble_mult = 1.0   
        
        for t in range(start_time, end_time + 1): 
            up_req = flow["progress_history"][t - flow["progress_shift"]]
            up_rem = rem[src_leaf][s]["up"][t] 
            down_req = flow["progress_history"][t - flow["progress_shift"]]
            down_rem = rem[dst_leaf][s]["down"][t]
            
            up_max_available_mult = min(1, up_rem / up_req)
            down_max_available_mult = min(1, down_rem / down_req)   
            this_time_max_available_mult = min(up_max_available_mult, down_max_available_mult)  

            spine_min_max_availble_mult = min(spine_min_max_availble_mult, this_time_max_available_mult)    
            
        spine_availablity.append((s, spine_min_max_availble_mult))

    return spine_availablity    




def merge_overlapping_ranges(ranges_dict, plot_path, hash_to_traffic_id):
    # Flatten all intervals with their corresponding key
    intervals = []
    for key, ranges in ranges_dict.items():
        for start, end in ranges:
            intervals.append((start, end, {key}))
    
    # Sort by start time
    intervals.sort()
    
    # Merge overlapping intervals while tracking keys
    merged = []
    for start, end, keys in intervals:
        if merged: 
            print(f"current: {start}, {end}, {keys}, last: {merged[-1][0]}, {merged[-1][1]}, {merged[-1][2]}" , file=sys.stderr)    
        else: 
            print(f"current: {start}, {end}, {keys}, last: None", file=sys.stderr)  
            
        if merged and merged[-1][1] >= start:  # Overlap exists
            merged[-1] = (merged[-1][0], max(merged[-1][1], end), merged[-1][2] | keys)
        else:
            merged.append((start, end, keys))
    
    new_ranges = defaultdict(list) 
    
    for idx, (start, end, keys) in enumerate(merged):
        comb_key = tuple(sorted(keys))    
        new_ranges[comb_key].append((start, end))   
    
    
    for comb_key in new_ranges:
        new_ranges[comb_key].sort() 
        
        summarized_ranges = [] 
        
        last_range = None
        for i, (start, end) in enumerate(new_ranges[comb_key]):
            if last_range is None:
                last_range = (start, end) 
            elif start > last_range[1] + 1: 
                summarized_ranges.append(last_range)
                last_range = (start, end) 
            else:
                last_range = (last_range[0], end)
            
            if i == len(new_ranges[comb_key]) - 1:
                summarized_ranges.append(last_range)
            
        new_ranges[comb_key] = summarized_ranges                        
    
    if plot_path is not None:   
        plot_time_ranges(ranges_dict, dict(new_ranges), hash_to_traffic_id, plot_path)
    
    return new_ranges 


      
    
def find_value_in_range(d, value):
    for (start, end), v in d.items():
        if start <= value <= end:
            return v
    return None  # Return None if no range contains the value




def get_all_flows(job_profiles, job_deltas, 
                  job_throttle_rates, job_periods, job_iterations):
    all_flows = [] 
    
    for job_id, job_profile in job_profiles.items():
        shift = 0 
            
        for iter in range(job_iterations[job_id]):
            shift += job_deltas[job_id][iter]
            iter_throttle_rate = job_throttle_rates[job_id][iter]  
    
            for flow in job_profile[iter_throttle_rate]["flows"]: 
                f = deepcopy(flow)
                
                f["eff_start_time"] = f["start_time"] + shift
                f["eff_end_time"] = f["end_time"] + shift
                f["progress_shift"] = shift 
                f["iteration"] = iter  
                
                f["throttle_rate"] = iter_throttle_rate 
                f["max_load"] = max(f["progress_history"])
                
                all_flows.append(f)  
            
            shift += job_periods[job_id][iter] 

    return all_flows


def initialize_rem(num_leaves, num_spines, link_bandwidth, routing_time):
    rem = []
    for i in range(num_leaves):
        rem.append([])
        for j in range(num_spines):
            rem[i].append({"up": [link_bandwidth] * routing_time, 
                            "down": [link_bandwidth] * routing_time})
    return rem

def initialize_usage(all_job_ids, num_leaves, num_spines, routing_time):
    usage = {}
    for job in all_job_ids:
        usage[job] = []
        for i in range(num_leaves):
            usage[job].append([])
            for j in range(num_spines):
                usage[job][i].append({"up": [0] * routing_time, 
                                        "down": [0] * routing_time})
    return usage
            
            