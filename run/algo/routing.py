from pprint import pprint 
import matplotlib.pyplot as plt
import numpy as np  
import os
from copy import deepcopy
import random 

def plot_link_capacity(ax, time_range, remaining_capacity, link_label, direction, color, min_affected_time, max_affected_time):
    """Plot the remaining capacity for a given link direction."""
    ax.plot(time_range, remaining_capacity, label=f'{direction}', color=color)
    ax.set_title(f'{link_label} - {direction}')
    ax.set_xlabel('Time')
    ax.set_ylabel('Remaining Capacity')
    ax.grid(True)
    ax.set_ylim(-1, 101)
    ax.set_xlim(min_affected_time - 100, max_affected_time + 100)

def draw_stuff(rem, num_leaves, num_spines, routing_time, min_affected_time, max_affected_time):
    time_range = range(routing_time)

    # Ensure the target directory exists
    dir_path = "plots/routing/"
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    # Create a figure with multiple subplots
    total_subplots = num_leaves * num_spines * 2  # Two plots (up and down) per leaf-spine pair
    fig, axes = plt.subplots(num_leaves, num_spines * 2, figsize=(num_spines * 8, num_leaves * 3), constrained_layout=True)

    # Iterate over each leaf, spine pair to plot its remaining capacity
    for leaf in range(num_leaves):
        for spine in range(num_spines):
            # Plot "up" direction
            ax_up = axes[leaf, spine * 2] if num_leaves > 1 else axes[spine * 2]
            plot_link_capacity(ax_up, time_range, rem[leaf][spine]["up"], f'Leaf {leaf}, Spine {spine}', 'Up', 'blue', min_affected_time, max_affected_time)

            # Plot "down" direction
            ax_down = axes[leaf, spine * 2 + 1] if num_leaves > 1 else axes[spine * 2 + 1]
            plot_link_capacity(ax_down, time_range, rem[leaf][spine]["down"], f'Leaf {leaf}, Spine {spine}', 'Down', 'red', min_affected_time, max_affected_time)

    # Give a super title to the whole figure
    fig.suptitle('Remaining Bandwidth for Each Link (Up and Down)', fontsize=16)

    # Save the entire subplot grid
    plt_path = os.path.join(dir_path, 'combined_subplots_remaining.png')
    plt.savefig(plt_path)
    plt.close(fig)

    print("Combined subplot figure has been saved in the directory:", dir_path)

def route_flows(jobs, options, run_context, config_sweeper, job_profiles, job_timings): 
    
    servers_per_rack = options["ft-server-per-rack"]
    num_leaves = options["machine-count"] // servers_per_rack   
    num_spines = options["ft-core-count"]
    link_bandwidth = options["link-bandwidth"]  

    pprint(jobs) 
    
    print ("Number of leaves: {}, Number of spines: {}, Link bandwidth: {}".format(num_leaves, num_spines, link_bandwidth))
    
    # there this array that will be used to store the remaining capacity of the links 
    rem = []
    routing_time = 100000
    
    min_affected_time = routing_time   
    max_affected_time = 0   
    
    lb_decisions = {} 
    
    for i in range(num_leaves):
        rem.append([])
            
        for j in range(num_spines):
            rem[i].append({"up": [link_bandwidth] * routing_time, 
                           "down": [link_bandwidth] * routing_time})

    job_deltas = {} 
    job_periods = {} 
    job_iterations = {} 
    
    for job_timing in job_timings:
        job_deltas[job_timing["job_id"]] = job_timing["initial_wait"]
    
    for job in jobs:
        job_periods[job["job_id"]] = job["period"]
        job_iterations[job["job_id"]] = job["iter_count"]
        
    all_flows = [] 
    for job_id, job_profile in job_profiles.items():
        for flow in job_profile["flows"]: 
            for iter in range(job_iterations[job_id]):
                   
                shift = job_deltas[job_id] + (iter * job_periods[job_id])
                f = deepcopy(flow)
                
                f["eff_start_time"] = f["start_time"] + shift
                f["eff_end_time"] = f["end_time"] + shift
                f["progress_shift"] = shift 
                f["iteration"] = iter   
                
                all_flows.append(f)  
        
    # sort flows by their start time.
    all_flows.sort(key=lambda x: x["eff_start_time"])
    
    pprint(job_deltas)
    for flow in all_flows:  
        # print the flow_id, job_id, start_time, srcrack, dstrack 
        src_leaf = flow["srcrack"]
        dst_leaf = flow["dstrack"]
        start_time = flow["eff_start_time"] 
        end_time = flow["eff_end_time"]     
        job_id = flow["job_id"]
        flow_id = flow["flow_id"]
        iteration = flow["iteration"] 
        
        # print ("Flow: {}-{}, Start: {}, Src: {}, Dst: {}".format(job_id, flow_id, start_time, src_leaf, dst_leaf))  
        
        good_spines = [] 

        # find one path for the flow. 
        for s in range(num_spines): 
            # test this spine. 
            # it has to have capacity 
            spine_okay = True 
            
            for t in range(start_time, end_time + 1): 
                # test this time.
                up_req = flow["progress_history"][t - flow["progress_shift"]]
                up_rem = rem[src_leaf][s]["up"][t] 
                down_req = flow["progress_history"][t - flow["progress_shift"]] 
                down_rem = rem[dst_leaf][s]["down"][t]
                
                if up_rem < up_req or down_rem < down_req:
                    spine_okay = False 
                    break
                
            if spine_okay:
                good_spines.append(s)
                
        if len(good_spines) == 0:
            print ("No spine found for the flow: {}-{}, Start: {}, Src: {}, Dst: {}".format(job_id, flow_id, start_time, src_leaf, dst_leaf))
            selected_s = random.choice(range(num_spines))
        else:
            selected_s = random.choice(good_spines) 
        
        lb_decisions[(job_id, flow_id, iteration)] = selected_s 
        
        if start_time < min_affected_time:
            min_affected_time = start_time 
        if end_time > max_affected_time:     
            max_affected_time = end_time
            
        for t in range(start_time, end_time + 1): 
            rem[src_leaf][selected_s]["up"][t]   -= flow["progress_history"][t - flow["progress_shift"]]
            rem[dst_leaf][selected_s]["down"][t] -= flow["progress_history"][t - flow["progress_shift"]]    
    
    # draw_stuff(rem, num_leaves, num_spines, routing_time, min_affected_time, max_affected_time)
    # draw the assignments of the flows. 
    # input("Press Enter to continue...") 
    
    # change the lb_decisions to be a list of tuples. 
    # lb_decisions = [(job_id, flow_id, iteration, s) for (job_id, flow_id, iteration), s in lb_decisions.items()]
    
    lb_decisions_proper = []    
    for (job_id, flow_id, iteration), s in lb_decisions.items():
        lb_decisions_proper.append({
            "job_id": job_id,
            "flow_id": flow_id,
            "iteration": iteration,
            "spine": s
        })
                                   
    
    return lb_decisions_proper 

