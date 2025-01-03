from pprint import pprint 
import matplotlib.pyplot as plt
import numpy as np  
import os
from copy import deepcopy
import random 
import sys 


############################################################################################################
############################################################################################################
############################################################################################################


def plot_link_capacity(ax, time_range, base_rem, smoothed_rem, link_label, direction, color, min_affected_time, max_affected_time):
    """Plot the remaining capacity for a given link direction."""
    
    ax.plot(time_range, smoothed_rem, label=f'{direction}', color=color)
    
    # add a gray shade to the area in the regions where base_rem is negative
    for t in range(len(time_range)):
        # find a range where the base_rem is negative   
        if base_rem[t] < 0:
            start = t
            while t < len(time_range) and base_rem[t] < 0:
                t += 1
            end = t
            ax.fill_between(range(start, end), -1, 101, color='gray', alpha=0.5)
    
    ax.set_title(f'{link_label} - {direction}')
    ax.set_xlabel('Time')
    ax.set_ylabel('Remaining Capacity')
    
    ax.grid(True)
    ax.set_ylim(-1, 101)
    ax.set_xlim(min_affected_time - 100, max_affected_time + 100)
    

def draw_stuff(run_context, rem, num_leaves, num_spines, routing_time, 
               min_affected_time, max_affected_time, plots_dir, smoothing_window=1):
    
    if "visualize-routing" in run_context and not run_context["visualize-routing"]:
        return  
    
    sys.stderr.write("plotting for smoothing window {} ...\n".format(smoothing_window)) 
    
    time_range = range(routing_time)
    smoothed_rem = deepcopy(rem) 
    
    if smoothing_window > 1:
        for leaf in range(num_leaves):
            for spine in range(num_spines):
                for direction in ["up", "down"]:
                    smthed = np.convolve(smoothed_rem[leaf][spine][direction], 
                                         np.ones(smoothing_window) / smoothing_window, 
                                         mode='same')

                    smoothed_rem[leaf][spine][direction] = smthed 

    # Create a figure with multiple subplots
    total_subplots = num_leaves * num_spines * 2  # Two plots (up and down) per leaf-spine pair
    fig, axes = plt.subplots(num_leaves, num_spines * 2, figsize=(num_spines * 8, num_leaves * 3), constrained_layout=True)

    # Iterate over each leaf, spine pair to plot its remaining capacity
    for leaf in range(num_leaves):
        for spine in range(num_spines):
            # Plot "up" direction
            ax_up = axes[leaf, spine * 2] if num_leaves > 1 else axes[spine * 2]
            plot_link_capacity(ax_up, time_range, 
                               rem[leaf][spine]["up"],
                               smoothed_rem[leaf][spine]["up"], 
                               f'Leaf {leaf}, Spine {spine}', 'Up', 'blue', 
                               min_affected_time, max_affected_time)

            # Plot "down" direction
            ax_down = axes[leaf, spine * 2 + 1] if num_leaves > 1 else axes[spine * 2 + 1]
            plot_link_capacity(ax_down, time_range, 
                               rem[leaf][spine]["down"],    
                               smoothed_rem[leaf][spine]["down"], 
                               f'Leaf {leaf}, Spine {spine}', 'Down', 'red', 
                               min_affected_time, max_affected_time)

    # Give a super title to the whole figure
    fig.suptitle('Remaining Bandwidth for Each Link (Up and Down)', fontsize=16)

    # Save the entire subplot grid
    plt_path = os.path.join(plots_dir, 'remaining_{}.png'.format(smoothing_window))    
    plt.savefig(plt_path)
    plt.close(fig)

    sys.stderr.write("Combined subplot figure has been saved in the directory: {}\n".format(plots_dir))




############################################################################################################
############################################################################################################
############################################################################################################


def route_flows(jobs, options, run_context, job_profiles, job_timings): 
    servers_per_rack = options["ft-server-per-rack"]
    num_leaves = options["machine-count"] // servers_per_rack   
    num_spines = options["ft-core-count"]
    link_bandwidth = options["link-bandwidth"]  
    # print ("Number of leaves: {}, Number of spines: {}, Link bandwidth: {}".format(num_leaves, num_spines, link_bandwidth))

    max_subflow_count = options["subflows"]
    # assert max_subflow_count < 3, "The current implementation only supports 1 or 2 subflows."
    
    # pprint(jobs) 
    job_deltas = {} 
    job_throttle_rates = {} 
    job_periods = {} 
    job_iterations = {} 
    
    for job_timing in job_timings:
        job_id = job_timing["job_id"]
        job_deltas[job_id] = job_timing["deltas"]
        job_throttle_rates[job_id] = job_timing["throttle_rates"]  
    
    for job in jobs:
        job_iterations[job["job_id"]] = job["iter_count"]
        job_periods[job["job_id"]] = []
        
        for i in range(job["iter_count"]):
            iter_throttle_rate = job_throttle_rates[job["job_id"]][i]
            job_periods[job["job_id"]].append(job["period"][str(iter_throttle_rate)])
            
    # routing_time = run_context["sim-length"]  
    # it might actually be more than that.     
    routing_time = 0
    for job in jobs: 
        job_id = job["job_id"]
        total_productive_time = sum(job_periods[job_id])   
        total_time_delay = sum(job_deltas[job_id]) 
        this_job_time = total_productive_time + total_time_delay 
        routing_time = max(routing_time, this_job_time)     
        
    # there this array that will be used to store the remaining capacity of the links 
    rem = []
    for i in range(num_leaves):
        rem.append([])
        for j in range(num_spines):
            rem[i].append({"up": [link_bandwidth] * routing_time, 
                           "down": [link_bandwidth] * routing_time})
            
            
    all_flows = [] 
    
    for job_id, job_profile in job_profiles.items():
        shift = 0 
            
        for iter in range(job_iterations[job_id]):
            shift += job_deltas[job_id][iter]
            iter_throttle_rate = job_throttle_rates[job_id][iter]  
    
            for flow in job_profile[iter_throttle_rate]["flows"]: 
                # shift = job_deltas[job_id] + (iter * job_periods[job_id])
                
                f = deepcopy(flow)
                
                f["eff_start_time"] = f["start_time"] + shift
                f["eff_end_time"] = f["end_time"] + shift
                f["progress_shift"] = shift 
                f["iteration"] = iter   
                
                all_flows.append(f)  
            
            shift += job_periods[job_id][iter] 
                    
    # sort flows by their start time.
    all_flows.sort(key=lambda x: x["eff_start_time"])
    
    lb_decisions = {} 
    min_affected_time = routing_time   
    max_affected_time = 0   
    
    for flow in all_flows:  
        # print the flow_id, job_id, start_time, srcrack, dstrack 
        src_leaf = flow["srcrack"]
        dst_leaf = flow["dstrack"]
        start_time = flow["eff_start_time"] 
        end_time = flow["eff_end_time"]     
        job_id = flow["job_id"]
        flow_id = flow["flow_id"]
        iteration = flow["iteration"] 
        min_subflow_mult = 1.0 / max_subflow_count    
        
        # print ("Flow: {}-{}, Start: {}, Src: {}, Dst: {}".format(job_id, flow_id, start_time, src_leaf, dst_leaf))  
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
        
        # sort the spines by their availability.
        spine_availablity.sort(key=lambda x: x[1], reverse=True)
        # print ("Spine availability: ", spine_availablity)   
        
        good_spines = [] 

        for s, spine_min_max_availble_mult in spine_availablity:
            if spine_min_max_availble_mult >= min_subflow_mult:
                good_spines.append((s, spine_min_max_availble_mult))    
        
        # print ("Good spines: ", good_spines)
        selected_spines = []
        selection_strategy = run_context["routing-fit-strategy"]
        
        if selection_strategy == "ecmp": 
            selected_spines_samples = random.sample(range(num_spines), max_subflow_count)
            for s in selected_spines_samples:
                selected_spines.append((s, min_subflow_mult))

        if len(good_spines) == 0:
            message = "No spine found for the flow: {}-{}-{}, Start: {}, Src: {}, Dst: {}".format(
                      job_id, flow_id, iteration, start_time, src_leaf, dst_leaf) 
            # print (message)
            selected_spines_samples = random.sample(range(num_spines), max_subflow_count)
            for s in selected_spines_samples:
                selected_spines.append((s, min_subflow_mult))
        else:
            if len(good_spines) == 1:
                selected_spines = [(good_spines[0][0], 1.0)]
            else: 
                if selection_strategy == "first":
                    selected_spines_samples = good_spines[:max_subflow_count]
                elif selection_strategy == "random":
                    selected_spines_samples = random.sample(good_spines, max_subflow_count)
                
                for s, spine_min_max_availble_mult in selected_spines_samples:
                    selected_spines.append((s, min_subflow_mult))
        
        # print ("Selected spines: ", selected_spines)
        
        lb_decisions[(job_id, flow_id, iteration)] = selected_spines 
        
        if start_time < min_affected_time:
            min_affected_time = start_time 
        if end_time > max_affected_time:     
            max_affected_time = end_time
            
        for t in range(start_time, end_time + 1):
            for s, mult in selected_spines:
                time_req = flow["progress_history"][t - flow["progress_shift"]] * mult
                rem[src_leaf][s]["up"][t]   -= time_req
                rem[dst_leaf][s]["down"][t] -= time_req    
    
    routing_plot_dir = "{}/routing/".format(run_context["routings-dir"])  
    os.makedirs(routing_plot_dir, exist_ok=True)    

    for smoothing_window in [1, 1000]: 
        draw_stuff(run_context, 
                rem, num_leaves, num_spines, routing_time, 
                min_affected_time, max_affected_time, 
                routing_plot_dir, smoothing_window=smoothing_window)
    
    lb_decisions_proper = []    
    
    for (job_id, flow_id, iteration), selected_spines in lb_decisions.items():
        lb_decisions_proper.append({
            "job_id": job_id,
            "flow_id": flow_id,
            "iteration": iteration,
            "spine_count": len(selected_spines),     
            "spine_rates": [(s, mult) for s, mult in selected_spines]
        })
                                   
    
    return lb_decisions_proper 

