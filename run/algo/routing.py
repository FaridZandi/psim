from pprint import pprint 
import matplotlib.pyplot as plt
import numpy as np  
import os
from copy import deepcopy
import random 
import sys 
import itertools

############################################################################################################
############################################################################################################
############################################################################################################

def get_color(job_id):  
    return plt.cm.tab20.colors[job_id % 20] 

def plot_link_usage(ax, time_range, 
                    usage, smoothed_usage,   
                    link_label,
                    leaf, spine, direction, 
                    min_affected_time, max_affected_time):
    
    # draw plt.stackplot for the jobs
    usage_stacks = []   
    job_ids = []
    
    for job_id, job_usage in usage.items():
        usage_stacks.append(job_usage[leaf][spine][direction])  
        job_ids.append(job_id) 
        
    ax.stackplot(time_range, usage_stacks, 
                 labels=[f'Job {job_id}' for job_id in job_ids], 
                 colors=[get_color(job_id) for job_id in job_ids])      
    
    # plot a horizontal line at y=100 
    ax.axhline(y=100, color='black', linestyle='--')
    
    ax.set_title(f'{link_label} - {direction}')
    ax.set_xlabel('Time')
    ax.set_ylabel('Remaining Capacity')
    
    ax.grid(True)
    ax.set_xlim(min_affected_time - 100, max_affected_time + 100)

def draw_stuff(run_context, rem, usage, all_job_ids, num_leaves, 
               num_spines, routing_time, min_affected_time, 
               max_affected_time, plots_dir, smoothing_window=1, suffix=""): 
    
    if "visualize-routing" in run_context and not run_context["visualize-routing"]:
        return  
    
    sys.stderr.write("plotting for smoothing window {} ...\n".format(smoothing_window)) 
    
    time_range = range(routing_time)
    smoothed_rem = deepcopy(rem) 
    smoothed_usage = deepcopy(usage)    
    
    if smoothing_window > 1:
        for leaf in range(num_leaves):
            for spine in range(num_spines):
                for direction in ["up", "down"]:
                    smthed = np.convolve(smoothed_rem[leaf][spine][direction], 
                                         np.ones(smoothing_window) / smoothing_window, 
                                         mode='same')

                    smoothed_rem[leaf][spine][direction] = smthed 
                    
                    for job in all_job_ids: 
                        smthed = np.convolve(smoothed_usage[job][leaf][spine][direction], 
                                             np.ones(smoothing_window) / smoothing_window, 
                                             mode='same')
                        smoothed_usage[job][leaf][spine][direction] = smthed                    
            

    # Create a figure with multiple subplots
    total_subplots = num_leaves * num_spines * 2  # Two plots (up and down) per leaf-spine pair
    fig, axes = plt.subplots(num_leaves, num_spines * 2, 
                             sharey=True, sharex=True,
                             figsize=(num_spines * 8, num_leaves * 3), 
                             constrained_layout=True, squeeze=False)

    # Iterate over each leaf, spine pair to plot its remaining capacity
    for leaf in range(num_leaves):
        for spine in range(num_spines):
            # Plot "up" direction
            ax_up = axes[leaf, spine]
            
            plot_link_usage(ax_up, time_range,  
                            usage, smoothed_usage, 
                            f'Leaf {leaf}, Spine {spine}', leaf, spine, 'up', 
                            min_affected_time, max_affected_time)   

            # Plot "down" direction
            ax_down = axes[leaf, spine + num_spines] 
            
            plot_link_usage(ax_down, time_range,    
                            usage, smoothed_usage, 
                            f'Leaf {leaf}, Spine {spine}', leaf, spine, 'down', 
                            min_affected_time, max_affected_time)   
            
    # Give a super title to the whole figure
    fig.suptitle('Remaining Bandwidth for Each Link (Up and Down)', fontsize=16)

    # Save the entire subplot grid
    plt_path = os.path.join(plots_dir, 'remaining_{}_{}.png'.format(smoothing_window, suffix))    
    plt.savefig(plt_path)
    plt.close(fig)

    sys.stderr.write("Combined subplot figure has been saved in the directory: {}\n".format(plots_dir))


############################################################################################################
############################################################################################################
############################################################################################################


def route_flows_best(spine_availablity, max_subflow_count):   
    selected_spines = []
    
    # sort the spines by their availability.
    spine_availablity.sort(key=lambda x: x[1], reverse=True)

    # assuming we want to make at most max_subflow_count subflows, 
    # how much of the flow can we actually serve?
    max_subf_availablity = sum([mult for s, mult in spine_availablity[:max_subflow_count]]) 

    remaining = 1.0 
    epsilon = 1e-3 
    
    # if we can serve the entire flow, then we can just use the spines as they are.
    if max_subf_availablity >= 1.0 - epsilon:
        min_available_mult = 1.0 
        for s, mult in spine_availablity:
            min_available_mult = min(min_available_mult, mult)   
            selected_spines.append((s, min(remaining, mult)))   
            remaining -= mult
            if remaining < epsilon:
                break
        
        ####################################################################
        # trying this but not sure if it's a good idea. 
        ####################################################################
        assigned_spines_count = len(selected_spines)    
        fair_assigned_mult = 1.0 / assigned_spines_count    
        
        sys.stderr.write("Assigned spines count: {}, Fair assigned mult: {}, Min available mult: {}\n".format(
            assigned_spines_count, fair_assigned_mult, min_available_mult))
        
        if fair_assigned_mult <= min_available_mult:
            spines = [s for s, _ in selected_spines]    
            selected_spines = [] 
            for s in spines:
                selected_spines.append((s, fair_assigned_mult))
            
        ####################################################################
        
    # otherwise, we need to distribute the flow among the spines.
    # TODO: there might be better ways to do this. 
    # like if there's a spine that can serve more than the equal_mult,
    # we can give it more. 
    # For now, we just distribute the flow equally among the spines.
    else: 
        equal_mult = 1.0 / max_subflow_count    
        for s, _ in spine_availablity[:max_subflow_count]:  
            selected_spines.append((s, equal_mult))

    return selected_spines


def route_flows_first(spine_availablity, max_subflow_count, num_spines):    
    selected_spines = [] 
     
    total_avail = sum([mult for s, mult in spine_availablity]) 
    availability_map = {s: mult for s, mult in spine_availablity} 

    if total_avail < 1.0:
        equal_mult = 1.0 / max_subflow_count    
        for s, _ in spine_availablity[:max_subflow_count]:  
            selected_spines.append((s, equal_mult))
        
    else: 
        # start
        found = False                 

        # for current_subflow_count in range(1, max_subflow_count + 1):
        for current_subflow_count in [max_subflow_count]:
            
            spine_range = range(num_spines) 
            for spine_comb in itertools.combinations(spine_range, current_subflow_count):

                current_comb_avail = sum([availability_map[s] for s in spine_comb]) 

                if current_comb_avail >= 1.0:
                    # tap out until we nothing remains. 
                    found = True    
                    remaining = 1.0 
                    epsilon = 1e-3 

                    for s in spine_comb:    
                        if availability_map[s] < epsilon:   
                            continue
                        
                        selected_spines.append((s, min(remaining, availability_map[s])))
                        
                        remaining -= availability_map[s]
                        if remaining < epsilon:
                            break
                        
                    break
            if found:
                break
            
        if not found:   
            equal_mult = 1.0 / max_subflow_count    
            random_sample = random.sample(range(num_spines), max_subflow_count) 
            for s in random_sample:
                selected_spines.append((s, equal_mult))
    
    return selected_spines  
    
    
def route_flows_random(max_subflow_count, num_spines):   
    selected_spines = []    
    
    min_subflow_mult = 1.0 / max_subflow_count    

    random_sample = random.sample(range(num_spines), max_subflow_count) 
    for s in random_sample:
        selected_spines.append((s, min_subflow_mult))
        
    return selected_spines  


def route_flows_useall(num_spines):  
    min_subflow_mult = 1.0 / num_spines 
    selected_spines = [(s, min_subflow_mult) for s in range(num_spines)]
    return selected_spines  

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

def update_time_range(start_time, end_time, flow, selected_spines, rem, usage, src_leaf, dst_leaf): 

    for t in range(start_time, end_time + 1):
        for s, mult in selected_spines:
            time_req = flow["progress_history"][t - flow["progress_shift"]] * mult
            rem[src_leaf][s]["up"][t]   -= time_req
            rem[dst_leaf][s]["down"][t] -= time_req    
            
            job_id = flow["job_id"]
            usage[job_id][src_leaf][s]["up"][t] += time_req 
            usage[job_id][dst_leaf][s]["down"][t] += time_req
            
            
def route_flows(jobs, options, run_context, job_profiles, job_timings): 
    servers_per_rack = options["ft-server-per-rack"]
    num_leaves = options["machine-count"] // servers_per_rack   
    num_spines = options["ft-core-count"]
    link_bandwidth = options["link-bandwidth"]  
    max_subflow_count = options["subflows"]
    
    job_deltas = {} 
    job_throttle_rates = {} 
    job_periods = {} 
    job_iterations = {} 
    
    all_job_ids = [job["job_id"] for job in jobs]   
    
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
            
    
    routing_plot_dir = "{}/routing/".format(run_context["routings-dir"])  
    os.makedirs(routing_plot_dir, exist_ok=True)   
    
    # routing_time = run_context["sim-length"]  
    # it might actually be more than that.     
    routing_time = 0
    for job in jobs: 
        job_id = job["job_id"]
        total_productive_time = sum(job_periods[job_id])    
        total_time_delay = sum(job_deltas[job_id]) 
        this_job_time = total_productive_time + total_time_delay 
        routing_time = max(routing_time, this_job_time)     
        
    rem = initialize_rem(num_leaves, num_spines, link_bandwidth, routing_time)
    usage = initialize_usage(all_job_ids, num_leaves, num_spines, routing_time)
    
    all_flows = get_all_flows(job_profiles, job_deltas, job_throttle_rates, 
                              job_periods, job_iterations)
                    
    all_flows.sort(key=lambda x: x["eff_start_time"])
    
    lb_decisions = {} 
    min_affected_time = routing_time   
    max_affected_time = 0   
    
    counter = 0 
    for flow in all_flows:
        
        counter += 1
          
        src_leaf = flow["srcrack"]
        dst_leaf = flow["dstrack"]
        start_time = flow["eff_start_time"] 
        end_time = flow["eff_end_time"]     
        job_id = flow["job_id"]
        flow_id = flow["flow_id"]
        iteration = flow["iteration"] 
        
        spine_availablity = get_spine_availablity(flow, rem, num_spines, 
                                                  start_time, end_time, 
                                                  src_leaf, dst_leaf)    

        # sys.stderr.write("flow: src_leaf: {}, dst_leaf: {}, start_time: {}, end_time: {}\n".format(
        #     src_leaf, dst_leaf, start_time, end_time))
        # sys.stderr.write("spine_availability: {}\n".format(spine_availablity))  
        
        ###########################################################################
        ### routing v2: ###########################################################
        ###########################################################################
        selected_spines = [] 

        selection_strategy = run_context["routing-fit-strategy"]
        
        if selection_strategy == "best": 
            selected_spines = route_flows_best(spine_availablity, max_subflow_count)

        elif selection_strategy == "first": 
            if counter == 1:
                selected_spines = [(0, 1.0)]
            elif counter == 2: 
                selected_spines = [(2, 1.0)]
            elif counter == 3:
                selected_spines = [(1, 1.0)]
            elif counter == 4:
                selected_spines = [(2, 1.0)]
            elif counter == 5:
                selected_spines = [(0, 1.0)]
            elif counter == 6:
                selected_spines = [(1, 1.0)]
            elif counter == 7:
                selected_spines = [(2, 1.0)]
            elif counter == 8:
                selected_spines = [(0, 1.0)]
            else:
                selected_spines = route_flows_first(spine_availablity, max_subflow_count, 
                                                num_spines)
            
        elif selection_strategy == "random":
            selected_spines = route_flows_random(max_subflow_count, num_spines)              
            
        elif selection_strategy == "useall":
            selected_spines = route_flows_useall(num_spines) 
        ###########################################################################

        lb_decisions[(job_id, flow_id, iteration)] = selected_spines 
        
        min_affected_time = min(min_affected_time, start_time)  
        max_affected_time = max(max_affected_time, end_time)
        
        update_time_range(start_time, end_time, flow, 
                          selected_spines, rem, usage, 
                          src_leaf, dst_leaf)       

        if counter < 10: 
            draw_stuff(run_context, 
                   rem, usage, all_job_ids, num_leaves, num_spines, routing_time, 
                   min_affected_time, max_affected_time, 
                   routing_plot_dir, smoothing_window=1, suffix="{}".format(counter))
            
    for smoothing_window in [1, 1000]: 
        draw_stuff(run_context, 
                   rem, usage, all_job_ids, num_leaves, num_spines, routing_time, 
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

