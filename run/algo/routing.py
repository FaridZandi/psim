from pprint import pprint 
import matplotlib.pyplot as plt
import numpy as np  
import os
from copy import deepcopy
import random 
import sys 
import itertools
import networkx as nx
import matplotlib.pyplot as plt
from networkx.algorithms.flow import maximum_flow
from collections import deque, defaultdict
import hashlib
from itertools import chain
from datetime import datetime   


############################################################################################################
############################################################################################################
############################################################################################################

from algo.routing_logics.routing_plot_util import plot_routing  
from algo.routing_logics.routing_util import * 
from algo.routing_logics.coloring_v2 import route_flows_graph_coloring_v2
from algo.routing_logics.coloring_v3 import route_flows_graph_coloring_v3   
from algo.routing_logics.coloring_v4 import route_flows_graph_coloring_v4
from algo.routing_logics.coloring_v5 import route_flows_graph_coloring_v5
from algo.routing_logics.simple_routing import route_flows_one_by_one
    
############################################################################################################
############################################################################################################
############################################################################################################

def log_results(run_context, key, value):
    # print to stderr first  
    sys.stderr.write(f"KEY: {key}\n")
    sys.stderr.write(f"VALUE: {value}\n")   

    with open(run_context["output-file"], "a+") as f:
        f.write(key + ":\n")
        pprint(value, f) 
        f.write("\n---------------------------------\n")   



def route_flows(jobs, options, run_context, job_profiles, job_timings, 
                suffix=1, highlighted_ranges=[], early_return=False, 
                override_routing_strategy=None): 
    
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
                    
    lb_decisions = {} 
    # for flow in all_flows:
    
    fit_strategy = run_context["routing-fit-strategy"] 
    if override_routing_strategy is not None:
        fit_strategy = override_routing_strategy
        
    # TODO: the times ranges can be calculated in here, instead of copying in each of the functions. 
    ############################################################################################################  
    # experimental code for graph coloring.
    ############################################################################################################  
    if fit_strategy == "graph-coloring-v2":  
        times_range = route_flows_graph_coloring_v2(all_flows, rem, usage, num_spines, 
                                                      lb_decisions, run_context)
        
    elif fit_strategy == "graph-coloring-v3":
        times_range = route_flows_graph_coloring_v3(all_flows, rem, usage, num_spines, 
                                                   lb_decisions, run_context)
            
    elif fit_strategy == "graph-coloring-v4":
        times_range = route_flows_graph_coloring_v4(all_flows, rem, usage, num_spines, 
                                                    lb_decisions, run_context)
                    
    elif fit_strategy == "graph-coloring-v5":
        times_range = route_flows_graph_coloring_v5(all_flows, rem, usage, num_spines, 
                                                    lb_decisions, run_context, 
                                                    max_subflow_count, link_bandwidth, suffix, 
                                                    highlighted_ranges, early_return)
    else: # regular execution path 
        times_range = route_flows_one_by_one(all_flows, rem, usage, num_spines,   
                                             lb_decisions, run_context, max_subflow_count)
        
    min_affected_time, max_affected_time, bad_ranges = times_range
        
    if run_context["plot-routing-assignment"]: 
        plot_routing(run_context, rem, usage, all_job_ids, 
                     num_leaves, num_spines, routing_time, 
                     min_affected_time, max_affected_time, 
                     routing_plot_dir, smoothing_window=1, 
                     suffix=suffix)
    
    
    lb_decisions_proper = []    
    
    for (job_id, flow_id, iteration), selected_spines in lb_decisions.items():
        lb_decisions_proper.append({
            "job_id": job_id,
            "flow_id": flow_id,
            "iteration": iteration,
            "spine_count": len(selected_spines),     
            "spine_rates": [(s, mult) for s, mult in selected_spines]
        })

    return lb_decisions_proper, bad_ranges

