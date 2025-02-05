from algo.routing_logics.routing_util import update_time_range
from algo.routing_logics.coloring_util import color_bipartite_multigraph_2
from algo.routing_logics.routing_util import merge_overlapping_ranges
from algo.routing_logics.routing_util import find_value_in_range

from pprint import pprint 
from collections import defaultdict

import sys 
import hashlib


def route_flows_graph_coloring_v4(all_flows, rem, usage, num_spines, 
                                  lb_decisions, run_context):
    min_affected_time = 1e9   
    max_affected_time = 0 
    
    all_flows.sort(key=lambda x: x["eff_start_time"])

    for f in all_flows: 
        f["traffic_id"] = f"{f['eff_start_time']}_{f['job_id']}"
        f["traffic_member_id"] = f"{f['job_id']}_{f['srcrack']}_{f['dstrack']}"

    # group the flows by the traffic_id.        
    all_traffic_ids = set([flow["traffic_id"] for flow in all_flows])    
    traffic_id_to_flows = defaultdict(list)
    
    for flow in all_flows:  
        traffic_id_to_flows[flow["traffic_id"]].append(flow)
    
    traffic_id_to_hash = {} 
    hash_to_traffic_id = {} 
    hash_to_time_ranges = defaultdict(list)
    
    for traffic_id in all_traffic_ids:
        flows = traffic_id_to_flows[traffic_id]

        # sort them on the basis of the identifier.
        flows.sort(key=lambda x: x["traffic_member_id"])
        
        traffic_pattern = "#".join([flow["traffic_member_id"] for flow in flows])
        traffic_pattern_hash = hashlib.md5(traffic_pattern.encode()).hexdigest()
        traffic_pattern_hash = traffic_pattern_hash[:8]
        
        max_end_time = max([flow["eff_end_time"] for flow in flows])    
        min_start_time = min([flow["eff_start_time"] for flow in flows]) 
        
        traffic_time_range = (min_start_time, max_end_time)
        
        traffic_id_to_hash[traffic_id] = traffic_pattern_hash 
        # this will get overwritten, but that's fine, we need just one.   
        hash_to_traffic_id[traffic_pattern_hash] = traffic_id 
        hash_to_time_ranges[traffic_pattern_hash].append(traffic_time_range)
        
        for flow in flows:
            flow["traffic_pattern_hash"] = traffic_pattern_hash 
        
        # print(f"traffic_id: {traffic_id}, hash: {traffic_pattern_hash}, traffic_pattern: {traffic_pattern}", file=sys.stderr)

    for hash in hash_to_traffic_id.keys():
        traffic_pattern_rep = hash_to_traffic_id[hash]
        flows = traffic_id_to_flows[traffic_pattern_rep]
        traffic_pattern = "#".join([flow["traffic_member_id"] for flow in flows])
        print(f"hash: {hash}, traffic_pattern: {traffic_pattern}", file=sys.stderr) 
        
    # unique hash values.   
    for key in hash_to_time_ranges.keys():
        hash_to_time_ranges[key].sort()
        
    # print(hash_to_time_ranges, file=sys.stderr) 
    if "visualize-routing" in run_context and run_context["visualize-routing"]: 
        routing_plot_dir = run_context["routing-plot-dir"] 
        plot_path = routing_plot_dir + "/merged_ranges.png"
    else:
        plot_path = None
    merged_ranges = merge_overlapping_ranges(hash_to_time_ranges, plot_path)  

    solutions = {} 
    for overlapping_keys, overlapping_ranges in merged_ranges.items():
        current_flows = []
        # for all the hashes that are overlapping, get the traffic patterns, put them all together
        
        for hash in overlapping_keys:
            traffic_pattern_rep = hash_to_traffic_id[hash]
            flows = traffic_id_to_flows[traffic_pattern_rep]
            print(f"Processing flows for hash: {hash}, len flows: {len(flows)}", file=sys.stderr)
            
            # append the flows of a representative traffic pattern to the current mix
            current_flows.extend(flows)
            
        print("Current flows count: ", len(current_flows), file=sys.stderr)
        
        edges = [] 
        flow_counter = 0 

        for flow in current_flows:  
            flow_counter += 1

            src_leaf = flow["srcrack"]
            dst_leaf = flow["dstrack"]
            edges.append((f"{src_leaf}_l", f"{dst_leaf}_r", flow_counter))    
                
        edge_color_map = color_bipartite_multigraph_2(edges)
        color_id_to_color = defaultdict(list)    
        
        flow_counter = 0
        for flow in current_flows:
            flow_counter += 1
            traffic_pattern_hash = flow["traffic_pattern_hash"]
            color_id = flow["traffic_pattern_hash"] + "_" + flow["traffic_member_id"]   
            color = edge_color_map[flow_counter]

            color_id_to_color[color_id].append(color)
            
        for time_range in overlapping_ranges:
            solutions[time_range] = color_id_to_color   
            
    
    # use pprint to stderr 
    pprint(solutions, stream=sys.stderr)
    
    for flow in all_flows:
        src_leaf = flow["srcrack"]
        dst_leaf = flow["dstrack"]
        start_time = flow["eff_start_time"] 
        end_time = flow["eff_end_time"]     
        job_id = flow["job_id"] 
        flow_id = flow["flow_id"]
        iteration = flow["iteration"]
        
        color_id = flow["traffic_pattern_hash"] + "_" + flow["traffic_member_id"]
        
        time_range_coloring = find_value_in_range(solutions, start_time)
        color = time_range_coloring[color_id][0]
        # rotate the list. this is done assuming that all the members of this 
        # traffic pattern will happen at the same time. So they will consume 
        # all the list, and leave it as it was in the beginning, for the next
        # set of flows.
        def rotate(somelist):
            return somelist[1:] + [somelist[0]] 
        # corresponding_solution[color_id] = corresponding_solution[color_id][1:] + [corresponding_solution[color_id][0]]
        time_range_coloring[color_id] = rotate(time_range_coloring[color_id])
        
        chosen_spine = color - 1 
        chosen_spine = chosen_spine % num_spines # just in case.    
        selected_spines = [(chosen_spine, 1.0)] 
        
        lb_decisions[(job_id, flow_id, iteration)] = selected_spines 
        
        min_affected_time = min(min_affected_time, start_time)  
        max_affected_time = max(max_affected_time, end_time)
        
        update_time_range(start_time, end_time, flow, selected_spines, rem, usage, 
                            src_leaf, dst_leaf)     
        
        
    return min_affected_time, max_affected_time 