from algo.routing_logics.routing_util import update_time_range
from algo.routing_logics.coloring_util import color_bipartite_multigraph
# from algo.routing_logics.routing_util import merge_overlapping_ranges
# from algo.routing_logics.routing_util import find_value_in_range
from algo.routing_logics.routing_plot_util import plot_time_ranges  

from pprint import pprint 
from collections import defaultdict

import sys 
import hashlib
import math
import networkx as nx



def merge_overlapping_ranges_v7(ranges_dict, 
                                traffic_pattern_to_src_racks, 
                                traffic_pattern_to_dst_racks):

    def racks_overlap(src_a, dst_a, src_b, dst_b):
        if src_a & src_b:
            return True
        if dst_a & dst_b:
            return True
        return False

    # Flatten all intervals with their corresponding key and rack sets
    intervals = []
    for key, ranges in ranges_dict.items():
        src_racks = set(traffic_pattern_to_src_racks.get(key, set()))
        dst_racks = set(traffic_pattern_to_dst_racks.get(key, set()))
        for start, end in ranges:
            intervals.append([start, end, key, src_racks, dst_racks])

    # Sort by start time
    intervals.sort(key=lambda x: x[0])

    interval_count = len(intervals)
    if interval_count == 0:
        return defaultdict(list)

    parent = list(range(interval_count))
    rank = [0] * interval_count

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x, y):
        root_x = find(x)
        root_y = find(y)
        if root_x == root_y:
            return
        if rank[root_x] < rank[root_y]:
            parent[root_x] = root_y
        elif rank[root_x] > rank[root_y]:
            parent[root_y] = root_x
        else:
            parent[root_y] = root_x
            rank[root_x] += 1

    active = []

    for idx, (start, end, _, src_racks, dst_racks) in enumerate(intervals):
        active = [i for i in active if intervals[i][1] >= start]

        for active_idx in active:
            active_start, active_end, _, active_src, active_dst = intervals[active_idx]
            if active_end >= start and racks_overlap(active_src, active_dst, src_racks, dst_racks):
                union(idx, active_idx)

        active.append(idx)

    component_ranges = defaultdict(list)
    component_keys = defaultdict(set)

    for idx, (start, end, key, _, _) in enumerate(intervals):
        root = find(idx)
        component_ranges[root].append((start, end))
        component_keys[root].add(key)

    new_ranges = defaultdict(list)

    for root, ranges in component_ranges.items():
        keys = component_keys[root]
        ranges.sort(key=lambda x: x[0])

        # Merge overlapping and back-to-back ranges
        summarized_ranges = []
        last_range = None
        for start, end in ranges:
            if last_range is None:
                last_range = (start, end)
            elif start <= last_range[1] + 1:  # Merge if overlapping or back-to-back
                last_range = (last_range[0], max(last_range[1], end))
            else:
                summarized_ranges.append(last_range)
                last_range = (start, end)
        if last_range is not None:
            summarized_ranges.append(last_range)

        comb_key = tuple(sorted(keys))
        new_ranges[comb_key].extend(summarized_ranges)

    for comb_key in list(new_ranges.keys()):
        ranges = sorted(new_ranges[comb_key])
        merged_ranges = []
        for start, end in ranges:
            if merged_ranges and start <= merged_ranges[-1][1] + 1:
                prev_start, prev_end = merged_ranges[-1]
                merged_ranges[-1] = (prev_start, max(prev_end, end))
            else:
                merged_ranges.append((start, end))
        new_ranges[comb_key] = merged_ranges

    return new_ranges 


def find_value_in_range_v7(entries, value, pattern_hash):
    for entry in entries:
        start, end = entry["time_range"]
        if start <= value <= end and pattern_hash in entry["patterns"]:
            return entry["coloring"]
    return None


def plot_rack_dependencies(hash_to_time_ranges, 
                           traffic_pattern_to_src_racks, 
                           traffic_pattern_to_dst_racks, 
                           plot_path):
    import matplotlib.pyplot as plt

    G = nx.Graph()
    hashes = list(hash_to_time_ranges.keys())

    # Add nodes
    hash_to_text = {}   
    for h in hashes:
        text = f"{h} S:{list(traffic_pattern_to_src_racks[h])} D:{list(traffic_pattern_to_dst_racks[h])}"
        hash_to_text[h] = text
        G.add_node(text) 

    # Add edges if two patterns share any src or dst racks
    for i in range(len(hashes)):
        for j in range(i + 1, len(hashes)):
            h1, h2 = hashes[i], hashes[j]
            src_overlap = traffic_pattern_to_src_racks[h1] & traffic_pattern_to_src_racks[h2]
            dst_overlap = traffic_pattern_to_dst_racks[h1] & traffic_pattern_to_dst_racks[h2]
            if src_overlap or dst_overlap:
                G.add_edge(hash_to_text[h1], hash_to_text[h2])

    pos = nx.spring_layout(G)
    plt.figure(figsize=(10, 8))
    nx.draw(G, pos, with_labels=True, node_color='lightblue', edge_color='gray', font_size=8)
    plt.title("Rack Dependency Graph (Traffic Pattern Hashes)")
    plt.tight_layout()
    plt.savefig(plot_path)
    plt.close()


def route_flows_graph_coloring_v7(all_flows, rem, usage, num_spines, 
                                  lb_decisions, run_context, max_subflow_count, link_bandwidth, 
                                  suffix=1, highlighted_ranges=[], early_return=False): 


    # open a file to log the decisions.
    # log_path = "{}/routing/routing_log_{}.txt".format(run_context["routings-dir"], suffix)  
    # log_file = open(log_path, "w")
    # log_file.write("job_id, flow_id, iteration, selected_spines\n")
    
    available_colors_max = num_spines * max_subflow_count

    min_affected_time = 1e9   
    max_affected_time = 0 
    
    signature_length = 16
    all_flows.sort(key=lambda x: x["eff_start_time"])

    for f in all_flows: 
        f["traffic_id"] = f"{f['eff_start_time']}_{f['job_id']}_{f['throttle_rate']}"
        
        subflow_capacity = link_bandwidth / max_subflow_count 
        f["needed_subflows"] = int(math.ceil(f["max_load"] / subflow_capacity))    

        f["traffic_member_id"] = f"{f['job_id']}_{f['srcrack']}_{f['dstrack']}_{f['needed_subflows']}"
    
    # group the flows by the traffic_id.        
    all_traffic_ids = set([flow["traffic_id"] for flow in all_flows])    
    traffic_id_to_flows = defaultdict(list)
    
    for flow in all_flows:  
        traffic_id_to_flows[flow["traffic_id"]].append(flow)
    
    traffic_id_to_hash = {} 
    hash_to_traffic_id = {} 
    hash_to_time_ranges = defaultdict(list)
    traffic_pattern_to_src_racks = defaultdict(set)
    traffic_pattern_to_dst_racks = defaultdict(set)
    
    for traffic_id in all_traffic_ids:
        flows = traffic_id_to_flows[traffic_id]

        # sort them on the basis of the identifier.
        flows.sort(key=lambda x: x["traffic_member_id"])
        
        traffic_pattern = "#".join([flow["traffic_member_id"] for flow in flows])
        traffic_pattern_hash = hashlib.md5(traffic_pattern.encode()).hexdigest()
        traffic_pattern_hash = traffic_pattern_hash[:signature_length]
        
        max_end_time = max([flow["eff_end_time"] for flow in flows])    
        min_start_time = min([flow["eff_start_time"] for flow in flows]) 
        
        traffic_time_range = (min_start_time, max_end_time)
        
        traffic_id_to_hash[traffic_id] = traffic_pattern_hash 
        # this will get overwritten, but that's fine, we need just one.   
        hash_to_traffic_id[traffic_pattern_hash] = traffic_id 
        hash_to_time_ranges[traffic_pattern_hash].append(traffic_time_range)
        
        for flow in flows:
            flow["traffic_pattern_hash"] = traffic_pattern_hash 
            traffic_pattern_to_src_racks[traffic_pattern_hash].add(flow["srcrack"])
            traffic_pattern_to_dst_racks[traffic_pattern_hash].add(flow["dstrack"])
            # log_file.write(f"{flow['job_id']}, {flow['flow_id']}, {flow['iteration']}, {flow['srcrack']}-{flow['dstrack']}\n")
        # print(f"traffic_id: {traffic_id}, hash: {traffic_pattern_hash}, traffic_pattern: {traffic_pattern}", file=sys.stderr)

    # log_file.close()
    
    for hash in hash_to_traffic_id.keys():
        traffic_pattern_rep = hash_to_traffic_id[hash]
        flows = traffic_id_to_flows[traffic_pattern_rep]
        traffic_pattern = "#".join([flow["traffic_member_id"] for flow in flows])
        # print(f"hash: {hash}, traffic_pattern: {traffic_pattern}", file=sys.stderr) 
        
    # unique hash values.   
    for key in hash_to_time_ranges.keys():
        hash_to_time_ranges[key].sort()
    
    if run_context["plot-merged-ranges"]:   
        plot_rack_dependencies(hash_to_time_ranges, 
                            traffic_pattern_to_src_racks, 
                            traffic_pattern_to_dst_racks, 
                                "{}/routing/rack_dependency_{}.png".format(run_context["routings-dir"], suffix))
    
    
    merged_ranges = merge_overlapping_ranges_v7(hash_to_time_ranges, 
                                                traffic_pattern_to_src_racks, 
                                                traffic_pattern_to_dst_racks)

    # pprint(merged_ranges, stream=log_file)
    # log_file.close()
        
    needed_color_count = {} 
    max_degrees = {} 
    solutions = [] 
    bad_ranges = []

    for overlapping_keys, overlapping_ranges in merged_ranges.items():
        current_flows = []
        # for all the hashes that are overlapping, get the traffic patterns, put them all together
        
        for hash in overlapping_keys:
            traffic_pattern_rep = hash_to_traffic_id[hash]
            flows = traffic_id_to_flows[traffic_pattern_rep]
            # print(f"Processing flows for hash: {hash}, len flows: {len(flows)}", file=sys.stderr)
            
            # append the flows of a representative traffic pattern to the current mix
            current_flows.extend(flows)
            
        # print("Current flows count: ", len(current_flows), file=sys.stderr)
        
        edges = [] 
        subflow_counter = 0 

        for flow in current_flows:  
            # print(f"flow max load: {flow['max_load']}, needed subflows: {flow['needed_subflows']}", file=sys.stderr)
            
            for subflow in range(flow["needed_subflows"]):
                subflow_counter += 1

                src_leaf = flow["srcrack"]
                dst_leaf = flow["dstrack"]
                
                edges.append((f"{src_leaf}_l", f"{dst_leaf}_r", subflow_counter))    
            
        # print(f"Edges count: {len(edges)}", file=sys.stderr)    
        
        edge_color_map, max_degree = color_bipartite_multigraph(edges)
        color_id_to_color = defaultdict(list)
            
        all_colors_used = set(edge_color_map.values()) 
        colors_used_count = len(all_colors_used) 
        
        if colors_used_count > max_degree: 
            # there's something wrong about this. 
            print("edges:", edges, file=sys.stderr)
            print("edge_color_map:", edge_color_map, file=sys.stderr)
            print("max_degree:", max_degree, file=sys.stderr)
            print("all_colors_used:", all_colors_used, file=sys.stderr)
            
        # print(f"Colors used count: {colors_used_count}", file=sys.stderr)
        
        subflow_counter = 0
        
        for flow in current_flows:
            for subflow in range(flow["needed_subflows"]):
                subflow_counter += 1
                traffic_pattern_hash = flow["traffic_pattern_hash"]
                color_id = flow["traffic_pattern_hash"] + "_" + flow["traffic_member_id"]   
                color = edge_color_map[subflow_counter]
                color_id_to_color[color_id].append(color)
        
        # pprint(color_id_to_color, stream=sys.stderr)
                    
        for time_range in overlapping_ranges:
            color_map_snapshot = {key: list(colors) for key, colors in color_id_to_color.items()}
            solutions.append({
                "time_range": time_range,
                "patterns": set(overlapping_keys),
                "coloring": color_map_snapshot,
            })
            
            used_spines = colors_used_count / max_subflow_count
            if used_spines > num_spines:
                bad_ranges.append(time_range)
            
            if time_range in needed_color_count:
                needed_color_count[time_range] = max(needed_color_count[time_range], used_spines)
            else: 
                needed_color_count[time_range] = used_spines
            max_degrees[time_range] = max_degree / max_subflow_count

    print("solutions:", solutions, file=sys.stderr)
    
    if run_context["plot-merged-ranges"]:   
        plot_path = "{}/routing/merged_ranges_{}.png".format(run_context["routings-dir"], suffix)  
        plot_time_ranges(hash_to_time_ranges, dict(merged_ranges), 
                         needed_color_count, max_degrees, num_spines,
                         highlighted_ranges, None, plot_path)
    
    # use pprint to stderr 
    # pprint(solutions, stream=sys.stderr)
    
    if early_return and len(bad_ranges) > 0:
        return min_affected_time, max_affected_time, bad_ranges
    
    for flow in all_flows:
        src_leaf = flow["srcrack"]
        dst_leaf = flow["dstrack"]
        start_time = flow["eff_start_time"] 
        end_time = flow["eff_end_time"]     
        job_id = flow["job_id"] 
        flow_id = flow["flow_id"]
        iteration = flow["iteration"]
        
        color_id = flow["traffic_pattern_hash"] + "_" + flow["traffic_member_id"]
        
        pattern_hash = flow["traffic_pattern_hash"]
        time_range_coloring = find_value_in_range_v7(solutions, start_time, pattern_hash)
        if time_range_coloring is None:
            print(f"Time range not found for flow: {flow}")
            exit(f"Time range not found for flow: {flow}")
        
        def rotate(somelist):
            return somelist[1:] + [somelist[0]] 
                
        chosen_spines = []
        
        for i in range(flow["needed_subflows"]):
            # draw max_flow_count colors from the list. 
            color = time_range_coloring[color_id][0]
            time_range_coloring[color_id] = rotate(time_range_coloring[color_id])
            
            chosen_spine = color - 1 
            chosen_spine = chosen_spine // max_subflow_count
            chosen_spine = chosen_spine % num_spines # just in case.    
            
            chosen_spines.append(chosen_spine)
            
            # rotate the list. this is done assuming that all the members of this 
            # traffic pattern will happen at the same time. So they will consume 
            # all the list, and leave it as it was in the beginning, for the next
            # set of flows.

        chosen_spine_count = defaultdict(int)
        for spine in chosen_spines:
            chosen_spine_count[spine] += 1  
            
        selected_spines = [] 
        for spine, count in chosen_spine_count.items():         
            ratio = count / flow["needed_subflows"]    
            selected_spines.append((spine, ratio))

        lb_decisions[(job_id, flow_id, iteration)] = selected_spines 
        
        min_affected_time = min(min_affected_time, start_time)  
        max_affected_time = max(max_affected_time, end_time)
        
        if run_context["plot-routing-assignment"]:
            update_time_range(start_time, end_time, flow, selected_spines, rem, usage, 
                              src_leaf, dst_leaf)
        
    bad_ranges.sort()
    
    return min_affected_time, max_affected_time, bad_ranges
