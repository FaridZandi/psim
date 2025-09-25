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

import time as timesleep 




def merge_overlapping_ranges_v8(ranges_dict, 
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
        component_ranges[root].append((start, end, key))
        component_keys[root].add(key)

    new_ranges = defaultdict(list)

    for root, ranges in component_ranges.items():
        keys = component_keys[root]
        ranges.sort(key=lambda x: x[0])

        # Merge overlapping and back-to-back ranges
        # summarized_ranges = []
        # last_range = None
        # for start, end in ranges:
        #     if last_range is None:
        #         last_range = (start, end)
        #     elif start <= last_range[1] + 1:  # Merge if overlapping or back-to-back
        #         last_range = (last_range[0], max(last_range[1], end))
        #     else:
        #         summarized_ranges.append(last_range)
        #         last_range = (start, end)
        # if last_range is not None:
        #     summarized_ranges.append(last_range)
        # summarized_range = (min(r[0] for r in ranges), max(r[1] for r in ranges))
        
        comb_key = tuple(sorted(keys))
        new_ranges[comb_key].append(ranges)

    return new_ranges 


def find_value_in_range_v8(entries, value, pattern_hash):
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


def route_flows_graph_coloring_v8(all_flows, rem, usage, num_spines, 
                                  lb_decisions, run_context, max_subflow_count, link_bandwidth, 
                                  suffix=1, highlighted_ranges=[], early_return=False): 


    # open a file to log the decisions.
    # log_path = "{}/routing/routing_log_{}.txt".format(run_context["routings-dir"], suffix)  
    # log_file = open(log_path, "w")
    # log_file.write("job_id, flow_id, iteration, selected_spines\n")
    
    available_colors_max = num_spines * max_subflow_count
    subflow_capacity = link_bandwidth / max_subflow_count 

    min_affected_time = 1e9   
    max_affected_time = 0 
    
    signature_length = 16
    all_flows.sort(key=lambda x: x["eff_start_time"])

    rack_count = 0 
    
    ##############################    
    # The edge count on the ingress and egress of each rack at each time point. 
    # The edge count gives a lower bound on the number of colors needed.
    # if the edge count exceeds the available colors, that means that we cannot
    # possibly color the graph with the available colors. 
    # So if early_return is set, we can return the bad ranges right away. 
    # This is an optimization to avoid doing unnecessary work.
    # Note: There's another early return later, after the coloring is done (which is more accurate). 
    
    # Further notes: the usual case where this happens should normally be taken care of 
    # by the timing solver. Is this really needed?   
    ##############################    
    
    flows_max_time = max([f["eff_end_time"] for f in all_flows])
    edge_count_in = []
    edge_count_out = []
    for f in all_flows:
        start_time = f["eff_start_time"]
        end_time = f["eff_end_time"]
        
        src_rack = f["srcrack"]
        dst_rack = f["dstrack"]
        
        needed_subflows = int(math.ceil(f["max_load"] / subflow_capacity))    

        min_affected_time = min(min_affected_time, start_time)  
        max_affected_time = max(max_affected_time, end_time)

        rack_count = max(rack_count, src_rack + 1, dst_rack + 1) 
           
        while src_rack >= len(edge_count_in) or dst_rack >= len(edge_count_out):
            edge_count_in.append([0] * (flows_max_time + 1))
            edge_count_out.append([0] * (flows_max_time + 1))   
        
        for t in range(start_time, end_time + 1):
            edge_count_in[dst_rack][t] += needed_subflows   
            edge_count_out[src_rack][t] += needed_subflows  

    max_edge_count = [0] * (flows_max_time + 1)
    for r in range(len(edge_count_in)):
        for t in range(flows_max_time):
            max_edge_count[t] = max(max_edge_count[t], edge_count_in[r][t])
            max_edge_count[t] = max(max_edge_count[t], edge_count_out[r][t])
            
    if early_return:
        # find all the ranges where the max_edge_count exceeds available_colors_max
        bad_ranges = []
        in_bad_range = False
        range_start = None
        for t in range(len(max_edge_count)):
            if max_edge_count[t] > available_colors_max:
                if not in_bad_range:
                    in_bad_range = True
                    range_start = t
            else:
                if in_bad_range:
                    in_bad_range = False
                    bad_ranges.append((range_start, t - 1)) 
        if in_bad_range:
            bad_ranges.append((range_start, len(max_edge_count) - 1))
        
        if len(bad_ranges) > 0: 
            return min_affected_time, max_affected_time, bad_ranges
    ##############################    
    
    for f in all_flows: 
        f["traffic_id"] = f"{f['eff_start_time']}_{f['job_id']}_{f['throttle_rate']}"
        
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
    
    
    merged_ranges = merge_overlapping_ranges_v8(hash_to_time_ranges, 
                                                traffic_pattern_to_src_racks, 
                                                traffic_pattern_to_dst_racks)

    # pprint(merged_ranges, stream=log_file)
    # log_file.close()
        
    needed_color_count = {} 
    max_degrees = {} 
    solutions = [] 
    bad_ranges = []
        
    solutions = {} 
    
    for keys, time_ranges_list in merged_ranges.items():
        
        # all the joined patterns that share the same key set. 
        # each time_range in the time_ranges list is a list of (start, end, key) tuples.
        
        print("keys:", keys, file=sys.stderr)
        
        print("time_ranges:", file=sys.stderr)
        pprint(time_ranges_list, stream=sys.stderr)        
        
        
        for time_ranges in time_ranges_list: 
            # time_ranges is a list of (start, end, key) tuples.
             
            # making a representative graph for this set of keys. 
            # we go through the first pattern and and add all its edges to the graph. 
            # for each edge, we note that color_id = pattern_hash + "_" + traffic_member_id
            # and the time in which this edge is active. 
            # then we go through the second pattern, and so on.
            # when we see an edge that is already in the graph, we add another color_id to it.
            # but only if the time ranges don't overlap. 
            # if they do overlap, then we have to add another edge to the graph. 

            # therefore the data structure that we need while going through the patterns is: 
            # for each source-destination pair, a list of (time_range, color_ids)
            
            edges = [] 
            for r in range(rack_count):
                edges.append([])
                for c in range(rack_count):
                    edges[r].append([])
                    
            for time_range in time_ranges:
                start, end, key = time_range
                traffic_pattern_rep = hash_to_traffic_id[key]
                flows = traffic_id_to_flows[traffic_pattern_rep]
                
                for flow in flows:
                    for subflow in range(flow["needed_subflows"]):
                        src_rack = flow["srcrack"]
                        dst_rack = flow["dstrack"]
                        color_id = flow["traffic_pattern_hash"] + "_" + flow["traffic_member_id"] + f"_{subflow}"   
                        
                        # looking at the edges[src_rack][dst_rack] we see a list. 
                        # any of those entries could potentially be able to fit this new time range.
                        # if none of them can fit, we have to add a new entry.
                        
                        placed = False
                        for entry in edges[src_rack][dst_rack]:
                            is_good_entry = True
                            for entry_time_range, entry_color_ids in entry: 
                                # does it overlap with start,end? 
                                if not (end < entry_time_range[0] or start > entry_time_range[1]):
                                    is_good_entry = False
                                    break
                            if is_good_entry:
                                entry.append(((start, end), color_id))
                                placed = True
                                break
                        if not placed:
                            edges[src_rack][dst_rack].append([((start, end), color_id)])
                    
            input("above are the edges. press enter to continue...")
            
            # so now we have the edges. Let's do the coloring: 
            # we should make a list of edges to send to the coloring function: 
            
            coloring_edges = [] 
            
            for r in range(rack_count):
                for c in range(rack_count): 
                    if r == c: 
                        continue 
                    if len(edges[r][c]) == 0:
                        continue    
                    for i, entry in enumerate(edges[r][c]):
                        print(f"edge {i} between {r}->{c}:", file=sys.stderr) 
                        pprint(entry, stream=sys.stderr)    

                        coloring_edges.append((f"{r}_l", f"{c}_r", (r, c, i)))
            
            edge_color_map, max_degree = color_bipartite_multigraph(coloring_edges)            
            
            pprint(edge_color_map, stream=sys.stderr)
            
            input("above is the coloring. press enter to continue...")
            
            for edge_index, color in edge_color_map.items():
                r, c, i = coloring_edges[edge_index - 1][2]
                entry = edges[r][c][i]
                print(f"assigning color {color} to edge {r}->{c} index {i}: {entry}", file=sys.stderr)
                
                for time_range, color_id in entry:
                    solutions[(color_id, time_range)] = color

            pprint(solutions, stream=sys.stderr)

            input("above are the color assignments. press enter to continue...")
            
            # for this to be useful, we need to map the color_ids and time_ranges to the coloring 
            
                                    
    
    if run_context["plot-merged-ranges"]:   
        plot_path = "{}/routing/merged_ranges_{}.png".format(run_context["routings-dir"], suffix)  
        plot_time_ranges(hash_to_time_ranges, dict(merged_ranges), 
                         needed_color_count, max_degrees, num_spines,
                         highlighted_ranges, None, plot_path, max_edge_count)
    
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
        time_range_coloring = find_value_in_range_v8(solutions, start_time, pattern_hash)
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
        

        
    
    return min_affected_time, max_affected_time, bad_ranges
