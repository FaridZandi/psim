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



def merge_overlapping_ranges_v8(ranges_dict, 
                                traffic_pattern_to_src_racks, 
                                traffic_pattern_to_dst_racks):

    def racks_overlap(src_a, dst_a, src_b, dst_b):
        if src_a & src_b:
            return True
        if dst_a & dst_b:
            return True
        return False

    intervals = []
    for key, ranges in ranges_dict.items():
        src_racks = set(traffic_pattern_to_src_racks.get(key, set()))
        dst_racks = set(traffic_pattern_to_dst_racks.get(key, set()))
        for start, end in ranges:
            intervals.append([start, end, key, src_racks, dst_racks])

    intervals.sort(key=lambda x: x[0])

    interval_count = len(intervals)
    if interval_count == 0:
        return []

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

    component_key_ranges = defaultdict(lambda: defaultdict(list))
    component_keys = defaultdict(set)

    for idx, (start, end, key, _, _) in enumerate(intervals):
        root = find(idx)
        component_key_ranges[root][key].append((start, end))
        component_keys[root].add(key)

    components = []
    for root, key_ranges in component_key_ranges.items():
        ranges_copy = {}
        for key, ranges in key_ranges.items():
            ranges_copy[key] = sorted(ranges)
        components.append({
            "keys": tuple(sorted(component_keys[root])),
            "ranges": ranges_copy,
        })

    return components 


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

        while src_rack >= len(edge_count_in) or dst_rack >= len(edge_count_out):
            edge_count_in.append([0] * (flows_max_time + 1))
            edge_count_out.append([0] * (flows_max_time + 1))   
        
        for t in range(start_time, end_time + 1):
            edge_count_in[dst_rack][t] += needed_subflows   
            edge_count_out[src_rack][t] += needed_subflows  

    max_edge_count = [0] * (flows_max_time + 1)
    for r in range(len(edge_count_in)):
        for t in range(flows_max_time + 1):
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
    pattern_hash_to_flows = {}
    
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
        pattern_hash_to_flows[traffic_pattern_hash] = flows
        
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
    
    
    components = merge_overlapping_ranges_v8(hash_to_time_ranges, 
                                             traffic_pattern_to_src_racks, 
                                             traffic_pattern_to_dst_racks)

    needed_color_count = {} 
    max_degrees = {} 
    solutions = [] 
    bad_ranges = []
    merged_ranges_for_plot = defaultdict(list)
    pattern_assignments_global = {}

    def recompute_color_assignments(active_patterns, existing_assignments):
        if not active_patterns:
            return {}, set(), 0

        edges = []
        subflow_meta = []
        subflow_counter = 0

        for pattern_hash in sorted(active_patterns):
            flows = pattern_hash_to_flows.get(pattern_hash, [])
            for flow in flows:
                src_leaf = flow["srcrack"]
                dst_leaf = flow["dstrack"]
                for _ in range(flow["needed_subflows"]):
                    subflow_counter += 1
                    edges.append((f"{src_leaf}_l", f"{dst_leaf}_r", subflow_counter))
                    subflow_meta.append((pattern_hash, flow["traffic_member_id"]))

        if not edges:
            return {}, set(), 0

        edge_color_map, max_degree = color_bipartite_multigraph(edges)

        new_assignments = defaultdict(lambda: defaultdict(list))
        for idx, (pattern_hash, member_id) in enumerate(subflow_meta):
            color = edge_color_map[idx + 1]
            color_id = f"{pattern_hash}_{member_id}"
            new_assignments[pattern_hash][color_id].append(color)

        # freeze defaultdicts
        new_assignments = {
            pattern_hash: {color_id: list(colors) for color_id, colors in color_map.items()}
            for pattern_hash, color_map in new_assignments.items()
        }

        final_assignments = {}
        color_remap = {}

        # Preserve colors for patterns that were already active
        for pattern_hash, assignment in new_assignments.items():
            if pattern_hash not in existing_assignments:
                continue

            preserved = {}
            previous_assignment = existing_assignments[pattern_hash]

            for color_id, new_colors in assignment.items():
                if color_id not in previous_assignment:
                    raise RuntimeError(f"Missing historical colors for {color_id}")

                old_colors = previous_assignment[color_id]
                if len(new_colors) != len(old_colors):
                    raise RuntimeError(f"Color width changed for {color_id}")

                remapped = []
                for new_color, old_color in zip(new_colors, old_colors):
                    mapped_color = color_remap.setdefault(new_color, old_color)
                    if mapped_color != old_color:
                        raise RuntimeError("Conflicting color remap detected")
                    remapped.append(mapped_color)

                preserved[color_id] = remapped

            final_assignments[pattern_hash] = preserved

        # Assign colors for newly active patterns
        for pattern_hash, assignment in new_assignments.items():
            if pattern_hash in final_assignments:
                continue

            assigned = {}
            for color_id, new_colors in assignment.items():
                remapped = []
                for new_color in new_colors:
                    mapped_color = color_remap.setdefault(new_color, new_color)
                    remapped.append(mapped_color)
                assigned[color_id] = remapped
            final_assignments[pattern_hash] = assigned

        all_colors_used = set()
        for assignment in final_assignments.values():
            for colors in assignment.values():
                all_colors_used.update(colors)

        return final_assignments, all_colors_used, max_degree

    for component in components:
        key_ranges = component["ranges"]
        events = []

        for pattern_hash, ranges in key_ranges.items():
            for start, end in ranges:
                events.append((start, "start", pattern_hash))
                events.append((end + 1, "end", pattern_hash))

        if not events:
            continue

        events.sort()

        active_patterns = set()
        active_assignments = {}
        slice_start = None
        current_solver_max_degree = 0
        idx = 0

        while idx < len(events):
            time_marker = events[idx][0]

            if slice_start is not None and active_patterns and slice_start <= time_marker - 1:
                key_tuple = tuple(sorted(active_patterns))
                merged_ranges_for_plot[key_tuple].append((slice_start, time_marker - 1))

                color_snapshot = {}
                for pattern_hash in active_patterns:
                    assignment = active_assignments.get(pattern_hash, {})
                    for color_id, colors in assignment.items():
                        color_snapshot[color_id] = list(colors)

                if color_snapshot:
                    solutions.append({
                        "time_range": (slice_start, time_marker - 1),
                        "patterns": set(active_patterns),
                        "coloring": color_snapshot,
                    })

                    colors_used = set()
                    for colors in color_snapshot.values():
                        colors_used.update(colors)

                    used_spines = len(colors_used) / max_subflow_count if max_subflow_count else 0
                    if used_spines > num_spines:
                        bad_ranges.append((slice_start, time_marker - 1))

                    needed_color_count[(slice_start, time_marker - 1)] = max(
                        needed_color_count.get((slice_start, time_marker - 1), 0),
                        used_spines,
                    )

                    reference_degree = current_solver_max_degree if current_solver_max_degree else len(colors_used)
                    max_degrees[(slice_start, time_marker - 1)] = reference_degree / max_subflow_count if max_subflow_count else 0

            leaving = []
            entering = []

            while idx < len(events) and events[idx][0] == time_marker:
                _, event_type, pattern_hash = events[idx]
                if event_type == "end":
                    leaving.append(pattern_hash)
                else:
                    entering.append(pattern_hash)
                idx += 1

            for pattern_hash in leaving:
                if pattern_hash in active_patterns:
                    active_patterns.remove(pattern_hash)
                    active_assignments.pop(pattern_hash, None)

            need_recolor = False
            for pattern_hash in entering:
                if pattern_hash not in active_patterns:
                    need_recolor = True
                active_patterns.add(pattern_hash)
                if pattern_hash in pattern_assignments_global:
                    active_assignments[pattern_hash] = pattern_assignments_global[pattern_hash]

            if need_recolor and active_patterns:
                existing_assignments = {
                    pattern_hash: pattern_assignments_global[pattern_hash]
                    for pattern_hash in active_patterns
                    if pattern_hash in pattern_assignments_global
                }

                final_assignments, colors_used, solver_max_degree = recompute_color_assignments(
                    active_patterns, existing_assignments
                )

                for pattern_hash, assignment in final_assignments.items():
                    pattern_assignments_global[pattern_hash] = assignment
                    active_assignments[pattern_hash] = assignment

                current_solver_max_degree = solver_max_degree
            elif not active_patterns:
                current_solver_max_degree = 0
            elif active_patterns:
                colors_in_use = set()
                for assignment in active_assignments.values():
                    for colors in assignment.values():
                        colors_in_use.update(colors)
                current_solver_max_degree = len(colors_in_use)

            slice_start = time_marker if active_patterns else None

    print("solutions:", solutions, file=sys.stderr)
    
    if run_context["plot-merged-ranges"]:   
        plot_path = "{}/routing/merged_ranges_{}.png".format(run_context["routings-dir"], suffix)  
        plot_time_ranges(hash_to_time_ranges, merged_ranges_for_plot, 
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
        if color_id not in time_range_coloring:
            raise RuntimeError(f"Coloring missing for {color_id} at {start_time}")
        
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