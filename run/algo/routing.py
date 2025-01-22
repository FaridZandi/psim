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

############################################################################################################
############################################################################################################
############################################################################################################

from collections import defaultdict


def bipartite_edge_color(G, left_partition):
    """
    Color the edges of a bipartite MultiGraph `G` with at most Delta colors,
    where Delta is the maximum degree of the graph.
    
    :param G: A NetworkX MultiGraph that is bipartite.
              Parallel edges are allowed.
    :param left_partition: The set (or list) of nodes in the 'left' bipartition.
                           All other nodes in G are assumed to be in the 'right' partition.
    :return: A dictionary mapping (u, v, key) -> color_index (an integer).
    """
    # 1) Collect all edges (u,v,key) in a set of uncolored edges.
    #    We'll store them in a list so we can refer back to them easily.
    uncolored_edges = set(G.edges(keys=True))
    
    # We'll keep track of the color assigned to each edge in this dictionary
    edge_color = dict()  # (u, v, key) -> color_index
    
    # 2) Repeatedly find a maximum matching among uncolored edges until none remain.
    color_index = 0
    
    while uncolored_edges:
        color_index += 1  # We'll assign this new color to the next matching
        
        # Build a directed flow network from the *uncolored* edges
        # We'll add a super-source 'S' and super-sink 'T'.
        flow_net = nx.DiGraph()
        S, T = "_SOURCE_", "_SINK_"
        
        # 2a) Add nodes
        flow_net.add_node(S)
        flow_net.add_node(T)
        for node in G.nodes():
            flow_net.add_node(node)
        
        # 2b) Add edges from S -> each node in left_partition with capacity=1
        #     This ensures each left-node can only match with 1 edge in the flow sense.
        for u in left_partition:
            flow_net.add_edge(S, u, capacity=1)
        
        # 2c) Add edges from each right-partition node -> T with capacity=1
        #     Ensures each right-node can only match with 1 edge.
        right_partition = set(G.nodes()) - set(left_partition)
        for v in right_partition:
            flow_net.add_edge(v, T, capacity=1)
        
        # 2d) For each uncolored edge in the bipartite MultiGraph, add an edge u->v in the flow_net
        #     with capacity=1 (u in left, v in right).
        #     Since G is undirected, we only add it in the left->right direction.
        for (u, v, k) in uncolored_edges:
            if u in left_partition and v in right_partition:
                # Each parallel edge is a distinct edge in the flow network
                flow_net.add_edge(u, v, capacity=1, key=k)
            elif v in left_partition and u in right_partition:
                # reversed
                flow_net.add_edge(v, u, capacity=1, key=k)
            else:
                # If the graph isn't truly bipartite, or the partition was incorrect,
                # we'd hit this case. We'll ignore or raise an error.
                pass
        
        # 2e) Compute maximum flow
        flow_value, flow_dict = maximum_flow(flow_net, S, T)
        
        # 2f) Extract which edges got "flow = 1" from left to right
        #     Those edges form our matching.
        matched_edges = []
        for (u, out_edges) in flow_dict.items():
            if u in left_partition:
                # Check each v in the adjacency of u
                for v, fval in out_edges.items():
                    # If there's flow of 1 on edge (u->v), that means it's in the matching
                    if fval == 1 and v in right_partition:
                        # We need to identify which (u, v, key) in G corresponds to this
                        # The flow network stored 'key' only in the capacity graph, so let's look that up.
                        # We'll check the data from flow_net[u][v].
                        # Because it's a DiGraph, we can store our "key" in the edge data:
                        # but networkx's maximum_flow merges capacities, so let's see how we can track it.
                        # 
                        # A simpler approach:
                        # Because we used separate edges for each parallel edge, we can do:
                        #    for each (u,v) in flow_net.edges(data=True):
                        #        if 'key' in data
                        # But that might be a bigger search. 
                        # So let's store a back-reference from flow_net to G by an attribute.
                        
                        # We'll do it the simpler way now:
                        # flow_net[u][v] might have 'key' in the attribute, but maximum_flow creates
                        # a residual graph, so let's be safe. We'll do:
                        matched_edges.append((u, v))
        
        # 2g) Assign the color_index to each matched edge in our original graph
        #     We have to figure out which (u, v, key) in G correspond to (u, v).
        #     Because we used `flow_net.add_edge(u, v, capacity=1, key=k)`,
        #     we can cross-check G's edges.
        #     For parallel edges, the direction (u,v) is unique to a single parallel edge in left->right mode.
        
        for (u, v) in matched_edges:
            # In the original bipartite multi-graph, the edge could be (u,v,k) or (v,u,k).
            # We only added left->right in flow_net, so let's assume (u,v,k) is the correct orientation.
            # We need to see *which* key it had. But we lost direct reference in the flow dict.
            #
            # A quick hack: We can look for edges in uncolored_edges that are exactly (u,v,k),
            #   given that it's left->right. If there's exactly one match, we color it.
            # In a real robust implementation, we'd store extra data. But let's do a quick approach:
            possible_keys = []
            if (u, v) in G.edges():
                # G[u][v] is a "AtlasView" of keys and attributes if it's a MultiGraph
                for k in G[u][v]:
                    # Check if that (u,v,k) is still uncolored
                    if (u, v, k) in uncolored_edges:
                        possible_keys.append(k)
            # We pick the first one (there should be exactly 1 in this approach):
            if possible_keys:
                chosen_key = possible_keys[0]
                edge_color[(u, v, chosen_key)] = color_index
                uncolored_edges.remove((u, v, chosen_key))
            else:
                # It's possible the real orientation in G is (v, u, k). Let's check that too:
                if (v, u) in G.edges():
                    for k in G[v][u]:
                        if (v, u, k) in uncolored_edges:
                            possible_keys.append(k)
                    if possible_keys:
                        chosen_key = possible_keys[0]
                        edge_color[(v, u, chosen_key)] = color_index
                        uncolored_edges.remove((v, u, chosen_key))
                # else no match found - should not happen in a correct bipartite setting
    
    return edge_color



def plot_bipartite_graph(edges, plot_path):
    """
    Plots a bipartite MultiGraph with potential parallel edges.
    Each pair of parallel edges is drawn with a different curvature (rad).
    """
    # Create a MultiGraph to keep parallel edges
    G = nx.MultiGraph()
    
    # Identify partitions (left vs right) based on context
    # If you truly have separate sets, you can pass them in directly
    # Here we just gather them from the edges for a bipartite layout
    left_nodes = set(src for src, _, edge_id in edges)
    right_nodes = set(dst for _, dst, edge_id in edges)
    
    # Add nodes and edges with edge_ids
    G.add_nodes_from(left_nodes, bipartite=0)
    G.add_nodes_from(right_nodes, bipartite=1)
    
    for e in edges:
        G.add_edge(e[0], e[1], key=e[2])    
    
    edge_colors = bipartite_edge_color(G, left_nodes) 
    
    # print(edges, file=sys.stderr)
    # print(edge_colors, file=sys.stderr)   
    
    # map the edge_ids to colors    
    edge_color_map = {} 
    for (u, v, key), color in edge_colors.items():
        edge_color_map[key] = color
        
    print(edge_color_map, file=sys.stderr)   
    
    
    return edge_color_map
    
    # Position: place left nodes at x=-1, right nodes at x=+1
    pos = {}
    sorted_left = sorted(left_nodes)
    sorted_right = sorted(right_nodes)
    
    for i, node in enumerate(sorted_left):
        pos[node] = (-1, i)
    for j, node in enumerate(sorted_right):
        pos[node] = (1, j)
    
    fig, ax = plt.subplots(figsize=(6, 4))

    # Draw the nodes (once)
    nx.draw_networkx_nodes(G, pos, nodelist=G.nodes(), node_color="lightblue", node_size=1200, ax=ax)
    nx.draw_networkx_labels(G, pos, labels={n: str(n) for n in G.nodes()}, ax=ax)
    
    # ---- Handle parallel edges with varying curvature ----
    # We'll group edges by their endpoints (since MultiGraph can have parallel edges).
    # Then, for each group, draw edges with different 'rad' values.
    edges_by_pair = defaultdict(list)
    # G.edges(data=True, keys=True) if you want a dictionary of edge attributes, or
    # G.edges(keys=True) if you'd like the edge “key” for parallel edges. But here
    # we just group by (u,v) ignoring any edge attributes:
    
    for (u, v, key) in G.edges(keys=True):
        if (u,v) not in edges_by_pair and (v,u) not in edges_by_pair:
            # store under a canonical ordering, e.g. smaller first
            # but for a bipartite graph, direction may not matter. We'll just store as is:
            edges_by_pair[(u,v)].append(key)
        elif (u,v) in edges_by_pair:
            edges_by_pair[(u,v)].append(key)
        else:
            edges_by_pair[(v,u)].append(key)
    
    # For each pair, draw parallel edges with distinct arcs
    for (u,v), keys_list in edges_by_pair.items():
        # total number of parallel edges
        count = len(keys_list)
        # We'll space the rad angles around 0. (e.g. -0.2, -0.1, 0, 0.1, 0.2 for count=5)
        
        # If you have a single edge, rad=0 => straight line
        # If you have two edges, you might do rad=-0.1, +0.1
        # etc.
        # Let's define an offset:
        start_rad = -0.1*(count-1)
        
        for i, _ in enumerate(keys_list):
            # rad for this edge
            rad = start_rad + i*0.1
            
            nx.draw_networkx_edges(
                G,
                pos,
                edgelist=[(u, v)],
                connectionstyle=f'arc3,rad={rad}',
                edge_color=plt.cm.tab20.colors[edge_colors[(u, v, keys_list[i])]],
                ax=ax
            )
    
    ax.set_title("Bipartite MultiGraph with Parallel Edges (Curved)")
    plt.axis("off")
    plt.savefig(plot_path)

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


def route_flow(flow, selection_strategy, spine_availablity, max_subflow_count, num_spines): 
    selected_spines = []    
    
    if selection_strategy == "best": 
        selected_spines = route_flows_best(spine_availablity, max_subflow_count)

    elif selection_strategy == "first": 
        selected_spines = route_flows_first(spine_availablity, max_subflow_count, 
                                            num_spines)
        
    elif selection_strategy == "random":
        selected_spines = route_flows_random(max_subflow_count, num_spines)              
        
    elif selection_strategy == "useall":
        selected_spines = route_flows_useall(num_spines) 
        
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
    
    
    # get all the flows at the front of the all_flows, that start at the same time. 
    # process them together, and then move on to the next set of flows. 
        
    # for flow in all_flows:
    fit_strategy = run_context["routing-fit-strategy"] 
     
     
    ############################################################################################################  
    # experimental code for graph coloring.
    ############################################################################################################  
    if fit_strategy == "graph-coloring":
        current_flow_idx = 0    
        set_counter = 0 
        
        while current_flow_idx < len(all_flows):
            set_counter += 1   
            
            this_set_start_time = all_flows[current_flow_idx]["eff_start_time"]

            current_flows = []
            while all_flows[current_flow_idx]["eff_start_time"] == this_set_start_time:
                current_flows.append(all_flows[current_flow_idx])
                current_flow_idx += 1
                if current_flow_idx >= len(all_flows):
                    break
            
            print(f"Processing {len(current_flows)} flows starting at time {this_set_start_time}", file=sys.stderr) 
                    
            edges = [] 
            flow_counter = 0 
            
            for flow in current_flows:  
                src_leaf = flow["srcrack"]
                dst_leaf = flow["dstrack"]
                flow_counter += 1
                edges.append((f"{src_leaf}_l", f"{dst_leaf}_r", flow_counter))      
            
            edge_color_map = plot_bipartite_graph(edges, 
                                                  f"{routing_plot_dir}/bipartite_{this_set_start_time}_{set_counter}.png")
            
            flow_counter = 0 
            
            for flow in current_flows:
                flow_counter += 1
                src_leaf = flow["srcrack"]
                dst_leaf = flow["dstrack"]
                start_time = flow["eff_start_time"] 
                end_time = flow["eff_end_time"]     
                job_id = flow["job_id"] 
                flow_id = flow["flow_id"]
                iteration = flow["iteration"]
                
                color = edge_color_map[flow_counter]
                chosen_spine = color - 1 
                chosen_spine = chosen_spine % num_spines # just in case.    
                selected_spines = [(chosen_spine, 1.0)] 
                
                lb_decisions[(job_id, flow_id, iteration)] = selected_spines 
                
                min_affected_time = min(min_affected_time, start_time)  
                max_affected_time = max(max_affected_time, end_time)
                
                update_time_range(start_time, end_time, flow, selected_spines, rem, usage, 
                                src_leaf, dst_leaf)     
                
    ############################################################################################################  
    else: # regular execution path 
        for flow in all_flows:
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

            selected_spines = route_flow(flow, run_context["routing-fit-strategy"], 
                                        spine_availablity, max_subflow_count, num_spines)

            lb_decisions[(job_id, flow_id, iteration)] = selected_spines 
            
            min_affected_time = min(min_affected_time, start_time)  
            max_affected_time = max(max_affected_time, end_time)
            
            update_time_range(start_time, end_time, flow, selected_spines, rem, usage, 
                            src_leaf, dst_leaf)       

            
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

