import matplotlib.pyplot as plt
import numpy as np  
from collections import defaultdict 

from copy import deepcopy
from pprint import pprint

import os
import sys 

import networkx as nx



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

def plot_routing(run_context, rem, usage, all_job_ids, num_leaves, 
               num_spines, routing_time, min_affected_time, 
               max_affected_time, plots_dir, smoothing_window=1, suffix=""): 
    
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
    for ext in ['png']:
        plt_path = os.path.join(plots_dir, 'remaining_{}_{}.{}'.format(smoothing_window, suffix, ext))
        plt.savefig(plt_path)
    plt.close(fig)

    sys.stderr.write("Combined subplot figure has been saved in the directory: {}\n".format(plots_dir))


def plot_ranged_keys_line(d, val_max, ax, label):   
    max_time = max([key[1] for key in d.keys()])   
    
    values = [0] * (max_time + 1)   
    
    for time_range, value in d.items():    
        for i in range(time_range[0], time_range[1] + 1):
            values[i] = value           
    
    ax.plot(range(max_time + 1), values, label=label)
    
    # draw a horizontal line at y=available_colors_max
    ax.axhline(y=val_max, color='r', linestyle='--')
    
def overlap_count(start, end, ranges):
    overlap_count = 0 
    for s, e in ranges:
        if start <= e and s <= end:
            overlap_count += 1 
    return overlap_count

def plot_ranges(ax, ranges): 
    plotted_ranges = [] 
    for i, (start, end) in enumerate(ranges):
        overlaps = overlap_count(start, end, plotted_ranges)   
        ax.plot([start, end], [overlaps, overlaps], marker='|', linewidth=5)
        plotted_ranges.append((start, end))

    ax.set_xlabel("Time")
    ax.set_title("Time Ranges by Key")


def plot_time_ranges(ranges_dict, merged_ranges_dict, needed_color_count, max_degrees, 
                     available_colors_max, bad_ranges, hash_to_traffic_id, plot_path):
    # two plots on top of each other
    fig, axes = plt.subplots(4, 1, figsize=(10, 10), sharex=True)   
    # adjust hspace
    fig.subplots_adjust(hspace=0.5)
    
    
    def plot_stuff(ax, data, other_ax=None):
        y = 0
        for key, ranges in data.items():
            for i, (start, end) in enumerate(ranges):
                ax.plot([start, end], [y, y], marker='|', label=f"{key}" if y == 0 else "")
                if other_ax is not None:
                    ax.axvline(x=start, color='gray', linestyle='--', linewidth=0.5)
                    other_ax.axvline(x=start, color='gray', linestyle='--', linewidth=0.5)
                    ax.axvline(x=end, color='gray', linestyle='--', linewidth=0.5)
                    other_ax.axvline(x=end, color='gray', linestyle='--', linewidth=0.5)
            y += 1
            
        ax.set_yticks(range(len(data)))
        ytick_labels = list(data.keys()) 
        translated_labels = [] 
        for ytick_label in ytick_labels:    
            if not isinstance(ytick_label, tuple):
                translated_labels.append(hash_to_traffic_id[ytick_label])
                continue
            else:
                translated_label = [] 
                for hash in ytick_label:
                    translated_label.append(hash_to_traffic_id[hash])
                translated_labels.append(tuple(translated_label))   
            
        ax.set_yticklabels(translated_labels)
        ax.set_xlabel("Time")
        ax.set_title("Time Ranges by Key")
        
    plot_stuff(axes[0], ranges_dict)    
    plot_stuff(axes[1], merged_ranges_dict, axes[0])
    
    # plot the needed color count
    plot_ranged_keys_line(max_degrees, available_colors_max, axes[2], "Max Degrees")
    plot_ranged_keys_line(needed_color_count, available_colors_max, axes[2], "Needed Color Count")
        
    axes[2].set_title("Color Count, Max Degree")
    axes[2].set_xlabel("Time")
    axes[2].legend() 
    
    plot_ranges(axes[3], bad_ranges)
    
    plt.savefig(plot_path, bbox_inches='tight', dpi=300)
    
    

def plot_edge_coloring(edges, edge_color_map, plot_path):
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
    
    # Position: place left nodes at x=-1, right nodes at x=+1
    pos = {}
    sorted_left = sorted(left_nodes)
    sorted_right = sorted(right_nodes)
    
    leaf_num = len(sorted_right)
    
    for i, node in enumerate(sorted_left):
        pos[node] = (-1, i)
    for j, node in enumerate(sorted_right):
        pos[node] = (1, j)
    
    fig, ax = plt.subplots(figsize=(leaf_num, leaf_num))

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
        start_rad = -0.1 * (count-1)
        
        for i, _ in enumerate(keys_list):
            # rad for this edge
            rad = start_rad + i * 0.1
            
            nx.draw_networkx_edges(
                G, pos,
                edgelist=[(u, v)],
                connectionstyle=f'arc3,rad={rad}',
                edge_color=plt.cm.tab20.colors[edge_color_map[keys_list[i]]],
                ax=ax
            )
    
    ax.set_title("Bipartite MultiGraph with Parallel Edges (Curved)")
    
    plt.axis("off")
    plt.savefig(plot_path)
    plt.clf()
    plt.close(fig)  
    
    