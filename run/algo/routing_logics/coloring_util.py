import matplotlib.pyplot as plt
import sys 
import networkx as nx
import matplotlib.pyplot as plt
from networkx.algorithms.flow import maximum_flow
from collections import deque, defaultdict


def color_bipartite_graph_1_helper(G, left_partition):
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


def color_bipartite_graph_1(edges, num_spines, plot_path):
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
    
    edge_colors = color_bipartite_graph_1_helper(G, left_nodes) 
    
    # print(edges, file=sys.stderr)
    # print(edge_colors, file=sys.stderr)   
    
    # map the edge_ids to colors    
    edge_color_map = {} 
    for (u, v, key), color in edge_colors.items():
        edge_color_map[key] = color
        
    print(edges, file=sys.stderr)
    print(edge_color_map, file=sys.stderr)   
    
    colors_used = set(edge_color_map.values())
    colors_used_num = len(colors_used) 
    
    if colors_used_num <= num_spines:
        # this is okay.
        return edge_color_map   


    # messed up:
    sys.stderr.write(f"Warning: {colors_used_num} colors used for {num_spines} spines\n")
    sys.stderr.write(f"Colors used: {colors_used}\n")
    sys.stderr.write(f"Edge color map: {edge_color_map}\n")
    sys.stderr.write(f"Edges: {edges}\n")   
    

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
                edge_color=plt.cm.tab20.colors[edge_colors[(u, v, keys_list[i])]],
                ax=ax
            )
    
    ax.set_title("Bipartite MultiGraph with Parallel Edges (Curved)")
    
    plt.axis("off")
    plt.savefig(plot_path)
    plt.close(fig)  
    
    return edge_color_map

############################################################################################################
############################################################################################################
############################################################################################################

def compute_max_degree(edges):
    degree = defaultdict(int)
    for u, v in edges:
        degree[u] += 1
        degree[v] += 1
    return max(degree.values(), default=0)

def hopcroft_karp(graph):
    pair_u = defaultdict(lambda: None)
    pair_v = defaultdict(lambda: None)
    dist = {}
    
    def bfs():
        queue = deque()
        for u in graph:
            if pair_u[u] is None:
                dist[u] = 0
                queue.append(u)
            else:
                dist[u] = float('inf')
        dist[None] = float('inf')
        
        while queue:
            u = queue.popleft()
            if u is not None:
                for v in graph[u]:
                    if dist[pair_v[v]] == float('inf'):
                        dist[pair_v[v]] = dist[u] + 1
                        queue.append(pair_v[v])
        return dist[None] != float('inf')
    
    def dfs(u):
        if u is not None:
            for v in graph[u]:
                if dist[pair_v[v]] == dist[u] + 1:
                    if dfs(pair_v[v]):
                        pair_u[u] = v
                        pair_v[v] = u
                        return True
            dist[u] = float('inf')
            return False
        return True
    
    while bfs():
        for u in list(graph.keys()):
            if pair_u[u] is None:
                dfs(u)
                
    return {k: v for k, v in pair_u.items() if v is not None}

def color_bipartite_multigraph_2(input_edges):
    if not input_edges:
        return []

    edges = [(r[0], r[1]) for r in input_edges]   
  
    max_degree = compute_max_degree(edges)
    edge_list = [(u, v) for u, v in edges]
    n = len(edge_list)
    colors = [0] * n
    remaining = set(range(n))
     
    for color in range(1, max_degree + 1):
        if not remaining:
            break
        
        # Build current bipartite graph of available edges
        uv_pairs = set()
        for idx in remaining:
            u, v = edge_list[idx]
            uv_pairs.add((u, v))
        
        # Create adjacency list for Hopcroft-Karp
        graph = defaultdict(list)
        for u, v in uv_pairs:
            graph[u].append(v)
        
        # Find maximum matching
        matching = hopcroft_karp(graph)
        
        # Color edges in the matching
        matched_pairs = [(u, v) for u, v in matching.items()]
        
        for u, v in matched_pairs:
            # Find first matching edge in remaining set
            for idx in list(remaining):
                if edge_list[idx] == (u, v):
                    colors[idx] = color
                    remaining.remove(idx)
                    break
    
    edge_color_map = {} 
    for i in range(n):  
        edge_color_map[i + 1] = colors[i]
        
    # print(f"Max degree: {max_degree}, colored with {len(set(colors))} colors", file=sys.stderr) 
    # print(f"Edge color map: {edge_color_map}", file=sys.stderr)
    
    return edge_color_map, max_degree   
        