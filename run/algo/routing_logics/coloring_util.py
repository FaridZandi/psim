from collections import deque, defaultdict
import random 

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


def color_bipartite_multigraph_helper(input_edges):
    if not input_edges:
        return {}, 0

    edges = [(r[0], r[1]) for r in input_edges]   
  
    max_degree = compute_max_degree(edges)
    edge_list = [(u, v) for u, v in edges]
    n = len(edge_list)
    colors = [0] * n
    remaining = list(range(n))
     
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
        
        for u in graph:
            random.shuffle(graph[u])    

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
        
        
############################################################################################################
############################################################################################################
############################################################################################################

# apparently the greedy coloring algorithm is not working well for the bipartite multigraph coloring problem
# sometimes it uses more colors than the maximum degree of the graph

# this is just a wrapper function to call the helper function multiple times until the number of colors used 
# is <= maximum degree. this is probably an astoundingly stupid way to do this, but it works most of the time

# I tested it a couple of times and it seems to work, within a couple of attempts. 
# I should probably try adding more randomness to the base algorithm, but I'm not sure how to do that.
# now I just shuffle the edges before coloring them, but not sure if that's enough.

# seriously, though, why isn't there a proper algorithm for this available? I'm pretty sure this is a well-known problem
# I found papers on it, but I'm not going to implement a proper algorithm from scratch.

def color_bipartite_multigraph(input_edges):
    
    edge_color_map, max_degree = color_bipartite_multigraph_helper(input_edges) 
    colors_used_count = len(set(edge_color_map.values()) ) 

    round = 1
    while colors_used_count > max_degree and round < 10:
        edge_color_map, max_degree = color_bipartite_multigraph_helper(input_edges) 
        colors_used_count = len(set(edge_color_map.values()) ) 
        round += 1  
        
    return edge_color_map, max_degree