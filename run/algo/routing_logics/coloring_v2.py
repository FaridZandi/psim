from algo.routing_logics.routing_util import update_time_range
from algo.routing_logics.coloring_util import color_bipartite_multigraph

import sys  

def route_flows_graph_coloring_v2(all_flows, rem, usage, num_spines, 
                                     lb_decisions, run_context):

    min_affected_time = 1e9   
    max_affected_time = 0 
    
    all_flows.sort(key=lambda x: x["eff_start_time"])
        
    current_flow_idx = 0    
    set_counter = 0 
            
    while current_flow_idx < len(all_flows):
        set_counter += 1   
        
        this_set_start_time = all_flows[current_flow_idx]["eff_start_time"]
        this_set_job_id = all_flows[current_flow_idx]["job_id"] 

        current_flows = []
        while (all_flows[current_flow_idx]["eff_start_time"] == this_set_start_time and 
                all_flows[current_flow_idx]["job_id"] == this_set_job_id): 
            
            current_flows.append(all_flows[current_flow_idx])
            current_flow_idx += 1
            if current_flow_idx >= len(all_flows):
                break
        
        print(f"Processing {len(current_flows)} flows for job {this_set_job_id} starting at time {this_set_start_time}", file=sys.stderr) 
                
        edges = [] 
        flow_counter = 0 
        
        for flow in current_flows:  
            src_leaf = flow["srcrack"]
            dst_leaf = flow["dstrack"]
            start_time = flow["eff_start_time"]  
            end_time = flow["eff_end_time"] 
            
            flow_counter += 1
            edges.append((f"{src_leaf}_l", f"{dst_leaf}_r", flow_counter))    
        
        edge_color_map, _ = color_bipartite_multigraph(edges)
        
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
        
    return min_affected_time, max_affected_time, []