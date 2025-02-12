from algo.routing_logics.routing_util import update_time_range, get_spine_availablity

import random 
import itertools

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
        
        # sys.stderr.write("Assigned spines count: {}, Fair assigned mult: {}, Min available mult: {}\n".format(
        #     assigned_spines_count, fair_assigned_mult, min_available_mult))
        
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

def route_one_flow(flow, selection_strategy, rem, max_subflow_count, num_spines): 
    src_leaf = flow["srcrack"]
    dst_leaf = flow["dstrack"]
    start_time = flow["eff_start_time"] 
    end_time = flow["eff_end_time"]   
    
    spine_availablity = get_spine_availablity(flow, rem, num_spines, 
                                              start_time, end_time, 
                                              src_leaf, dst_leaf)   
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


# based on some heuristic, we can route the flows one by one.
def route_flows_one_by_one(all_flows, rem, usage, num_spines, 
                           lb_decisions, run_context, max_subflow_count):
    
    strategy = run_context["routing-fit-strategy"]
    
    min_affected_time = 1e9   
    max_affected_time = 0 
    
    all_flows.sort(key=lambda x: x["eff_start_time"])
        
    for flow in all_flows:
        src_leaf = flow["srcrack"]
        dst_leaf = flow["dstrack"]
        start_time = flow["eff_start_time"] 
        end_time = flow["eff_end_time"]     
        job_id = flow["job_id"] 
        flow_id = flow["flow_id"]
        iteration = flow["iteration"]

        selected_spines = route_one_flow(flow, strategy, rem,
                                         max_subflow_count, num_spines)

        lb_decisions[(job_id, flow_id, iteration)] = selected_spines 
        
        min_affected_time = min(min_affected_time, start_time)  
        max_affected_time = max(max_affected_time, end_time)
        
        update_time_range(start_time, end_time, flow, 
                            selected_spines, rem, usage, 
                            src_leaf, dst_leaf)       
        
    return min_affected_time, max_affected_time, [] 

