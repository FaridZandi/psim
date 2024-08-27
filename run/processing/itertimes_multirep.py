import re
import sys 
import matplotlib.pyplot as plt
from pprint import pprint 

def parse_line_iteration(line):
    """Parse a line to extract simulation time, job ID, and iteration ID."""
    # pattern like below, but instead of the first int, it should accept a floating point number 
    # so the ([0-9]+)\ part should be changed
    # pattern = r'\[([0-9]+)\]: job (\d+) iter (\d+) finished'
    pattern = r'\[([+-]?(?:\d+(\.\d*)?|\.\d+))\]: job (\d+) iter (\d+) finished'
    
    match = re.search(pattern, line)
    if match:
        time = float((match.group(1)))    
        time = round(time, 2)
        return time, int(match.group(3)), int(match.group(4)), "iterfinish"
    else: 
        pattern = r'\[([+-]?(?:\d+(\.\d*)?|\.\d+))\]: job (\d+) started'
        match = re.search(pattern, line)
        if match:
            time = float((match.group(1)))
            time = round(time, 2)           
            return time, int(match.group(3)), 0, "jobstart"
        else:
            return None
        
def parse_line_all_reduce(line):
    start_pattern = r'\[([+-]?(?:\d+(\.\d*)?|\.\d+))\]: job (\d+) iter (\d+) layer (\d+) all-reduce started'
    finish_pattern = r'\[([+-]?(?:\d+(\.\d*)?|\.\d+))\]: job (\d+) iter (\d+) layer (\d+) all-reduce finished'
    
    match = re.search(start_pattern, line)
    if match:
        time = float((match.group(1)))    
        time = round(time, 2)
        job_id = int(match.group(3))
        iter_id = int(match.group(4))
        layer_id = int(match.group(5))
        return time, job_id, iter_id, layer_id, "allreducestart"
    
    else:
        match = re.search(finish_pattern, line)
        if match:
            time = float((match.group(1)))    
            time = round(time, 2)
            job_id = int(match.group(3))
            iter_id = int(match.group(4))
            layer_id = int(match.group(5))
            return time, job_id, iter_id, layer_id, "allreducefinish"
        else:
            return None
    
def get_iter_lengths(output_lines):
    
    job_start_times = {}
    job_iteration_times = {}
    job_iteration_starts = {}

    for line in output_lines:
        line = line.strip()
        
        if "done with rep" in line: 
            yield job_iteration_times, job_iteration_starts, job_start_times
            job_start_times = {}
            job_iteration_times = {}
            job_iteration_starts = {}
            
            continue
        
        parsed_data = parse_line_iteration(line)
            
        if parsed_data:
            sim_time, job_id, iter_id, type = parsed_data
            
            if type == "jobstart":
                job_start_times[job_id] = sim_time
                job_iteration_times[job_id] = []
                job_iteration_starts[job_id] = [sim_time]
            
            elif type == "iterfinish":
                if iter_id == 1: 
                    iteration_length = sim_time - job_start_times[job_id]
                    iteration_length = round(iteration_length, 2)
                else:
                    iteration_length = sim_time - (sum(job_iteration_times[job_id]) + job_start_times[job_id])
                    iteration_length = round(iteration_length, 2)
                    
                job_iteration_starts[job_id].append(sim_time)
                job_iteration_times[job_id].append(iteration_length)
    
    
    print("we shouldn't get here!")
    yield job_iteration_times, job_iteration_starts, job_start_times

def get_all_reduce_times(output_lines):
    
    all_reduce_times = {} 
    all_reduce_lengths = {} 

    current_rep = 1 
        
    for line in output_lines:
        line = line.strip()
        
        if "done with rep" in line: 
            yield all_reduce_lengths, all_reduce_times
            
            all_reduce_lengths = {}
            all_reduce_times = {}
                    
            current_rep += 1    
            
            continue
        
        parsed_data = parse_line_all_reduce(line)
        
        if parsed_data:
            sim_time, job_id, iter_id, layer_id, type = parsed_data
            
            if type == "allreducestart":
                if job_id not in all_reduce_times:
                    all_reduce_times[job_id] = {}
                    
                if iter_id not in all_reduce_times[job_id]:
                    all_reduce_times[job_id][iter_id] = {}
                
                if layer_id not in all_reduce_times[job_id][iter_id]:
                    all_reduce_times[job_id][iter_id][layer_id] = {}
                
                all_reduce_times[job_id][iter_id][layer_id]["start"] = sim_time
                                
            elif type == "allreducefinish":
                if job_id not in all_reduce_times:
                    print("job_id {} not found in all_reduce_times for rep {}".format(job_id, current_rep))
                if iter_id not in all_reduce_times[job_id]:
                    print("iter_id {} not found in all_reduce_times[] for rep {}".format(iter_id, job_id, current_rep))
                if layer_id not in all_reduce_times[job_id][iter_id]:
                    print("layer_id {} not found in all_reduce_times[{}][{}] for rep {}".format(layer_id, job_id, iter_id, current_rep))
                if "start" not in all_reduce_times[job_id][iter_id][layer_id]:
                    print("start not found in all_reduce_times[{}][{}][{}] for rep {}".format(job_id, iter_id, layer_id, current_rep))                   
                    
                start_time = all_reduce_times[job_id][iter_id][layer_id]["start"]
                all_reduce_times[job_id][iter_id][layer_id]["finish"] = sim_time
                
                duration = round(sim_time - start_time, 2)

                if job_id not in all_reduce_lengths:
                    all_reduce_lengths[job_id] = []
                    
                all_reduce_lengths[job_id].append(duration)
                 

    print("we shouldn't get here!")
    yield all_reduce_times
    
    
def get_all_rep_iter_lengths(output_lines, rep_count):
    # TODO: I should somehow only count the iterations that are happening when all the jobs are running. 
    # I could have 5 jobs, but after the first one finishes, the other 4 will be running. the numbers 
    # will not repreresent the actual iteration lengths. However, the other counting methods also has 
    # its own merits. 
    rep = 0
    results = [] 

    for rep_result in get_iter_lengths(output_lines): 
        iteration_lengths, iteration_starts, job_start_times = rep_result
        results.append(iteration_lengths)
        
        rep += 1
        if rep == rep_count:
            break
    
    return results

def get_all_rep_all_reduce_times(output_lines, rep_count):
    rep = 0
    results = [] 

    for rep_result in get_all_reduce_times(output_lines): 
        all_reduce_lengths, all_reduce_times = rep_result
        results.append(all_reduce_lengths)
        
        rep += 1
        if rep == rep_count:
            break
    
    return results

if __name__ == "__main__":
    if len(sys.argv) < 2:
        path = "workers/worker-0/run-1/runtime.txt"
        print("using the default path: ", path)    
    else :
        path = sys.argv[1]
    
    with open(path, "r") as f:
        output_lines = f.readlines()
        
    iter_lengths = get_all_rep_iter_lengths(output_lines, 1)
    all_reduce_lengths = get_all_rep_all_reduce_times(output_lines, 1)
    
    pprint(iter_lengths)
    pprint(all_reduce_lengths)