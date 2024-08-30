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
    
def get_iter_lengths(output_lines, all_jobs_running=False): 
    
    iteration_times = {} 
    
    job_start_times = {}
    job_finish_times = {} 
    
    # job_iteration_times = {}
    # job_iteration_starts = {}

    for line in output_lines:
        line = line.strip()
        
        if "done with rep" in line:
            job_iteration_lengths = {}

            latest_job_start = max(job_start_times.values())
            earliest_job_finish = min(job_finish_times.values())
            
            
            for job_id in iteration_times:
                for iter_id in iteration_times[job_id]:
                    if "finish" not in iteration_times[job_id][iter_id]:
                        # this must be the last iteration that doesn't have a finish time
                        continue 
                    else: 
                        start_time = iteration_times[job_id][iter_id]["start"]
                        finish_time = iteration_times[job_id][iter_id]["finish"]
                        iteration_length = round(finish_time - start_time, 2)
                        
                        if job_id not in job_iteration_lengths:
                            job_iteration_lengths[job_id] = []
                        
                        if all_jobs_running:
                            if start_time >= latest_job_start and finish_time <= earliest_job_finish:
                                job_iteration_lengths[job_id].append(iteration_length)
                                iteration_times[job_id][iter_id]["accepted"] = "Yes"
                            else: 
                                iteration_times[job_id][iter_id]["accepted"] = "No"
                        else: 
                            job_iteration_lengths[job_id].append(iteration_length)
                            iteration_times[job_id][iter_id]["accepted"] = "X"

            yield job_iteration_lengths, iteration_times 
            
            iteration_times = {}            
            job_start_times = {}
            job_finish_times = {}
            
            continue
        
        parsed_data = parse_line_iteration(line)
            
        if parsed_data:
            sim_time, job_id, iter_id, type = parsed_data
            
            if type == "jobstart":
                job_start_times[job_id] = sim_time
                iteration_times[job_id] = {1 : {"start": sim_time}}
                                
            
            elif type == "iterfinish":
                # if iter_id == 1: 
                #     iteration_length = sim_time - job_start_times[job_id]
                #     iteration_length = round(iteration_length, 2)
                # else:
                #     iteration_length = sim_time - (sum(job_iteration_times[job_id]) + job_start_times[job_id])
                #     iteration_length = round(iteration_length, 2)
                    
                # job_iteration_starts[job_id].append(sim_time)
                # job_iteration_times[job_id].append(iteration_length)
                
                if job_id not in iteration_times: 
                    print("job_id {} not found in iteration_times".format(job_id))
                if iter_id not in iteration_times[job_id]:
                    print("iter_id {} not found in iteration_times[{}]".format(iter_id, job_id))
                
                iteration_times[job_id][iter_id]["finish"] = sim_time 
                next_iter_id = iter_id + 1 
                
                if next_iter_id not in iteration_times[job_id]:
                    # the last iteration will also create one more entry in the dictionary
                    # which has to be considered later. There will always be one entry at the end 
                    # that doesn't have a finish time.
                    iteration_times[job_id][next_iter_id] = {"start": sim_time}
                
                
                # handle the job_finish_times 
                if job_id not in job_finish_times:
                    job_finish_times[job_id] = sim_time
                else:
                    job_finish_times[job_id] = max(job_finish_times[job_id], sim_time)
                    
    print("we shouldn't get here!")
    exit(0)
    

def get_all_reduce_times(output_lines, all_jobs_running=False):
    all_reduce_times = {} 
    job_start_times = {} 
    job_finish_times = {} 
    
    current_rep = 1 
        
    for line in output_lines:
        line = line.strip()
        
        if "done with rep" in line: 
            
            all_reduce_lengths = {} 
            
            latest_job_start = max(job_start_times.values())
            earliest_job_finish = min(job_finish_times.values())


            for job_id in all_reduce_times:
                for iter_id in all_reduce_times[job_id]:
                    for layer_id in all_reduce_times[job_id][iter_id]:
                        
                        start_time = all_reduce_times[job_id][iter_id][layer_id]["start"]
                        finish_time = all_reduce_times[job_id][iter_id][layer_id]["finish"]
                        
                        duration = round(finish_time - start_time, 2)
                        
                        if job_id not in all_reduce_lengths:
                            all_reduce_lengths[job_id] = []
                        
                        if all_jobs_running:
                            if start_time >= latest_job_start and finish_time <= earliest_job_finish:
                                all_reduce_lengths[job_id].append(duration)
                                all_reduce_times[job_id][iter_id][layer_id]["accepted"] = "Yes"
                            else: 
                                all_reduce_times[job_id][iter_id][layer_id]["accepted"] = "No"
                        else: 
                            all_reduce_lengths[job_id].append(duration)
                            all_reduce_times[job_id][iter_id][layer_id]["accepted"] = "X"

            yield all_reduce_lengths, all_reduce_times
            
            all_reduce_times = {}
            job_start_times = {}
            job_finish_times = {}        

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
                    
                all_reduce_times[job_id][iter_id][layer_id]["finish"] = sim_time
                
        
        iter_parse_data = parse_line_iteration(line)    
        
        if iter_parse_data: 
            sim_time, job_id, iter_id, type = iter_parse_data
            
            if type == "jobstart":
                job_start_times[job_id] = sim_time
            
            if type == "iterfinish":
                if job_id not in job_finish_times:
                    job_finish_times[job_id] = sim_time 
                else:
                    job_finish_times[job_id] = max(job_finish_times[job_id], sim_time)


    print("we shouldn't get here!")
    exit(0)    

    
    
def get_all_rep_iter_lengths(output_lines, rep_count, all_jobs_running=False, verbose=False):
    rep = 0
    results = [] 

    for rep_result in get_iter_lengths(output_lines, all_jobs_running): 

        iteration_lengths, iteration_times = rep_result
        
        if verbose:
            print("iteration_lengths: ")        
            pprint(iteration_lengths)
            
            print("iteration_times: ")
            pprint(iteration_times)
        
        results.append(iteration_lengths)
        
        rep += 1
        if rep == rep_count:
            break
        
    return results

def get_all_rep_all_reduce_times(output_lines, rep_count, all_jobs_running=False, verbose=False):
    rep = 0
    results = [] 

    for rep_result in get_all_reduce_times(output_lines, all_jobs_running): 
        
        all_reduce_lengths, all_reduce_times = rep_result
        
        if verbose:
            print("all_reduce_lengths: ")        
            pprint(all_reduce_lengths)
            
            print("all_reduce_times: ")
            pprint(all_reduce_times)
        
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
        
    get_all_rep_iter_lengths(output_lines, 1, 
                             all_jobs_running=True, verbose=True)
    
    get_all_rep_all_reduce_times(output_lines, 1, 
                                 all_jobs_running=True, verbose=True)