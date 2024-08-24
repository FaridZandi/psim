import re
import sys 
import matplotlib.pyplot as plt

def parse_line(line):
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
        
        parsed_data = parse_line(line)
            
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
    
    
    print("we shouldn't get to here!")
    yield job_iteration_times, job_iteration_starts, job_start_times


def get_all_rep_iter_lengths(output_lines, rep_count):
    
    # TODO: I should somehow only count the iterations that are happening when all the jobs are running. 
    # I could have 5 jobs, but after the first one finishes, the other 4 will be running. the numbers 
    # will not repreresent the actual iteration lengths. However, the other counting methods also has 
    # its own merits. 
    
    rep = 0
    
    results = [] 
    
    for rep_result in get_iter_lengths(output_lines): 
        rep += 1
        
        iteration_lengths, iteration_starts, job_start_times = rep_result
    
        results.append(iteration_lengths)
                
        if rep == rep_count:
            break
    
    return results

# if __name__ == "__main__":
#     if len(sys.argv) < 2:
#         path = "workers/worker-0/run-1/runtime.txt"
#         print("using the default path: ", path)    
#     else :
#         path = sys.argv[1]
    
#     main(file_path=path)