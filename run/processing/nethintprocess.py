import re
import sys 
import matplotlib.pyplot as plt
import numpy as np


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

def calculate_iteration_lengths(file_path):
    job_start_times = {}
    job_iteration_times = {}
    job_iteration_starts = {}

    with open(file_path, 'r') as file:
        for line in file:
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
    
    
    
    drifts = []
    
    if len(job_iteration_times) == 2: 
        drifts.append(job_start_times[1] - job_start_times[2])
        for iter_id in range(1, min(len(job_iteration_times[1]), len(job_iteration_times[2]))):
            drift = job_iteration_starts[1][iter_id] - job_iteration_starts[2][iter_id]
            drifts.append(drift)
        
    return job_iteration_times, job_iteration_starts, job_start_times, drifts

def main(file_path):
    iteration_lengths, iteration_starts, job_start_times, drifts = calculate_iteration_lengths(file_path)
    
    all_iteration_lengths = [] 
    
    for job_id, iterations in iteration_lengths.items():
        all_iteration_lengths.extend(iterations)
        print(f"Job {job_id}:")
        for iter_id, length in enumerate(iterations, start=1):
            print(f"  Iteration {iter_id}: {length} ms")
    
    
    # plot the CDF of the iteration lengths
    all_iteration_lengths.sort()
    y = np.arange(len(all_iteration_lengths)) / len(all_iteration_lengths)
    plt.plot(all_iteration_lengths, y)
    plt.xlabel("Iteration length (ms)")
    plt.ylabel("CDF")
    plt.savefig("plots/iteration-lengths-cdf.png")
    plt.clf()
    

def find_convergence(arr, repeat_tolerance = 10):
    if not arr or repeat_tolerance < 1:
        return None, None  # Invalid input case

    last_value = arr[0]
    count = 1
    
    for i in range(1, len(arr)):
        if arr[i] == last_value:
            count += 1
            if count == repeat_tolerance:
                return i - repeat_tolerance + 1, last_value
        else:
            last_value = arr[i]
            count = 1
            
    return None, None  # No convergence found

def get_convergence_info(file_path, repeat_tolerance=10): 
    iteration_lengths, iteration_starts, job_start_times, drifts = calculate_iteration_lengths(file_path)
    
    # Find the convergence point
    convergence_point_1, convergence_value_1 = find_convergence(iteration_lengths[1], repeat_tolerance)
    convergence_point_2, convergence_value_2 = find_convergence(iteration_lengths[2], repeat_tolerance)
    drifts_convergence_point, drifts_convergence_value = find_convergence(drifts, repeat_tolerance)

    return (convergence_point_1, convergence_value_1, 
            convergence_point_2, convergence_value_2, 
            drifts_convergence_point, drifts_convergence_value)


def get_first_iter_info(file_path): 
    iteration_lengths, _, _, _ = calculate_iteration_lengths(file_path)
    
    return iteration_lengths[1][0], iteration_lengths[2][0]
    
    
if __name__ == "__main__":
    if len(sys.argv) < 2:
        path = "workers/worker-0/run-1/runtime.txt"
        print("using the default path: ", path)    
    else :
        path = sys.argv[1]
    
    main(file_path=path)