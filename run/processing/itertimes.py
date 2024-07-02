import re
import sys 
import matplotlib.pyplot as plt

def parse_line(line):
    """Parse a line to extract simulation time, job ID, and iteration ID."""
    # pattern like below, but instead of the first int, it should accept a floating point number 
    # so the ([0-9]+)\ part should be changed
    # pattern = r'\[([0-9]+)\]: job (\d+) iter (\d+) finished'
    print(line)    
    pattern = r'\[([+-]?(?:\d+(\.\d*)?|\.\d+))\]: job (\d+) iter (\d+) finished'
    
    match = re.search(pattern, line)
    if match:
        time = float((match.group(1)))            
        return time, int(match.group(3)), int(match.group(4)), "iterfinish"
    else: 
        pattern = r'\[([+-]?(?:\d+(\.\d*)?|\.\d+))\]: job (\d+) started'
        match = re.search(pattern, line)
        if match:
            time = float((match.group(1)))            
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
                print (sim_time, job_id, iter_id, type)
                
                if type == "jobstart":
                    job_start_times[job_id] = sim_time
                    job_iteration_times[job_id] = []
                    job_iteration_starts[job_id] = [sim_time]
                
                elif type == "iterfinish":
                    if iter_id == 1: 
                        iteration_length = sim_time - job_start_times[job_id]
                    else:
                        iteration_length = sim_time - (sum(job_iteration_times[job_id]) + job_start_times[job_id])

                    job_iteration_starts[job_id].append(sim_time)
                    job_iteration_times[job_id].append(iteration_length)
                    print(job_id, job_iteration_times[job_id])
    
    return job_iteration_times, job_iteration_starts

def main():
    file_path = sys.argv[1]  # Update this path to your file containing the log data
    iteration_lengths, iteration_starts = calculate_iteration_lengths(file_path)
    
    for job_id, iterations in iteration_lengths.items():
        print(f"Job {job_id}:")
        for iter_id, length in enumerate(iterations, start=1):
            print(f"  Iteration {iter_id}: {length} ms")
    
    # calculate the drift between the same iterations of the different jobs
    if len(iteration_lengths) != 2:
        print("The drift can only be calculated between two jobs.")
        return

    drifts = []
    for iter_id in range(1, min(len(iteration_lengths[1]), len(iteration_lengths[2]))):
        drift = iteration_starts[1][iter_id] - iteration_starts[2][iter_id]
        drifts.append(drift)
        print(f"Drift between iteration {iter_id}: {drift} ms")
    
    # Plot the iteration lengths
    for job_id, iterations in iteration_lengths.items():
        plt.plot(range(1, len(iterations) + 1), iterations, label=f"Job {job_id}")
    
    plt.plot(range(1, len(drifts) + 1), drifts, label="Drift")
        
    plt.legend() 
    plt.savefig("plots/iteration_lengths.png")
    
if __name__ == "__main__":
    main()