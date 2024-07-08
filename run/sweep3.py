import os
from pprint import pprint
import itertools
import subprocess
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import queue
import threading
import sys
import datetime
from utils.util import *
import resource
from processing.itertimes import get_convergence_info, get_first_iter_info

# pd.set_option('display.max_rows', 500)
# pd.set_option('display.max_columns', 500)

# setting up the basic paths
# run_id = os.popen("date +%s | sha256sum | base64 | head -c 4").read()
run_id = str(get_incremented_number())

base_dir = get_base_dir() 
input_dir = base_dir + "/input/"

workloads_dir = input_dir
if (len(sys.argv) == 1 or sys.argv[1].lower() == "full"):
    workloads_dir += "128search-dpstart-2/"
elif (sys.argv[1].lower() == "trans"):
    workloads_dir += "128search-dpstart-2-trans/"
elif (sys.argv[1].lower() == "ncf"):
    workloads_dir += "128search-dpstart-2-ncf/"
elif (sys.argv[1].lower() == "random"):
    workloads_dir += "128search-dpstart-2-random/"
elif (sys.argv[1].lower() == "limited"):
    workloads_dir += "128search-dpstart-2-limited/"
# workloads_dir = input_dir + "random/"

build_path = base_dir + "/build"
run_path = base_dir + "/run"
base_executable = build_path + "/psim"
run_executable = build_path + "/psim-" + run_id
shuffle_path  = input_dir + "/shuffle/shuffle-{}.txt".format(run_id)
results_dir = "results/{}-run/".format(run_id)
csv_path = results_dir + "results.csv".format(run_id)
workers_dir = run_path + "/workers/"

os.system("mkdir -p {}".format(results_dir))


simulation_timestep = 1
number_worker_threads = 40
rep_count = 1
protocols_count = 999 # all protocols

# select a random subset of protocols, exclude the random ones
protocol_names = list(os.listdir(workloads_dir))
protocols_count = min(protocols_count, len(protocol_names))
protocol_names = np.random.choice(protocol_names, protocols_count, replace=False)
protocol_names = list(protocol_names)
protocol_names.sort()

# configs to sweep over
sweep_config = {
    "lb-scheme": [
    #     "futureload",
        "zero",
        # "roundrobin",
    #     "powerof2",
    #     "powerof3",
    #     "powerof4",
        # "robinhood",
    #     "sita-e",
        # "leastloaded",
    ],
    # "priority-allocator": [
    #     "priorityqueue",
    #     "fairshare",
    # ],
    # "load-metric": [
    #     "flowsize",
    #     "flowcount",
    #     "utilization",
    # ],
    # "ft-core-count": [2, 4, 8],
    # "ft-agg-core-link-capacity-mult": [2, 4, 8],
    "protocol-file-name": ["periodic-test-simple"],
    # "general-param-1": range(0, 500, 25),
    "general-param-1": range(0, 800, 1),
    "general-param-2": range(10, 51, 10), 
}

load_metric_map = {
    "futureload": "utilization",
    "leastloaded": "flowsize",
    "powerof2": "flowsize",
    "powerof3": "flowsize",
    "powerof4": "flowsize",
    "random": "flowcount",
    "robinhood": "utilization",
    "roundrobin": "flowsize",
    "sita-e": "utilization",
    "ecmp": "flowsize",
    "zero": "flowsize", 
}

# base options
base_options = {
    "protocol-file-dir": workloads_dir,
    
    "step-size": simulation_timestep,
    "core-status-profiling-interval": 10000, #int(max(simulation_timestep, 1)),
    "rep-count": rep_count,
    "file-log-level": 4,
    "console-log-level": 4,

    # flow rate control options
    "initial-rate": 400,
    "min-rate": 400,
    ###########################
    # "min-rate": 10,
    # "drop-chance-multiplier": 0, 
    # "rate-increase": 2,
    ###########################
    "priority-allocator": "fairshare",
    # "priority-allocator": "priorityqueue", 

    # topology options
    "network-type": "leafspine",    
    "link-bandwidth": 400,
    "ft-server-per-rack": 4,
    "ft-rack-per-pod": 1,
    "ft-agg-per-pod": 4,
    "ft-core-count": 2,
    "ft-pod-count": 4,
    "ft-server-tor-link-capacity-mult": 1,
    "ft-tor-agg-link-capacity-mult": 1,
    "ft-agg-core-link-capacity-mult": 1,
    

    # load balancing options
    "lb-scheme": "random", 
    "load-metric" : "notset", # is set later based on lb-scheme
    "shuffle-device-map": False,
    "shuffle-map-file": shuffle_path,
    "regret-mode": "none", 
    
    "general-param-1": 100,  # job 2 initial shift
    "general-param-2": 80, # comm duty cycle
    "general-param-3": 2, # number of reps 
    "general-param-4": 4, # number of teeth
    "general-param-5": 0, # dependency between the flows
    "general-param-6": 400, # wait in between
}

# print the sweep config in a file in the results dir
with open(results_dir + "sweep-config.txt", "w") as f:
    pprint("------------------------------------------", stream=f)
    pprint("sweep_config", stream=f)
    pprint(sweep_config, stream=f)
    pprint("------------------------------------------", stream=f)
    pprint("base_options", stream=f)
    pprint(base_options, stream=f)
    pprint("------------------------------------------", stream=f)
    pprint("globals", stream=f)
    pprint(globals(), stream=f)

total_jobs = 0
non_converged_jobs = 0 
exp_results = []
threads = []
exp_q = queue.Queue()
print_lock = threading.Lock()    

build_exec(run_executable, base_executable, build_path, run_path)
make_shuffle(128, shuffle_path)
set_memory_limit(10 * 1e9)

def run_experiment(exp, worker_id):
    start_time = datetime.datetime.now()

    options = {
        "worker-id": worker_id,
    }
    options.update(base_options)
    options.update(exp)
    options["load-metric"] = load_metric_map[options["lb-scheme"]]

    # create the command
    cmd = make_cmd(run_executable, options, use_gdb=False, print_cmd=False)

    try:
        output = subprocess.check_output(cmd, shell=True)
        output = output.decode("utf-8")

        end_time = datetime.datetime.now()
        duration = end_time - start_time

        last_psim_time = 0
        min_psim_time = 1e12
        max_psim_time = 0
        all_times = []
        for line in output.splitlines():
            if "psim time" in line:
                psim_time = float(line.strip().split(" ")[-1])
                all_times.append(psim_time)
                if psim_time > max_psim_time:
                    max_psim_time = psim_time
                if psim_time < min_psim_time:
                    min_psim_time = psim_time
                last_psim_time = psim_time
                
        # assuming just one rep for now 
        runtime_file_path = workers_dir + "worker-{}/run-1/runtime.txt".format(worker_id)
        convergence_info = get_convergence_info(runtime_file_path)
        
        j1_conv_point = convergence_info[0] 
        j1_conv_value = convergence_info[1]
        j2_conv_point = convergence_info[2]
        j2_conv_value = convergence_info[3]
        drifts_conv_point = convergence_info[4]
        drifts_conv_value = convergence_info[5]
        
        j1_iter1, j2_iter1 = get_first_iter_info(runtime_file_path)
        
        if (j1_conv_point is None or j2_conv_point is None or 
            j1_conv_value is None or j2_conv_value is None or 
            drifts_conv_point is None or drifts_conv_value is None):
            
            print("Convergence not found for")
            pprint(options) 
            
            global non_converged_jobs
            non_converged_jobs += 1 
        
        
    except subprocess.CalledProcessError as e:
        min_psim_time = 0
        max_psim_time = 0
        last_psim_time = 0
        all_times = []
        duration = datetime.timedelta(0)

    this_exp_results = {
        "min_psim_time": min_psim_time,
        "max_psim_time": max_psim_time,
        "last_psim_time": last_psim_time,
        "avg_psim_time": np.mean(all_times),
        "all_times": all_times,
        "exp_duration": duration.microseconds,
        "run_id": run_id,
        "j1_conv_point": j1_conv_point,
        "j2_conv_point": j2_conv_point,
        "j1_conv_value": j1_conv_value,
        "j2_conv_value": j2_conv_value, 
        "drifts_conv_point": drifts_conv_point,
        "drifts_conv_value": drifts_conv_value,
        "job_1_iter_1": j1_iter1, 
        "job_2_iter_1": j2_iter1,
    }

    for key, val in sweep_config.items():
        this_exp_results[key] = exp[key]

    global exp_results
    exp_results.append(this_exp_results)

    global print_lock
    with print_lock:
        pprint(this_exp_results)
        print("min time: {}, max time: {}, last time: {}".format(
            min_psim_time, max_psim_time, last_psim_time))
        print("jobs completed: {}/{}".format(len(exp_results), total_jobs))
        print("duration: {}".format(duration))
        print("worker id: {}".format(worker_id))
        print("--------------------------------------------")

worker_num_counter = 0
def worker():
    global worker_num_counter
    worker_id = worker_num_counter
    worker_num_counter += 1

    while True:
        try:
            exp = exp_q.get(block = 0)
        except queue.Empty:
            return
        run_experiment(exp, worker_id)


def run_all_configs():
    keys, values = zip(*sweep_config.items())
    permutations_dicts = [dict(zip(keys, v)) for v in itertools.product(*values)]

    for exp in permutations_dicts:
        exp_q.put(exp)

    global total_jobs
    total_jobs = exp_q.qsize()

    for i in range(number_worker_threads):
        t = threading.Thread(target=worker, args=())
        threads.append(t)
        t.start()

    for t in threads:
        t.join()


if __name__ == "__main__": 
    run_all_configs()

    print ("number of jobs that didn't converge:", non_converged_jobs)
    os.system("rm {}".format(run_executable))
    os.system("mv {} {}".format(shuffle_path, results_dir))

    all_pd_frame = pd.DataFrame(exp_results)
    all_pd_frame.to_csv(csv_path)
    # os.system("python plot.py {}".format(csv_path))
