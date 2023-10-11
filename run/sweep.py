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
from util import make_shuffle

# pd.set_option('display.max_rows', 500)
# pd.set_option('display.max_columns', 500)

# setting up the basic paths
run_id = os.popen("date +%s | sha256sum | base64 | head -c 8").read()

base_dir = "/home/faridzandi/git/psim"
input_dir = base_dir + "/input/" 
workloads_dir = input_dir + "128search-dpstart-2/"
build_path = base_dir + "/build"
run_path = base_dir + "/run"
base_executable = build_path + "/psim"
run_executable = build_path + "/psim-" + run_id
shuffle_path  = input_dir + "/shuffle/shuffle-{}.txt".format(run_id)
results_dir = "results/x-{}/".format(run_id)
csv_path = results_dir + "results.csv".format(run_id)
os.system("mkdir -p {}".format(results_dir))

number_worker_threads = 40
protocols_count = 10 
reload_data = True
exp_results = [] 
total_jobs = 0 

# select a random subset of protocols, exclude the random ones
protocol_names_all = list(os.listdir(workloads_dir))
protocol_names = [] 
for protocol in protocol_names_all:
    if "random" not in protocol: 
        protocol_names.append(protocol)
protocol_names = np.random.choice(protocol_names, protocols_count, replace=False)
protocol_names = list(protocol_names)
protocol_names.sort()

# configs to sweep over
sweep_config = {	
    "core-selection-mechanism": [
        "random", 
        "roundrobin", 
        "leastloaded", 
        "futureload-utilization",
        "futureload-allocated",
        # "futureload-register",
    ],
    "priority-allocator": [
        "priorityqueue", 
        "fairshare",
    ],
    "protocol-file-name": protocol_names,
}


# base options
base_options = {
    "protocol-file-dir": workloads_dir,
    "step-size": 10,
    "link-bandwidth": 100,
    "initial-rate": 100,
    "min-rate": 10,
    "ft-core-count": 4,
    "ft-agg-per-pod": 4,
    "console-log-level": 4,
    "file-log-level": 4,
    "ft-server-tor-link-capacity-mult": 1,
    "ft-tor-agg-link-capacity-mult": 1,
    "ft-agg-core-link-capacity-mult": 1,
    "priority-allocator": "fairshare",
    "shuffle-device-map": True,
    "core-status-profiling-interval": 10,
    "load-metric" : "utilization",
    "shuffle-map-file": shuffle_path,
    "rep-count": 10,
}

def run_experiment(exp, worker_id):
    
    start_time = datetime.datetime.now()
    
    options = {
        "worker-id": worker_id,
    }
    options.update(base_options)
    options.update(exp)
    if options["core-selection-mechanism"].startswith("futureload"):
        load_metric = options["core-selection-mechanism"].split("-")[1]
        options["load-metric"] = load_metric
        options["core-selection-mechanism"] = "futureload"
        
    # create the command
    cmd = run_executable
    for option in options.items():
        if option[1] is False:
            continue
        elif option[1] is True:
            cmd += " --" + option[0]
        else: 
            cmd += " --" + option[0] + "=" + str(option[1])
    
    # print("running the command:", cmd)            
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
    
    global exp_results
    
    this_exp_results = {
        "min_psim_time": min_psim_time,
        "max_psim_time": max_psim_time,
        "last_psim_time": last_psim_time,
        "all_times": all_times,
        "exp_duration": duration.microseconds,
    }
    
    for key, val in sweep_config.items():
        this_exp_results[key] = exp[key]

    exp_results.append(this_exp_results)

    pprint(options)            
    print("min time: {}, max time: {}, last time: {}".format(
        min_psim_time, max_psim_time, last_psim_time))
    print("jobs completed: {}/{}".format(len(exp_results), total_jobs))
    
worker_num = 0 

def worker(exp): 
    global worker_num
    worker_id = worker_num
    worker_num += 1
        
    while True:
        try:
            exp = exp_q.get(block = 0)
        except queue.Empty:
            return
        
        run_experiment(exp, worker_id)

    
pd_frame = None
if not os.path.exists(csv_path):
    reload_data = True    

if reload_data:
    exp_q = queue.Queue()
    threads = []

    
    
    # build the executable, exit if build fails
    os.chdir(build_path)
    exit_code = os.system("make -j")
    if exit_code != 0:
        print("make failed, exiting")
        sys.exit(1)
    os.chdir(run_path)
    os.system("cp {} {}".format(base_executable, run_executable))
    
    make_shuffle(128, shuffle_path)
    
    keys, values = zip(*sweep_config.items())
    permutations_dicts = [dict(zip(keys, v)) for v in itertools.product(*values)]

    for exp in permutations_dicts:  
        exp_q.put(exp)

    total_jobs = exp_q.qsize()
    
    for i in range(number_worker_threads):
        t = threading.Thread(target=worker, args=(exp_q,))
        threads.append(t)
        t.start()
        
    for t in threads:
        t.join()
    
    os.system("rm {}".format(run_executable))
    os.system("mv {} {}".format(shuffle_path, results_dir))

    all_pd_frame = pd.DataFrame(exp_results)
    all_pd_frame.to_csv(csv_path) 
else:
    all_pd_frame = pd.read_csv(csv_path)


all_pd_frame = all_pd_frame.sort_values(by=["protocol-file-name", "core-selection-mechanism"])


colors = {
    "random": "red",
    "roundrobin": "orange",
    "leastloaded": "yellow",
    "futureload-register": "purple",
    "futureload-allocated": "blue",
    "futureload-utilization": "darkgreen",
}

def get_color(mech): 
    if mech in colors:
        return colors[mech]
    else:
        return None
    
all_pd_frame.reindex()     
protocols = all_pd_frame["protocol-file-name"].unique()
core_selection_mechanisms = all_pd_frame["core-selection-mechanism"].unique()
priority_allocators = all_pd_frame["priority-allocator"].unique()

for allocator in priority_allocators:
    pd_frame = all_pd_frame[all_pd_frame["priority-allocator"] == allocator]
    print(pd_frame)

    # for each protocol, normalize the times, the max max_time is 1, and everything else is relative to that
    pd_frame["rel_max_psim_time"] = 0
    pd_frame["rel_min_psim_time"] = 0
    pd_frame["rel_last_psim_time"] = 0

    for protocol in protocols:
        protocol_data = pd_frame[pd_frame["protocol-file-name"] == protocol]
        max_max_time = protocol_data["max_psim_time"].max()
        
        for index, row in protocol_data.iterrows():
            pd_frame.loc[index, "rel_max_psim_time"] = row["max_psim_time"] / max_max_time
            pd_frame.loc[index, "rel_min_psim_time"] = row["min_psim_time"] / max_max_time
            pd_frame.loc[index, "rel_last_psim_time"] = row["last_psim_time"] / max_max_time
            
            if row["rel_last_psim_time"] > 1:
                print("error: rel_last_psim_time > 1")
                print(row) 

    print(pd_frame)


    print("protocols:", protocols)
    print("core selection mechanisms:", core_selection_mechanisms)

    bar_width = 0.2
    group_width = bar_width * len(core_selection_mechanisms)
    group_spacing = 1

    # len(protocols) items, with (group_width + group_spacing) space between each two items
    x = np.arange(len(protocols)) * (group_width + group_spacing)

    plt.figure(figsize=(len(protocols) * 2, 10))

    for i, mech in enumerate(core_selection_mechanisms):
        # get the data for this mech
        mech_data = pd_frame[pd_frame["core-selection-mechanism"] == mech]
        print(mech)
        print(mech_data)
        
        x_offset = x + (i * bar_width - group_width / 2)
        
        # increase the density of the hatches
        plt.bar(x_offset, mech_data["rel_max_psim_time"],  
                width=bar_width, color="white",
                edgecolor="black", hatch="///", linewidth=2)
        
        plt.bar(x_offset, mech_data["rel_max_psim_time"],  
                width=bar_width, color=get_color(mech), alpha=0.5,
                edgecolor="black", hatch="\\\\\\", linewidth=2)
        
        plt.bar(x_offset, mech_data["rel_min_psim_time"], 
                width=bar_width, label=mech, 
                color=get_color(mech), edgecolor="black", linewidth=2)
        
        plt.plot(x_offset, mech_data["rel_last_psim_time"], 
                marker="o", color="white", markersize=4, markeredgecolor="black", linestyle="None")
            
    
    plt.xticks(x, protocols)
    plt.xticks(rotation=90)
    plt.legend()
    plt.ylabel("Normalized Psim Time")
    
    plot_name = results_dir + "{}.png".format(allocator)
    plt.savefig(plot_name, bbox_inches="tight", dpi=300)