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
import resource


# pd.set_option('display.max_rows', 500)
# pd.set_option('display.max_columns', 500)

# setting up the basic paths
run_id = os.popen("date +%s | sha256sum | base64 | head -c 8").read()

base_dir = os.environ.get("PSIM_BASE_DIR")
input_dir = base_dir + "/input/"
workloads_dir = input_dir + "128search-dpstart-2-limited/"
build_path = base_dir + "/build"
run_path = base_dir + "/run"
base_executable = build_path + "/psim"
run_executable = build_path + "/psim-" + run_id
shuffle_path  = input_dir + "/shuffle/shuffle-{}.txt".format(run_id)
results_dir = "results/x-{}/".format(run_id)
csv_path = results_dir + "results.csv".format(run_id)
os.system("mkdir -p {}".format(results_dir))


simulation_timestep = 10
number_worker_threads = 20
protocols_count = 2
reload_data = True
total_jobs = 0
memory_limit_kb = 10 * 1e9

# select a random subset of protocols, exclude the random ones
protocol_names_all = list(os.listdir(workloads_dir))
protocol_names = []
for protocol in protocol_names_all:
    if "random" not in protocol:
        protocol_names.append(protocol)
protocols_count = min(protocols_count, len(protocol_names))
protocol_names = np.random.choice(protocol_names, protocols_count, replace=False)
protocol_names = list(protocol_names)
protocol_names.sort()

# configs to sweep over
sweep_config = {
    "lb-scheme": [
        "futureload",
        "random",
        "roundrobin",
        "powerof2",
        "robinhood",
        "leastloaded",
    ],
    "priority-allocator": [
        # "priorityqueue",
        "fairshare",
    ],
    "load-metric": [
        "flowsize",
        "flowcount",
        "utilization",
    ],
    "protocol-file-name": protocol_names,
}


# base options
base_options = {
    "protocol-file-dir": workloads_dir,
    
    "step-size": simulation_timestep,
    "core-status-profiling-interval": simulation_timestep,
    "rep-count": 3,
    "file-log-level": 4,
    "console-log-level": 4,
    
    # flow rate control options
    "initial-rate": 100,
    "min-rate": 10,
    "priority-allocator": "fairshare",
    
    # topology options
    "network-type": "leafspine",
    "link-bandwidth": 100,
    "ft-server-per-rack": 8,
    "ft-rack-per-pod": 4,
    "ft-agg-per-pod": 4,
    "ft-core-count": 8,
    "ft-pod-count": 4,
    "ft-server-tor-link-capacity-mult": 1,
    "ft-tor-agg-link-capacity-mult": 1,
    "ft-agg-core-link-capacity-mult": 0.5,
    
    # load balancing options
    "load-metric" : "flowsize",
    "shuffle-device-map": True,
    "shuffle-map-file": shuffle_path,
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


exp_results = []
memory_limit_kb = int(memory_limit_kb)
resource.setrlimit(resource.RLIMIT_AS, (memory_limit_kb, memory_limit_kb))

def run_experiment(exp, worker_id):
    start_time = datetime.datetime.now()

    options = {
        "worker-id": worker_id,
    }
    options.update(base_options)
    options.update(exp)

    # create the command
    cmd = run_executable
    for option in options.items():
        if option[1] is False:
            continue
        elif option[1] is True:
            cmd += " --" + option[0]
        else:
            cmd += " --" + option[0] + "=" + str(option[1])

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

    except subprocess.CalledProcessError as e:
        min_psim_time = 0
        max_psim_time = 0
        last_psim_time = 0
        all_times = []
        duration = datetime.timedelta(0)

    global exp_results

    this_exp_results = {
        "min_psim_time": min_psim_time,
        "max_psim_time": max_psim_time,
        "last_psim_time": last_psim_time,
        "all_times": all_times,
        "exp_duration": duration.microseconds,
        "run_id": run_id,
    }

    for key, val in sweep_config.items():
        this_exp_results[key] = exp[key]

    exp_results.append(this_exp_results)

    pprint(this_exp_results)
    print("min time: {}, max time: {}, last time: {}".format(
        min_psim_time, max_psim_time, last_psim_time))
    print("jobs completed: {}/{}".format(len(exp_results), total_jobs))
    print("duration: {}".format(duration))
    print("worker id: {}".format(worker_id))
    print("--------------------------------------------")

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

    # build the shuffle map
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
    os.system("python plot.py {}".format(csv_path))
else:
    os.system("python plot.py {}".format(csv_path))
