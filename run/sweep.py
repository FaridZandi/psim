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
workloads_dir = input_dir + "128search-dpstart-2/"
build_path = base_dir + "/build"
run_path = base_dir + "/run"
base_executable = build_path + "/psim"
run_executable = build_path + "/psim-" + run_id
shuffle_path  = input_dir + "/shuffle/shuffle-{}.txt".format(run_id)
results_dir = "results/x-{}/".format(run_id)
csv_path = results_dir + "results.csv".format(run_id)
os.system("mkdir -p {}".format(results_dir))


simulation_timestep = 100
number_worker_threads = 20
protocols_count = 36
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
    "core-selection-mechanism": [
        "futureload-utilization",
        # "futureload-allocated",
        "random",
        "roundrobin",
        "powerof2",
        "robinhood",
        "leastloaded",
        # "futureload-register",
    ],
    "priority-allocator": [
        # "priorityqueue",
        "fairshare",
    ],
    "protocol-file-name": protocol_names,
}


# base options
base_options = {
    "protocol-file-dir": workloads_dir,
    "step-size": simulation_timestep,
    "core-status-profiling-interval": simulation_timestep,
    "link-bandwidth": 100,
    "initial-rate": 100,
    "min-rate": 10,
    "ft-agg-per-pod": 4,
    "console-log-level": 4,
    "file-log-level": 4,
    "ft-server-tor-link-capacity-mult": 1,
    "ft-tor-agg-link-capacity-mult": 1,
    "priority-allocator": "fairshare",
    "shuffle-device-map": True,
    "load-metric" : "utilization",
    "shuffle-map-file": shuffle_path,
    "rep-count": 5,

    "network-type": "leafspine",
    "ft-server-per-rack": 32,
    "ft-agg-core-link-capacity-mult": 0.5,
    "ft-core-count": 8,
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

    # set the memory limit


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
