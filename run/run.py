import os
from pprint import pprint 
import itertools
import subprocess
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import sys 
import signal
import matplotlib
import resource
from utils.util import *

run_id = os.popen("date +%s | sha256sum | base64 | head -c 8").read()

# setting up the basic paths
# read the env for the base dir 
base_dir = get_base_dir()
build_path = base_dir + "/build"
run_path = base_dir + "/run/"
base_executable = build_path + "/psim"
executable = build_path + "/psim-" + run_id
input_dir = base_dir + "/input/"
shuffle_path  = input_dir + "/shuffle/shuffle-{}.txt".format(run_id)

options = {
    "protocol-file-dir": base_dir + "/input/128search-dpstart-2",
    "protocol-file-name": "candle128-simtime.txt",
    # "protocol-file-name": "candle128-simtime.txt,candle128-comm.txt",
    # "protocol-file-name": "build-ring",
    # "protocol-file-name": "build-all-to-all",
    # "protocol-file-name": "dlrm128-simtime.txt",
    # "protocol-file-name": "transformer128-simtime+compute.txt",

    "step-size": 1,
    "core-status-profiling-interval": 1,
    "rep-count": 1, 
    "console-log-level": 4,
    "file-log-level": 3,
    
    "initial-rate": 400,
    "min-rate": 400,
    "priority-allocator": "fairshare", #"priorityqueue", 

    "network-type": "leafspine",    
    "link-bandwidth": 400,
    "ft-server-per-rack": 8,
    "ft-rack-per-pod": 4,
    "ft-agg-per-pod": 4,
    "ft-core-count": 4,
    "ft-pod-count": 4,
    "ft-server-tor-link-capacity-mult": 1,
    "ft-tor-agg-link-capacity-mult": 1,
    "ft-agg-core-link-capacity-mult": 1,
    
    
    "lb-scheme": "readfile",
    "lb-decisions-file": run_path + "ga/214/rounds/1000/0.txt",
    "load-metric": "utilization",
    "shuffle-device-map": False,
    "shuffle-map-file": shuffle_path,
    "regret-mode": "none",
}

set_memory_limit(10 * 1e9)
build_exec(executable, base_executable, build_path, run_path)
make_shuffle(128, shuffle_path)

params, use_gdb = parse_arguments(sys.argv)
options.update(params)
cmd = make_cmd(executable, options, use_gdb=use_gdb, print_cmd=True)


# run the simulation
p = subprocess.Popen(cmd, 
                     stdout=subprocess.PIPE, 
                     stderr=subprocess.STDOUT, 
                     shell=True, 
                     preexec_fn=os.setsid)
psim_times = get_psim_time(p.stdout, True)["all"]  


# plot the results
plt.plot(psim_times)
plt.xticks(np.arange(0, len(psim_times), 1.0))
plt.savefig("plots/psim-times.png")


# clean up the garbage 
os.system("rm {}".format(executable))
os.system("rm {}".format(shuffle_path))