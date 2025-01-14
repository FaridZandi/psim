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

simulation_timestep = 1
number_worker_threads = 40
protocols_count = 999 # all protocols

# configs to sweep over
sweep_config = {
    "lb-scheme": [
        "zero",
    ],
    "general-param-1": range(0, 50, 1),
    "general-param-2": [1],
}


# base options
base_options = {
    "protocol-file-dir": "/",
    "protocol-file-name": "periodic-test",
    
    "step-size": simulation_timestep,
    "core-status-profiling-interval": 10000, #int(max(simulation_timestep, 1)),
    "rep-count": 1,
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
    "regret-mode": "none", 
    
    "general-param-1": 20,  # job 2 initial shift
    "general-param-2": 1, # job 1 base length
    "general-param-3": 1, # job 2 base length
    "general-param-4": 50, # comm_duty_cycle both jobs 
    "general-param-5": 100, # reps multiplier over hyper period
    "general-param-6": 1, # number of teeth (layers)
}

# print the sweep config in a file in the results dir

def custom_update_func(options):
    if "general-param-7" in options and options["general-param-7"] == -1: 
        options["general-param-7"] = options["general-param-2"]
        

    print ("number of jobs that didn't converge:", non_converged_jobs)
    os.system("rm {}".format(run_executable))
    os.system("mv {} {}".format(shuffle_path, results_dir))

    all_pd_frame = pd.DataFrame(exp_results)
    all_pd_frame.to_csv(csv_path)
    # os.system("python plot.py {}".format(csv_path))
