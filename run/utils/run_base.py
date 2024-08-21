#!/usr/bin/env python3

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

def run_exp(options, sysargv):
    run_id = str(get_incremented_number())

    # setting up the basic paths
    # read the env for the base dir 
    base_dir = get_base_dir()
    build_path = base_dir + "/build"
    run_dir = base_dir + "/run"
    base_executable = build_path + "/psim"
    executable = build_path + "/psim-" + run_id
    input_dir = base_dir + "/input/"
    output_path = run_dir + "/workers/worker-0/run-1/"
    results_dir = run_dir + "/results/run/{}-run/".format(run_id)
    shuffle_path  = results_dir + "/shuffle-{}.txt".format(run_id)

    set_memory_limit(10 * 1e9)
    build_exec(executable, base_executable, build_path, run_dir)
    make_shuffle(options["machine-count"], shuffle_path)

    params, use_gdb = parse_arguments(sysargv)
    options.update(params)
    options.update({
        "worker-id": 0,
        "shuffle-map-file": shuffle_path,
    })
    
    cmd = make_cmd(executable, options, use_gdb=use_gdb, print_cmd=True)

    # run the simulation
    p = subprocess.Popen(cmd, 
                        stdout=subprocess.PIPE, 
                        stderr=subprocess.STDOUT, 
                        shell=True, 
                        preexec_fn=os.setsid)
    
    psim_times = get_psim_time(p.stdout, True)["all"]  

    # copy from the output to the results dir
    os.system("cp -r {} {}".format(output_path, results_dir))
    
    # plt.plot(psim_times)
    # plt.xticks(np.arange(0, len(psim_times), 1.0))
    # plt.savefig("plots/psim-times.png")

    # # clean up the garbage 
    # os.system("rm {}".format(executable))
    # os.system("rm {}".format(shuffle_path))