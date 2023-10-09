import os
from pprint import pprint 
import itertools
import subprocess
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import sys 

run_id = os.popen("date +%s | sha256sum | base64 | head -c 8").read()

# setting up the basic paths
base_dir = "/home/faridzandi/git/psim" 
build_path = base_dir + "/build"
run_path = base_dir + "/run"
base_executable = build_path + "/psim"
executable = build_path + "/psim-" + run_id

# build the executable, exit if build fails
os.chdir(build_path)
# run the make -j command, get the exit code
exit_code = os.system("make -j")
if exit_code != 0:
    print("make failed, exiting")
    sys.exit(1)
os.chdir(run_path)
os.system("cp {} {}".format(base_executable, executable))


# get the parameters from the command line
params = {}
for i, arg in enumerate(sys.argv):
    if i == 0:
        continue
    p = arg.split("=")
    key = p[0][2:]
    val = p[1]
    if val == "true":
        val = True
    if val == "false":
        val = False
    params[key] = val
    

options = {
    "protocol-file-dir": base_dir + "/input/128search-dpstart-2",
    "protocol-file-name": "candle128-simtime.txt",
    # "protocol-file-name": "vgg128-comm.txt",
    "step-size": 10,
    "rep-count": 10, 
    "link-bandwidth": 100,
    "initial-rate": 100,
    "min-rate": 100,
    "ft-core-count": 4,
    "ft-agg-per-pod": 4,
    "console-log-level": 3,
    "file-log-level": 2,
    "ft-server-tor-link-capacity-mult": 1,
    "ft-tor-agg-link-capacity-mult": 1,
    "ft-agg-core-link-capacity-mult": 1,
    "priority-allocator": "fairshare",
    "core-selection-mechanism": "futureload",
    "shuffle-device-map": True,
}

options.update(params)

# create the command
cmd = executable
for option in options.items():
    if option[1] is False:
        continue
    elif option[1] is True:
        cmd += " --" + option[0]
    else: 
        cmd += " --" + option[0] + "=" + str(option[1])

print("running the command:", cmd)            

subprocess.run(cmd, stdout=sys.stdout, stderr=sys.stderr, shell=True)

os.system("rm {}".format(executable))
