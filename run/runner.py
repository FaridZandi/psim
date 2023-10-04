import os
from pprint import pprint 
import itertools
import subprocess
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


# setting up the basic paths
base_dir = "/home/faridzandi/git/psim" 
workloads_dir = base_dir + "/input/128search-dpstart-2/"
build_path = base_dir + "/build"
executable = build_path + "/psim"
run_path = base_dir + "/run"
protocol_names = list(os.listdir(workloads_dir))[0:3]

pd_frame = None
reload_data = False
if not os.path.exists("results.csv"):
    reload_data = True    
    
if reload_data:
    base_options = {
        "protocol-file-dir": workloads_dir,
        "step-size": 1,
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
    }




    # build the executable, exit if build fails
    os.chdir(build_path)
    os.system("make -j")
    exit_code = os.system("echo $?")
    if exit_code != 0:
        exit(1)
    os.chdir(run_path)




    sweep_config = {	
        "core-selection-mechanism": ["random", "roundrobin", "leastloaded", "futureload"],
        "protocol-file-name": protocol_names,
    }
    keys, values = zip(*sweep_config.items())
    permutations_dicts = [dict(zip(keys, v)) for v in itertools.product(*values)]




    exp_results = [] 
    for exp in permutations_dicts:  
        pprint(exp)       
        
        rep_count = None
        if exp["core-selection-mechanism"] == "futureload":
            rep_count = 5
        else: 
            rep_count = 5
        
        options = {
            "rep-count": rep_count,
        }
        options.update(base_options)
        options.update(exp)
        
        # create the command
        cmd = executable
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
        
        # print (output)
        
        # go through the output line by line
        last_psim_time = 0
        min_psim_time = 1e12
        max_psim_time = 0 
        
        for line in output.splitlines():
            if "psim time" in line:
                psim_time = float(line.strip().split(" ")[-1])
                if psim_time > max_psim_time:
                    max_psim_time = psim_time
                if psim_time < min_psim_time:
                    min_psim_time = psim_time
                last_psim_time = psim_time
        

        exp_results.append({
            "core-selection-mechanism": exp["core-selection-mechanism"],
            "protocol-file-name": exp["protocol-file-name"],
            "min_psim_time": min_psim_time,
            "max_psim_time": max_psim_time,
            "last_psim_time": last_psim_time,
        })
                
        print("min time: {}, max time: {}, last time: {}".format(
            min_psim_time, max_psim_time, last_psim_time))
        
        print("-----------------------------")
        print("-----------------------------")

    pd_frame = pd.DataFrame(exp_results)
    pd_frame.to_csv("results.csv") 
else:
    pd_frame = pd.read_csv("results.csv")

pd_frame.reindex()     
protocols = pd_frame["protocol-file-name"].unique()
core_selection_mechanisms = pd_frame["core-selection-mechanism"].unique()


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

plt.figure()

# len(protocols) items, with (group_width + group_spacing) space between each two items
x = np.arange(len(protocols)) * (group_width + group_spacing)

for i, mech in enumerate(core_selection_mechanisms):
    # get the data for this mech
    mech_data = pd_frame[pd_frame["core-selection-mechanism"] == mech]
    x_offset = x + (i * bar_width - group_width / 2) 
    plt.bar(x_offset, mech_data["rel_max_psim_time"], width=bar_width)
    plt.bar(x_offset, mech_data["rel_min_psim_time"], width=bar_width, label=mech) 
    plt.plot(x_offset, mech_data["rel_last_psim_time"], marker="o", linestyle="None")
    
        

#change the ticks to be the protocol names
plt.xticks(x, protocols)
# rotate the ticks by 45 degrees
plt.xticks(rotation=90)

plt.legend()
plt.savefig("results.png", bbox_inches="tight", dpi=300)