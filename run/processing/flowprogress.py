import re
import sys 
import matplotlib.pyplot as plt
from pprint import pprint 
import seaborn as sns 
import json 

CORES = 0
JOBS = 0
RACKS = 0 

def parse_line(line, limit_flow_label=None):
    
    global CORES, JOBS, RACKS
    
    s = line.split("[warning]")
    if len(s) == 2: 
        critical_info = s[1].strip()
        if critical_info.startswith("flow:"):
            
            # [00:22:33.725] [warning] flow: 161 jobid: 1 srcrack: 1 dstrack: 0 start: 2100 end: 2149 fct: 50 core: 0 stepsize: 1 label: chain_6_hop_3_subflow_1 progress_history: 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 100.00 
 
            # parse the numbers: 
            flow_info = critical_info.split(" ")
            assert flow_info[0] == "flow:", f"Expected 'flow:', got {flow_info[0]}"
            flow_id = int(flow_info[1])
            assert flow_info[2] == "jobid:", f"Expected 'jobid:', got {flow_info[2]}"
            job_id = int(flow_info[3])
            assert flow_info[4] == "srcrack:", f"Expected 'srcrack:', got {flow_info[4]}"
            srcrack = int(flow_info[5])
            assert flow_info[6] == "dstrack:", f"Expected 'dstrack:', got {flow_info[6]}"
            dstrack = int(flow_info[7])
            assert flow_info[8] == "start:", f"Expected 'start:', got {flow_info[8]}"
            start_time = float(flow_info[9])
            assert flow_info[10] == "end:", f"Expected 'end:', got {flow_info[10]}"
            end_time = float(flow_info[11])
            assert flow_info[12] == "fct:", f"Expected 'fct:', got {flow_info[12]}"
            fct = float(flow_info[13])
            assert flow_info[14] == "core:", f"Expected 'core:', got {flow_info[14]}"
            core = int(flow_info[15])
            assert flow_info[16] == "stepsize:", f"Expected 'stepsize:', got {flow_info[16]}"
            stepsize = float(flow_info[17])
            assert flow_info[18] == "label:", f"Expected 'label:', got {flow_info[18]}"
            label = flow_info[19]
            assert flow_info[20] == "progress_history:", f"Expected 'progress_history:', got {flow_info[20]}"
            progress_history = list(map(float, flow_info[21:]))
            
            start_time = round(start_time / stepsize)
            end_time = round(end_time / stepsize)
            fct = round(fct / stepsize)
            
            flow_info = {
                "flow_id": flow_id,
                "job_id": job_id,
                "start_time": start_time,
                "end_time": end_time,
                "srcrack": srcrack,
                "dstrack": dstrack,
                "dir": "outgoing" if srcrack == 0 else "incoming",
                "fct": fct,
                "core": core,
                "label": label, 
                "progress_history": progress_history, 
                "flow_size": sum(progress_history)
            }
            
            if core + 1 > CORES:
                CORES = core + 1 
            if job_id > JOBS:
                JOBS = job_id
            if dstrack + 1 > RACKS:
                RACKS = dstrack + 1
            if srcrack + 1 > RACKS:
                RACKS = srcrack + 1
                
            assert len(progress_history) == fct, f"Expected progress history length to be {fct}, got {len(progress_history)}"

            if limit_flow_label is not None: 
                # if label contains the limit_flow_label, return the flow_info
                if limit_flow_label in label:
                    return flow_info
                else: 
                    return None
            else: 
                return flow_info
        
    return None
            

def parse_flow_progress(file_path, limit_flow_label):
    
    min_time = 1e9 
    max_time = 0
    
    core_flows_incoming = {} 
    core_flows_outgoing = {}
    
    with open(file_path, 'r') as file:
        for line in file:
            # print("line: ", line)   
            
            flow_info = parse_line(line, limit_flow_label)
            if flow_info is None:
                continue
            
            m = None 
            if flow_info["dir"] == "outgoing": 
                m = core_flows_outgoing
            else:
                m = core_flows_incoming
            
            if flow_info["core"] not in m:
                m[flow_info["core"]] = []
                
            m[flow_info["core"]].append(flow_info)
            
            if flow_info["start_time"] < min_time:
                min_time = flow_info["start_time"]
            if flow_info["end_time"] > max_time:
                max_time = flow_info["end_time"]
            
    return core_flows_incoming, core_flows_outgoing, min_time, max_time
 
def get_summarized_progress(progress_history):  
    # summarize the progress into tuples, each with a value and the number of times that value appears consecutively
    summarized_progress = []
    prev = progress_history[0]
    count = 1

    for i in range(1, len(progress_history)):
        if progress_history[i] == prev:
            count += 1
        else:
            summarized_progress.append((prev, count))
            prev = progress_history[i]
            count = 1
            
    summarized_progress.append((prev, count))
    
    return summarized_progress

def get_job_profiles(file_path, json_output_path=None, limit_flow_label=None):
    min_time = 1e9 
    max_time = 0
    
    # format of the job profile: 
    # for each job, there would one entry. 
    # that entry would have the start time and end time of the flow,
    # the source rack, destination rack, the core that the flow is running on,
    # the label of the flow, and the progress history of the flow.
    job_profiles = {}
    
    with open(file_path, 'r') as file:
        for line in file:
            # print("line: ", line)   
            flow_info = parse_line(line, limit_flow_label)
            if flow_info is None:
                continue
            
            if flow_info["job_id"] not in job_profiles:
                job_profiles[flow_info["job_id"]] = {
                    "period": 0, 
                    "flows": [] 
                }
                
            job_profiles[flow_info["job_id"]]["flows"].append(flow_info)
            
            if flow_info["start_time"] < min_time:
                min_time = flow_info["start_time"]
            if flow_info["end_time"] > max_time:
                max_time = flow_info["end_time"]

    expected_len = max_time + 1  
    
    for job_id, job in job_profiles.items():    
        # get the max flow end time among the flows of this job
        job["period"] = max([flow["end_time"] for flow in job["flows"]]) + 1
        
    for job_id, job in job_profiles.items():
        for flow in job["flows"]:
            leading_zeros = [0] * (flow["start_time"])
            tailing_zeros = [0] * (max_time - flow["end_time"])
            
            flow["progress_history"] = leading_zeros + flow["progress_history"] + tailing_zeros

            flow["progress_history_summarized"] = get_summarized_progress(flow["progress_history"])
            
            assert len(flow["progress_history"]) == expected_len, f"Expected length: {expected_len}, got {len(flow['progress_history'])}"

    return job_profiles, min_time, max_time
    
              
def get_color(min_time, max_time, time, jobid): 
    # for job 1, the color would gradually change from yellow to red 
    # for job 2, the color would gradually change from blue to green
    # the color is decided by the time, the earlier the time, the more yellow or blue the color would be
    if jobid == 1:
        r = (time - min_time) / (max_time - min_time)
        g = 1
        b = 0
        
    else:
        b = 1 - (time - min_time) / (max_time - min_time)
        g = 0
        r = 0
        
    return (r, g, b)
        
    
def main(file_path, limit_flow_label=None):
    flow_progress_incoming, flow_progress_outgoing, min_time, max_time = parse_flow_progress(file_path, limit_flow_label)
    print("min_time: ", min_time, " max_time: ", max_time)  
    base_util_array = [0] * (max_time - min_time + 1)
    
    # make a subplot for each core, vertically aligned 
    sns.set_theme() 
    fig, axs = plt.subplots(CORES + JOBS, RACKS, figsize=(20, 10), sharex=True, sharey=True)
    
    # increase the space between the subplots
    plt.subplots_adjust(hspace=0.5)
    plt.subplots_adjust(wspace=0.5)
    
    used_colors = {} 
    
    def plot_core_usage(core, flows, dir, ax): 
        if core == 0:   
            sns.set_palette("viridis", len(flows))
        else: 
            sns.set_palette("cividis", len(flows))
            
        ax.set_title(f"Core {core} {['Incoming', 'Outgoing'][dir]} Flows Progress")
        ax.set_xlabel("Time")
        ax.set_ylabel("Progress")
        
        print("core: ", core, " flows: ", len(flows))
        if len(flows) == 0:
            return 
        
        # the progress history for each flow is padded with 0s to match the min_time and max_time
        # so all the flows have the same history shape regardless of their start and end time
        progress_history = []
        
        for flow in flows:
            flow_progress_history = flow["progress_history"]
            padded_progress_history = base_util_array.copy()
            
            for i in range (flow["start_time"], flow["end_time"]):
                padded_progress_history[i - min_time] = flow_progress_history[i - flow["start_time"]]
            
            progress_history.append(padded_progress_history)

        labels = [str(flow['flow_id']) + "_" + flow["label"] for flow in flows]
        hatches = ['////' if flow["job_id"] == 1 else None for flow in flows]
        
        r = axs[core][dir].stackplot(range(min_time, max_time + 1), progress_history,
                                     baseline="zero", labels=labels, 
                                     edgecolor='black', linewidth=1)
        
        
        # go through each bar and set the hatch
        for i, patch in enumerate(r):
            patch.set_hatch(hatches[i])
            # get the color that was used for this bar 
            color = patch.get_facecolor()
            used_colors[labels[i]] = color
            

        # add the hatch guide to the existing legend items      
        # axs[core][dir].legend(loc='upper left', bbox_to_anchor=(1.05, 1))
        
        
    def plot_job_usage(dir, job, core_flows, ax):        
        ax.set_title(f"Job {job} {['Incoming', 'Outgoing'][dir]} Flows Progress")
        ax.set_xlabel("Time")
        ax.set_ylabel("Progress")
        
        # the progress history for each flow is padded with 0s to match the min_time and max_time
        # so all the flows have the same history shape regardless of their start and end time
        progress_history = []
        
        flows = [] 
        for core, core_flows in core_flows.items():
            for flow in core_flows:
                if flow["job_id"] == job:
                    flows.append(flow)
    
        print("job: ", job, " flows: ", len(flows))
        if len(flows) == 0: 
            return
              
        for flow in flows:
            flow_progress_history = flow["progress_history"]
            padded_progress_history = base_util_array.copy()
            
            for i in range (flow["start_time"], flow["end_time"]):
                padded_progress_history[i - min_time] = flow_progress_history[i - flow["start_time"]]
            
            progress_history.append(padded_progress_history)
        

        labels = [str(flow['flow_id']) + "_" + flow["label"] for flow in flows]
        hatches = ['////' if flow["job_id"] == 1 else None for flow in flows]
        
        r = ax.stackplot(range(min_time, max_time + 1), progress_history,
                                   baseline="zero", labels=labels, 
                                   edgecolor='black', linewidth=1)
        
        for i, patch in enumerate(r):
            patch.set_hatch(hatches[i])
            patch.set_facecolor(used_colors[labels[i]])
            
        # add the hatch guide to the existing legend items      
        # ax.legend(loc='upper left', bbox_to_anchor=(1.05, 1))

    for core, flows in flow_progress_incoming.items():
        plot_core_usage(core, flows, 0, axs[core][0])
        
    for core, flows in flow_progress_outgoing.items():
        plot_core_usage(core, flows, 1, axs[core][1]) 
        
    for dir in range(RACKS):
        for job in range(1, JOBS + 1): 
            ax = axs[job - 1 + CORES][dir]
            
            if dir == 0:
                plot_job_usage(dir, job, flow_progress_incoming, ax)
            else:
                plot_job_usage(dir, job, flow_progress_outgoing, ax)
                            
              
    # legend outside the plot, to the right of the plot 
    plt.savefig("plots/flow_progress.png", bbox_inches='tight', dpi=300)
    
if __name__ == "__main__":
    if len(sys.argv) < 2:
        path = "/tmp2/workers/worker-0/run-1/flow-info.txt"
        print("using the default path: ", path)    
    else:
        path = sys.argv[1]
    
    if len(sys.argv) > 2:
        json_output_path = sys.argv[2]
    else:
        json_output_path = None
        
    if len(sys.argv) > 3:
        limit_flow_label = sys.argv[3]
    else:
        limit_flow_label = None
            
    # main(file_path = path, 
    #      limit_flow_label = limit_flow_label)
    
    job_profiles, min_time, max_time = get_job_profiles(file_path = path)  
    # pprint(job_profiles)
    # visualize_job_profiles(job_profiles)
    # pprint(job_profiles)    
    pprint(job_profiles)    