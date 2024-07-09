import re
import sys 
import matplotlib.pyplot as plt
from pprint import pprint 
import seaborn as sns 

CORES = 1 
JOBS = 2 
DIRS = 2 

def parse_line(line, limit_flow_label=None):
    s = line.split("[critical]")
    if len(s) == 2: 
        critical_info = s[1].strip()
        if critical_info.startswith("flow:"):
            
            # parse the numbers: 
            flow_info = critical_info.split(" ")
            flow_id = int(flow_info[1])
            job_id = int(flow_info[3])
            dir_str = flow_info[5]
            start_time = float(flow_info[7])
            end_time = float(flow_info[9])
            fct = float(flow_info[11])
            core = int(flow_info[13])
            stepsize = float(flow_info[15])
            label = flow_info[17]
            progress_history = list(map(float, flow_info[19:]))
            
            start_time = round(start_time / stepsize)
            end_time = round(end_time / stepsize)
            fct = round(fct / stepsize)
            
            flow_info = {
                "flow_id": flow_id,
                "job_id": job_id,
                "start_time": start_time,
                "end_time": end_time,
                "dir": dir_str, # "in" or "out
                "fct": fct,
                "core": core,
                "label": label, 
                "progress_history": progress_history
            }
            
            if len(progress_history) != fct: 
                print("Error: progress history length does not match fct, len(progress_history): ", len(progress_history), " fct: ", fct)
                print("Probably something will go wrong") 
            
            sum_progress = sum(progress_history)
            print("sum_progress: ", sum_progress)
                
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
    fig, axs = plt.subplots(CORES + JOBS, DIRS, figsize=(20, 10), sharex=True, sharey=True)
    
    # increase the space between the subplots
    plt.subplots_adjust(hspace=0.5)
    plt.subplots_adjust(wspace=0.5)
    
    used_colors = {} 
    
    def plot_core_usage(core, flows, dir, ax): 
        
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
        axs[core][dir].legend(loc='upper left', bbox_to_anchor=(1.05, 1))
        
        
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
        
    for dir in range(DIRS):
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
        path = "workers/worker-0/run-1/runtime.txt"
        print("using the default path: ", path)    
    else :
        path = sys.argv[1]
    
    if len(sys.argv) > 2:
        limit_flow_label = sys.argv[2]
    else:
        limit_flow_label = None
        
    main(file_path = path, 
         limit_flow_label = limit_flow_label)
    