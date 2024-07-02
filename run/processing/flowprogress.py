import re
import sys 
import matplotlib.pyplot as plt
from pprint import pprint 

def parse_line(line):
    s = line.split("[critical]")
    if len(s) == 2: 
        critical_info = s[1].strip()
        if critical_info.startswith("flow:"):
            
            # critical_info looks like this. 
            # flow: 31 jobid: 1 start: 656 end: 717 fct: 61 core: 0 progress_history: 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 66.67 0.00
            
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
                        
            return flow_info
        
    return None
            

def parse_flow_progress(file_path):
    
    min_time = 1e9 
    max_time = 0
    
    core_flows_incoming = {} 
    core_flows_outgoing = {}
    
    with open(file_path, 'r') as file:
        for line in file:
            flow_info = parse_line(line)
            
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
        
    
def main():
    file_path = sys.argv[1]  # Update this path to your file containing the log data
    flow_progress_incoming, flow_progress_outgoing, min_time, max_time = parse_flow_progress(file_path)

    print("min_time: ", min_time, " max_time: ", max_time)  
    
    base_util_array = [0] * (max_time - min_time + 1)
    
    # make a subplot for each core, vertically aligned 
    fig, axs = plt.subplots(2, 2, figsize=(20, 10))
    
    # increase the space between the subplots
    plt.subplots_adjust(hspace=0.5)
    plt.subplots_adjust(wspace=0.5)
        
    def func(core, flows, j): 
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
        

        labels = [flow['label'] for flow in flows]
        hatches = ['////' if flow["job_id"] == 1 else 'oo' for flow in flows]
        colors = [get_color(min_time, max_time, flow["start_time"], flow["job_id"]) for flow in flows]
        
        r = axs[core][j].stackplot(range(min_time, max_time + 1), progress_history,
                                   labels=labels, edgecolor='black', linewidth=1)
        
        # go through each bar and set the hatch
        for i, patch in enumerate(r):
            patch.set_hatch(hatches[i])
            # patch.set_facecolor(colors[i])
            
        axs[core][j].set_title(f"Core {core} {['Incoming', 'Outgoing'][j]} Flows Progress")
        axs[core][j].set_xlabel("Time")
        axs[core][j].set_ylabel("Progress")

        # add the hatch guide to the existing legend items      
        axs[core][j].legend(loc='upper left', bbox_to_anchor=(1.05, 1))


        
    for core, flows in flow_progress_incoming.items():
        func(core, flows, 0)
        
    for core, flows in flow_progress_outgoing.items():
        func(core, flows, 1) 
    
    # legend outside the plot, to the right of the plot 
    plt.savefig("plots/flow_progress.png", bbox_inches='tight', dpi=300)
    
if __name__ == "__main__":
    main()
    