import matplotlib.pyplot as plt
from pprint import pprint
from math import gcd
import os
from tqdm import tqdm
import math
import random

script_dir = os.path.dirname(__file__)
NUM_PATHS = 2
SIM_LIMIT = 10000 
RANDOMNESS = 0.1


def get_the_array(a, b): 
    curr = 0
    iter = 0 
    
    x = [{"iter": iter, "offset": curr}]
    
    while True: 
        curr = (curr + a) % b
        iter += 1 
        if curr == 0: 
            break
        
        x.append({"iter": iter, "offset": curr})
    
    # sort by iter 
    x.sort(key=lambda x: x["offset"])
    iters = [x["iter"] for x in x]
    return iters
        
        
    
def get_base_signal(job_info, alpha=0.0):
    jobid = job_info["jobid"]
    teeth_count = job_info["teeth_count"]
    teeth_on_length = job_info["teeth_on_length"]
    
    whole_signal = []

    def rand(base_number):
        if alpha == 0:
            return base_number
        else: 
            return int(base_number * (1 + random.uniform(-alpha, alpha)))
    
    for i in range(teeth_count):
        tooth_period = job_info["tooth_period"]
        tooth_on_period = rand(teeth_on_length)
        tooth_off_period = rand(tooth_period - teeth_on_length)
        
        tooth_signal = []
        tooth_signal.extend([1] * tooth_on_period)
        tooth_signal.extend([0] * tooth_off_period)
        
        whole_signal.extend(tooth_signal)

    # add whatever's remaining to the end of the signal
    wait_time = rand(job_info["total_iter_time"] - len(whole_signal))
    whole_signal.extend([0] * (wait_time))
    
    return whole_signal

# if the signals don't have the same length, add zeros to the end of the shorter signals
def make_padded_signals_for_jobs(jobs, key, padded_key, list_index=-1):
    
    if list_index == -1: 
        max_signal_length = max([len(job[key]) for job in jobs])
    else:
        max_signal_length = max([len(job[key][list_index]) for job in jobs])
        
        
    for job in jobs:
        if list_index == -1:
            padding_signal = [0] * (max_signal_length - len(job[key]))
            job[padded_key] = job[key] + padding_signal

        else:
            if padded_key not in job:
                job[padded_key] = [[] for _ in range(NUM_PATHS)]
                
            padding_signal = [0] * (max_signal_length - len(job[key][list_index]))    
            job[padded_key][list_index] = job[key][list_index] + padding_signal

def get_sum_padded_signals(jobs, padded_key): 
    s = [0] * len(jobs[0][padded_key])
    
    for job in jobs: 
        for i in range(len(s)):
            s[i] += job[padded_key][i]
            
    return s
        
            
def lcm(numbers):
    lcm = numbers[0]
    for i in numbers[1:]:
        lcm = lcm * i // gcd(lcm, i)
    return lcm


def plot_signals(jobs, plot_path):
    # plot the stuff in subplots
    fig, ax = plt.subplots(2, 1, figsize=(10, 6))
    
    for i, job in enumerate(jobs):
        ax[0].plot(job["signal"],
                   color=job["color"],
                   label="Job {}".format(job["jobid"]))
    ax[0].set_xlabel("Time")
    ax[0].set_ylabel("Signal")
    ax[0].legend(loc="upper left", bbox_to_anchor=(1.05, 1))
    
    
    padded_signals = [job["padded_signal"] for job in jobs]
    ax[1].stackplot(range(len(padded_signals[0])), padded_signals, 
                    colors=[job["color"] for job in jobs],
                    labels=[f"Job {job['jobid']}" for job in jobs])
    ax[1].set_ylim(0, 2.1)

    plt.savefig("{}/{}".format(script_dir, plot_path), bbox_inches="tight", dpi=300)
    plt.clf()

def plot_signals_with_paths(jobs, plot_path):
    
    fig, ax = plt.subplots(NUM_PATHS + 1, 1, figsize=(6, 6), sharex=True) 
    
    plt.subplots_adjust(hspace=1)
    
    # draw a horizontal dashed line between the first plot and the rest 
    for i in range(NUM_PATHS + 1): 
        ax[i].set_ylim(0, 2.1)
        padded_signals = [] 
        
        if i == 0:         
            title = "Stackplot of all signals across all the paths"
            padded_signals = [job["padded_signal"] for job in jobs]
            
        else: 
            title = "Stackplot of path {}".format((i - 1) + 1)
            path_num = i - 1
            padded_signals = [job["padded_path_signals"][path_num] for job in jobs]
            
        stackplot_x = range(len(padded_signals[0]))
        
        ax[i].stackplot(stackplot_x, padded_signals,
                        colors=[job["color"] for job in jobs],
                        labels=[f"Job {job['jobid']}" for job in jobs])
        ax[i].set_xlabel("Time")
        ax[i].set_ylabel("Signal")
        ax[i].set_title(title)
        
        # add the tick lines every 100, but don't add any new labels 
        # ax[i].set_xticks(range(0, len(stackplot_x), 100))
        # ax[i].set_xticklabels([])
                
    
    ax[0].legend(loc="upper left", bbox_to_anchor=(1.05, 1))    
        
    # midpoint between first and second subplot
    divider_y_position = (ax[0].get_position().ymin * 1.1 + ax[1].get_position().ymax * 0.9) / 2  

    # Add horizontal line to the figure at normalized figure coordinates
    fig.add_artist(plt.Line2D([0.1, 0.9], [divider_y_position, divider_y_position], color='black', linewidth=1, linestyle="--"))

    plt.savefig("{}/{}".format(script_dir, plot_path), bbox_inches="tight", dpi=300)
    plt.clf()
    


def add_to_signal(signal1, signal2, shift):
    for i in range(len(signal2)):
        signal1[i + shift] += signal2[i]

def does_path_have_capacity(path_signal, added_signal, shift):
    for i in range(len(added_signal)):
        if path_signal[i + shift] + added_signal[i] > 1:
            return False  
    return True         


# this function will route the signals to the paths, with the assumption that 
# there's no randomness in the signal generation. the important thing that it 
# returns in the end is a map, that maps each iteration of the jobs to one path. 
# the map will be used to determine the path of the job in the simulation.
# however the simulation will be done in a way that the signals are generated
# with random noise, so the execution will differ from the routing that is done 
# here. 
# the goal will ultimately be to find the best routing strategy, and the best
# timing schedule for the jobs, such that even with the random noise, the
# signals will be routed to the paths in a way that the paths will not be congested. 

def route_signals(jobs, path_num, padded_length):
    
    sum_path_signals = [[0] * padded_length for _ in range(path_num)] 
    
    for job in jobs:
        job["decisions"] = {}

        curr_shift = job["initial_shift"]
            
        for i in range(job["signal_rep_count"]):
            chosen_path = -1 
            available_paths = [] 
            
            for j in range(path_num):
                # check if this path has capacity for the job signal
                if does_path_have_capacity(sum_path_signals[j], 
                                           job["base_signal"], 
                                           curr_shift):
                    
                    available_paths.append(j) 

            # if there's no path with capacity, choose a random path, otherwise 
            # choose a random path from the available paths
            if len(available_paths) == 0:
                print("No path had capacity for rep {} of job {}".format(i, job["jobid"]))
                chosen_path = random.randint(0, path_num - 1)
            else: 
                chosen_path = random.choice(available_paths)                 
            
            job["decisions"][i] = chosen_path
            
            # add the signal to the chosen path, and add zeros to the other paths
            add_to_signal(sum_path_signals[chosen_path],
                          job["base_signal"],
                          curr_shift)
                                    
            curr_shift += job["base_signal_length"]
    

                      
# this function should be called after the initial shift is set for the 
# jobs, i.e. the timing schedule should be done before. 
def create_signals_for_jobs(jobs, hyperperiod_multiplier=1):   
    signals_lengths = [] 
    
    for job in jobs: 
        job["base_signal"] = get_base_signal(job)        
        job["base_signal_length"] = len(job["base_signal"])
        signals_lengths.append(len(job["base_signal"]))

    # go through enough iterations to get back to the initial state
    hyperperiod = lcm(signals_lengths)
    hyperperiod = min(hyperperiod, SIM_LIMIT)

    for job in jobs: 
        base_signal_rep_count = hyperperiod // job["base_signal_length"]
        signal_rep_count = base_signal_rep_count * hyperperiod_multiplier
        job["signal_rep_count"] = signal_rep_count
    
    max_shift = max([job["initial_shift"] for job in jobs])
    padded_length = max_shift + hyperperiod
    
    route_signals(jobs, NUM_PATHS, padded_length)
    
    for job in jobs:
        pprint(job["decisions"])
    
    for job in jobs:
        job["signal"] = [0] * job["initial_shift"] 
        job["path_signals"] = [[0] * job["initial_shift"] for _ in range(NUM_PATHS)]
        
        for i in range(signal_rep_count):
            noisy_signal = get_base_signal(job, alpha=RANDOMNESS)
            
            job["signal"].extend(noisy_signal)
            
            for j in range(NUM_PATHS):
                if j == job["decisions"][i]: 
                    job["path_signals"][j].extend(noisy_signal)
                else: 
                    job["path_signals"][j].extend([0] * len(noisy_signal))
            
    make_padded_signals_for_jobs(jobs, "signal", "padded_signal")

    for j in range(NUM_PATHS): 
        make_padded_signals_for_jobs(jobs, "path_signals", "padded_path_signals", j)

    
# this function will make copies of the jobs, so nothing should be modified 
# the arguments by the time this function is finished. 
def find_compatible_regions(job1, job2, plot=False):
    job1_len = len(get_base_signal(job1))
    job2_len = len(get_base_signal(job2))
    
    print("Job 1 length: ", job1_len, " Job 2 length: ", job2_len)
    H = lcm([job1_len, job2_len])
    
    min_len_job = 1 
    min_len = job1_len
    
    if job1_len > job2_len:
        min_len_job = 2
        min_len = job2_len 
        
    compat_scores = [] 
    
    for i in tqdm(range(min_len)):
        if min_len_job == 1:
            jobs = [job1.copy(), job2.copy()]   
        else:
            jobs = [job2.copy(), job1.copy()]
            
        jobs[1]["initial_shift"] = i 

        create_signals_for_jobs(jobs, hyperperiod_multiplier=2)
        s = get_sum_padded_signals(jobs, padded_key="padded_signal")
        
        above_limit = 0
        for j in range(i, i + H): 
            if s[j] > 1: 
                above_limit += 1 

        compat_score = 1 - (above_limit / H) 
        compat_scores.append(compat_score) 

    if plot: 
        plt.plot(compat_scores, color="green", label="Compatibility")
        plt.xlabel("Shift")
        plt.ylabel("Compatibility")
        plt.savefig("{}/{}".format(script_dir, "compat.png"), 
                    bbox_inches="tight", dpi=300)
        plt.clf()
        
    return compat_scores 
    
def main(): 
    jobs = [
        {
            "jobid": 1,
            "color": "red",
            "teeth_count": 1, 
            "tooth_period": 60,
            "teeth_on_length": 60,
            "total_iter_time": 300,
            "initial_shift": 0,
        }, 
        {
            "jobid": 2,
            "color": "blue",
            "teeth_count": 1,
            "tooth_period": 60, 
            "teeth_on_length": 60,
            "total_iter_time": 400,
            "initial_shift": 0,
        },
        {
            "jobid": 3,
            "color": "green",
            "teeth_count": 1,
            "tooth_period": 60, 
            "teeth_on_length": 60,
            "total_iter_time": 500,
            "initial_shift": 0,
        },
        
        
        # {
        #     "jobid": 1,
        #     "color": "red",
        #     "teeth_count": 1, 
        #     "tooth_period": 60,
        #     "teeth_on_length": 60,
        #     "total_iter_time": 200,
        #     "initial_shift": 0,
        # }, 
        # {
        #     "jobid": 2,
        #     "color": "blue",
        #     "teeth_count": 1,
        #     "tooth_period": 60, 
        #     "teeth_on_length": 60,
        #     "total_iter_time": 300,
        #     "initial_shift": 0,
        # },
        # {
        #     "jobid": 3,
        #     "color": "green",
        #     "teeth_count": 1,
        #     "tooth_period": 60, 
        #     "teeth_on_length": 60,
        #     "total_iter_time": 300,
        #     "initial_shift": 0,
        # },
        
        
        # {
        #     "jobid": 4,
        #     "color": "black",
        #     "teeth_count": 1,
        #     "tooth_period": 20, 
        #     "teeth_on_length": 20,
        #     "total_iter_time": 700,
        #     "initial_shift": 0,
        # },
        
        # {
        #     "jobid": 1,
        #     "color": "red",
        #     "teeth_count": 1, 
        #     "tooth_period": 100,
        #     "teeth_on_length": 100,
        #     "total_iter_time": 600,
        #     "initial_shift": 0,
        # }, 
        # {
        #     "jobid": 2,
        #     "color": "blue",
        #     "teeth_count": 1,
        #     "tooth_period": 100, 
        #     "teeth_on_length": 100,
        #     "total_iter_time": 400,
        #     "initial_shift": 0,
        # },
    ]
    
    # compat = find_compatible_regions(jobs[0], jobs[1], plot=True)
    # arg_max_compat = max(range(len(compat)), key=compat.__getitem__) 
    # print("Most compatible region is at shift: ", arg_max_compat)
    # print("Compatibility at max: ", compat[arg_max_compat])

    # pack the jobs in the beginning
    accumulated_time = 0
    for i in range(len(jobs) - 1, -1, -1): 
        print("Job {} initial shift: {}".format(i, accumulated_time))
        jobs[i]["initial_shift"] = accumulated_time
        accumulated_time += jobs[i]["tooth_period"]
    
    # jobs[0]["initial_shift"] = 120
    # jobs[1]["initial_shift"] = 60    

    create_signals_for_jobs(jobs=jobs, hyperperiod_multiplier=1)
    plot_signals(jobs, "signals.png")
    plot_signals_with_paths(jobs, "signals-paths.png")
    
        
if __name__ == "__main__": 
    main()
    
    
    
