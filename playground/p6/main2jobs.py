import matplotlib.pyplot as plt
from pprint import pprint
from math import gcd
import os
from tqdm import tqdm
import math

script_dir = os.path.dirname(__file__)

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
        
        
    
def get_base_signal(job_info):
    jobid = job_info["jobid"]
    teeth_count = job_info["teeth_count"]
    teeth_on_length = job_info["teeth_on_length"]
    
    whole_signal = []
     
    for i in range(teeth_count):
        tooth_period = job_info["tooth_period"]
        tooth_on_period = teeth_on_length 
        tooth_off_period = (tooth_period - teeth_on_length)
        
        tooth_signal = []
        tooth_signal.extend([1] * tooth_on_period)
        tooth_signal.extend([0] * tooth_off_period)
        
        whole_signal.extend(tooth_signal)

    # add whatever's remaining to the end of the signal
    wait_time = job_info["total_iter_time"] - len(whole_signal)
    whole_signal.extend([0] * wait_time)
    
    return whole_signal

# if the signals don't have the same length, add zeros to the end of the shorter signals
def make_padded_signals_for_jobs(jobs, key, padded_key): 
    max_signal_length = max([len(job[key]) for job in jobs])
    for job in jobs:
        job[padded_key] = job[key] + [0] * (max_signal_length - len(job[key]))

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
    fig, ax = plt.subplots(3, 1, figsize=(10, 6)) 
    plt.subplots_adjust(hspace=0.5)
    
    for i in range(3): 
        ax[i].set_ylim(0, 2.1)
        key = ["padded_signal", "padded_path1_signal", "padded_path2_signal"][i]
        
        padded_signals = [job[key] for job in jobs]
        stackplot_x = range(len(padded_signals[0]))

        ax[i].stackplot(stackplot_x, padded_signals,
                        colors=[job["color"] for job in jobs],
                        labels=[f"Job {job['jobid']}" for job in jobs])
        
        ax[i].set_xlabel("Time")
        ax[i].set_ylabel("Signal")
    
    ax[0].legend(loc="upper left", bbox_to_anchor=(1.05, 1))    
        
    plt.savefig("{}/{}".format(script_dir, plot_path), bbox_inches="tight", dpi=300)
    plt.clf()
    


def create_signals_for_jobs(jobs, hyperperiod_multiplier=1):   
    signals_lengths = [] 
    
    for job in jobs: 
        job["base_signal"] = get_base_signal(job)        
        job["base_signal_length"] = len(job["base_signal"])
        signals_lengths.append(len(job["base_signal"]))

    # go through enough iterations to get back to the initial state
    hyperperiod = lcm(signals_lengths)
    
    if len(signals_lengths) != 2:
        print("Only two jobs are supported for now.")
        print("I hope you know what you doing. Good luck!")
        exit(0) 
        
    the_array = get_the_array(*signals_lengths)
    print("the array: ", the_array)
    x_plus_y = jobs[0]["tooth_period"] + jobs[1]["tooth_period"]
    array_limit = math.ceil(x_plus_y / math.gcd(*signals_lengths)) - 1
    
    print("x + y: ", x_plus_y)  
    print("Array limit: ", array_limit)
      
    for job in jobs:
        base_signal_rep_count = hyperperiod // job["base_signal_length"]
        signal_rep_count = base_signal_rep_count * hyperperiod_multiplier
        initial_shift = job["initial_shift"]
        
        job["signal"] = [0] * initial_shift + job["base_signal"] * signal_rep_count
        job["path1_signal"] = [0] * initial_shift 
        job["path2_signal"] = [0] * initial_shift
    
        ## this is highly experimental for now. don't worry about the extremely bad code         
        for i in range(signal_rep_count):
            iter = i % base_signal_rep_count 
            if job["jobid"] == 1 and iter in the_array[-1 * array_limit:]:
                job["path1_signal"].extend(job["base_signal"])
                job["path2_signal"].extend([0] * len(job["base_signal"]))
            else: 
                job["path1_signal"].extend([0] * len(job["base_signal"]))
                job["path2_signal"].extend(job["base_signal"])
                    
    make_padded_signals_for_jobs(jobs, "signal", "padded_signal")
    make_padded_signals_for_jobs(jobs, "path1_signal", "padded_path1_signal")
    make_padded_signals_for_jobs(jobs, "path2_signal", "padded_path2_signal")
    
    s_signal = get_sum_padded_signals(jobs, "padded_signal")
    s_path1 = get_sum_padded_signals(jobs, "padded_path1_signal")
    s_path2 = get_sum_padded_signals(jobs, "padded_path2_signal")
    
    s_signal_max = max(s_signal) 
    s_path1_max = max(s_path1)
    s_path2_max = max(s_path2)
    
    print("Signal max: ", s_signal_max, ", Path 1 max: ", s_path1_max, ", Path 2 max: ", s_path2_max)
    

    
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
            "tooth_period": 100,
            "teeth_on_length": 100,
            "total_iter_time": 554,
            "initial_shift": 0,
        }, 
        {
            "jobid": 2,
            "color": "blue",
            "teeth_count": 1,
            "tooth_period": 100, 
            "teeth_on_length": 100,
            "total_iter_time": 768,
            "initial_shift": 0,
        },
    ]
    
    # compat = find_compatible_regions(jobs[0], jobs[1], plot=True)
    # arg_max_compat = max(range(len(compat)), key=compat.__getitem__) 
    # print("Most compatible region is at shift: ", arg_max_compat)
    # print("Compatibility at max: ", compat[arg_max_compat])
    
    arg_max_compat = jobs[1]["tooth_period"]
    
    jobs[0]["initial_shift"] = arg_max_compat
    create_signals_for_jobs(jobs=jobs, hyperperiod_multiplier=2)
    plot_signals(jobs, "signals.png")
    plot_signals_with_paths(jobs, "signals-paths.png")
    
        
if __name__ == "__main__": 
    main()
    
    
    
