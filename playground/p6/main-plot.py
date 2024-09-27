import matplotlib.pyplot as plt
from pprint import pprint
from math import gcd
import os
from tqdm import tqdm
import math
import random


SIM_LIMIT = 100000


script_dir = os.path.dirname(__file__)


# this function will generate a signal for a single iteration of a job. 
# the signal will be a list of 1s and 0s, where 1s represent the signal being
# on, and 0s represent the signal being off.
# doesn't change the job itself, just returns the signal.
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
# def make_padded_signals_for_jobs(jobs, key, padded_key, list_index=-1):
    
#     if list_index == -1: 
#         max_signal_length = max([len(job[key]) for job in jobs])
#     else:
#         max_signal_length = max([len(job[key][list_index]) for job in jobs])
        
        
#     for job in jobs:
#         if list_index == -1:
#             padding_signal = [0] * (max_signal_length - len(job[key]))
#             job[padded_key] = job[key] + padding_signal

#         else:
#             if padded_key not in job:
#                 job[padded_key] = [[] for _ in range(NUM_PATHS)]
                
#             padding_signal = [0] * (max_signal_length - len(job[key][list_index]))    
#             job[padded_key][list_index] = job[key][list_index] + padding_signal


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


def pad_signals(signals, cut_zeros_at_end=False):
    max_length = max([len(signal) for signal in signals])
    for signal in signals:
        signal.extend([0] * (max_length - len(signal)))
        
    if cut_zeros_at_end:
        max_non_zero_index = 0 
        
        for signal in signals:
            this_signal_non_zero_index = max_length 
            for i in range(max_length - 1, -1, -1):
                if signal[i] != 0:
                    this_signal_non_zero_index = i
                    break
                
            max_non_zero_index = max(max_non_zero_index, this_signal_non_zero_index)
            
        for signal in signals:
            del signal[max_non_zero_index + 1:]
            

def plot_signals_with_paths(jobs, plot_path, num_paths):
    
    fig, ax = plt.subplots(num_paths + 1, 1, figsize=(6, 6), sharex=True) 
    
    plt.subplots_adjust(hspace=1)
    
    # draw a horizontal dashed line between the first plot and the rest 
    for i in range(num_paths + 1): 
        ax[i].set_ylim(0, 2.1)
        padded_signals = [] 
        
        if i == 0:         
            title = "All communication for all the jobs"
            signals = [job["signal"].copy() for job in jobs]
            pad_signals(signals)
            
        else: 
            title = "Communication assigned to path {}".format((i - 1) + 1)
            path_num = i - 1
            signals = [job["path_signals"][path_num].copy() for job in jobs]
            pad_signals(signals)
                            
        stackplot_x = range(len(signals[0]))
        
        ax[i].stackplot(stackplot_x, signals,
                        colors=[job["color"] for job in jobs],
                        labels=[f"Job {job['jobid']}" for job in jobs], 
                        edgecolor="black", linewidth=1)
        
        ax[i].set_xlabel("Time")
        ax[i].set_ylabel("Signal")
        ax[i].set_title(title)
        ax[i].legend(loc="upper left", bbox_to_anchor=(1.05, 1))    
        
        # add the tick lines every 100, but don't add any new labels 
        # ax[i].set_xticks(range(0, len(stackplot_x), 100))
        # ax[i].set_xticklabels([])
                
    
        
    # midpoint between first and second subplot
    divider_y_position = (ax[0].get_position().ymin * 1.1 + ax[1].get_position().ymax * 0.9) / 2  

    # Add horizontal line to the figure at normalized figure coordinates
    fig.add_artist(plt.Line2D([0.1, 0.9], [divider_y_position, divider_y_position], 
                              color='black', linewidth=1, linestyle="--"))

    plt.savefig("{}/{}".format(script_dir, plot_path), bbox_inches="tight", dpi=300)
    plt.clf()
    

# just plot all the jobs, in separate plots in a subplot. 
def plot_all_jobs(jobs):
    fig, ax = plt.subplots(len(jobs), 1, figsize=(6, 3), sharex=True) 
    
    plt.subplots_adjust(hspace=1)
    
    for i, job in enumerate(jobs): 
        ax[i].set_ylim(0, 1.1)
        padded_signals = [] 
        title = "Job {}".format(job["jobid"])
        signals = [job["signal"].copy()]
        pad_signals(signals)
        
        stackplot_x = range(len(signals[0]))
        
        ax[i].stackplot(stackplot_x, signals,
                        colors=[job["color"]],
                        labels=[f"Job {job['jobid']}"], 
                        edgecolor="black", linewidth=1)
        
        ax[i].set_xlabel("Time")
        ax[i].set_ylabel("Communication")
        ax[i].set_title(title)
        
        # add the tick lines every 100, but don't add any new labels 
        # ax[i].set_xticks(range(0, len(stackplot_x), 100))
        # ax[i].set_xticklabels([])
        
    plt.savefig("{}/plots/all-jobs-signals.png".format(script_dir), bbox_inches="tight", dpi=300)
    plt.clf()
    
# a subplot for each initial shift. in each one, a stackplot for all the jobs. 
def plot_different_initial_shifts(jobs, all_metrics, hyperiod_multiplier, shifts, height_mult=1, ylim=2.1):
    fig, axes = plt.subplots(len(shifts), 1, figsize=(6, 1.5 * len(shifts) * height_mult), sharex=True)
    plt.subplots_adjust(hspace=1)
               
    for i, shift in enumerate(shifts): 
        if len(shifts) == 1: 
            ax = axes
        else:
            ax = axes[i]
        
        jobs[1]["initial_shift"] = shift    
        do_the_stuff(jobs, metrics=all_metrics, hyperperiod_multiplier=hyperiod_multiplier)
        
        ax.set_ylim(0, ylim)
        padded_signals = [job["signal"].copy() for job in jobs]
        pad_signals(padded_signals, cut_zeros_at_end=True)
        
        stackplot_x = range(len(padded_signals[0]))
        
        ax.stackplot(stackplot_x, padded_signals,
                     colors=[job["color"] for job in jobs],
                     labels=[f"Job {job['jobid']}" for job in jobs], 
                     edgecolor="black", linewidth=1)
        
        ax.set_xlabel("Time")
        ax.set_ylabel("Load Units")
        ax.set_title("Relative Shift: {}".format(shift))
        ax.legend(loc="upper left", bbox_to_anchor=(1.05, 1))
        
    plt.savefig("{}/plots/different-initial-shifts.png".format(script_dir), bbox_inches="tight", dpi=300)
    plt.clf()
        
        

def add_to_signal(signal1, signal2, shift):
    if len(signal1) < len(signal2) + shift:
        signal1.extend([0] * (len(signal2) + shift - len(signal1)))
    
    for i in range(len(signal2)):
        signal1[i + shift] += signal2[i]

def does_path_have_capacity(path_signal, added_signal, shift):
    for i in range(len(added_signal)):
        if path_signal[i + shift] + added_signal[i] > 1:
            return False  
    return True         


# finds the paths that strictly have enough capacity for the job signal
# selects one of those paths based on the argument. 

# if no path with capacity is found, a random path is chosen.

def x_fit_route_jobs(jobs, path_num, padded_length, fit_type):
    sum_path_signals = [[0] * padded_length for _ in range(path_num)] 
    routing_decisions = {} 
    clock_hand = 0
    
    for job in jobs:
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
                # print("No path had capacity for rep {} of job {}".format(i, job["jobid"]))
                chosen_path = random.randint(0, path_num - 1)
            else: 
                if fit_type == "first":
                    chosen_path = available_paths[0]
                elif fit_type == "random":
                    chosen_path = random.choice(available_paths)
                elif fit_type == "sticky":
                    
                    # what was the last path that was chosen for this job?
                    prev_decision = routing_decisions.get((job["jobid"], i - 1), -1)
                    if prev_decision in available_paths:
                        chosen_path = prev_decision
                    else:
                        # if the last path is not available this time, choose a random path
                        chosen_path = random.choice(available_paths)                    
                    
                elif fit_type == "clock":
                    while clock_hand not in available_paths:
                        clock_hand = (clock_hand + 1) % path_num
                    chosen_path = clock_hand
                    clock_hand = (clock_hand + 1) % path_num
                    
                elif fit_type == "best":
                    print("Not implemented yet")
                    exit(0)
            
            routing_decisions[(job["jobid"], i)] = chosen_path
                        
            # add the signal to the chosen path, and add zeros to the other paths
            add_to_signal(sum_path_signals[chosen_path],
                          job["base_signal"],
                          curr_shift)
                                    
            curr_shift += job["base_signal_length"]
    
    return routing_decisions


def random_route_jobs(jobs, path_num):
    routing_decisions = {} 
    
    for job in jobs:
        for i in range(job["signal_rep_count"]):
            routing_decisions[(job["jobid"], i)] = random.randint(0, path_num - 1)
    
    return routing_decisions

def rr_route_jobs(jobs, path_num):
    routing_decisions = {} 
    path_index = 0
    
    for job in jobs:
        for i in range(job["signal_rep_count"]):
            routing_decisions[(job["jobid"], i)] = path_index
            path_index = (path_index + 1) % path_num
    
    return routing_decisions    


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
def route_jobs(jobs, path_num, padded_length, protocol="random"):
    
    if protocol == "randomfit":
        routing_decisions = x_fit_route_jobs(jobs, path_num, padded_length, "random")
    elif protocol == "firstfit":
        routing_decisions = x_fit_route_jobs(jobs, path_num, padded_length, "first")
    elif protocol == "stickyfit":
        routing_decisions = x_fit_route_jobs(jobs, path_num, padded_length, "sticky")
    elif protocol == "clockfit":
        routing_decisions = x_fit_route_jobs(jobs, path_num, padded_length, "clock")
    elif protocol == "bestfit":
        routing_decisions = x_fit_route_jobs(jobs, path_num, padded_length, "best")
    elif protocol == "random":
        routing_decisions = random_route_jobs(jobs, path_num)
    elif protocol == "round_robin":
        routing_decisions = rr_route_jobs(jobs, path_num)
    else:
        raise ValueError("Invalid routing protocol")
    
    return routing_decisions
    

def get_metric_for_routing(jobs, routing_decisions, path_signals, path_num, metric):
    if metric == "max_path_util":
        max_path_util = 0 
        
        for j in range(path_num):
            path_util = sum(path_signals[j]) / len(path_signals[j])
            max_path_util = max(max_path_util, path_util)
        
        return max_path_util    
                
    elif metric == "overflow_ratio":
        overflow = 0
        total_signal_length = 0
        for j in range(path_num):
            for i in range(len(path_signals[j])):
                if path_signals[j][i] > 1:
                    overflow += (path_signals[j][i] - 1)
            total_signal_length += len(path_signals[j])            
        return overflow / total_signal_length

    elif metric == "repath_count":
        repath_count = 0
        for job in jobs:
            for i in range(1, job["signal_rep_count"]):
                prev_iter_decision = routing_decisions[(job["jobid"], i - 1)]
                this_iter_decision = routing_decisions[(job["jobid"], i)]
                
                if prev_iter_decision != this_iter_decision:
                    repath_count += 1               
                
        return repath_count
            
# this function should be called after the initial shift is set for the 
# jobs, i.e. the timing schedule should be done before. 
def do_the_stuff(jobs,
                 randomness=0.0, 
                 hyperperiod_multiplier=1, 
                 rep_exp_count=1,
                 num_paths=2, 
                 routing_protocol="random", 
                 metrics=None):   
    
    if metrics is None: 
        return {} 
    
    signals_lengths = [] 
    
    for job in jobs: 
        job["base_signal"] = get_base_signal(job)        
        job["base_signal_length"] = len(job["base_signal"])
        signals_lengths.append(len(job["base_signal"]))

    # go through enough iterations to get back to the initial state
    hyperperiod = lcm(signals_lengths) * hyperperiod_multiplier
    hyperperiod = min(hyperperiod, SIM_LIMIT)

    for job in jobs: 
        job["signal_rep_count"] = hyperperiod // job["base_signal_length"]
    
    max_shift = max([job["initial_shift"] for job in jobs])
    padded_length = max_shift + hyperperiod
    
    results = {metric: [] for metric in metrics}
    
    for rep in range(rep_exp_count):
        routes = route_jobs(jobs, 
                            path_num=num_paths, 
                            padded_length=padded_length, 
                            protocol=routing_protocol)
        
        path_signals = [[0] * padded_length for _ in range(num_paths)] 
         
        for job in jobs:
            job["signal"] = [0] * job["initial_shift"] 
            job["path_signals"] = [[0] * job["initial_shift"] for _ in range(num_paths)]

            current_shift = job["initial_shift"]
            
            for i in range(job["signal_rep_count"]):
                noisy_signal = get_base_signal(job, alpha=randomness)

                # add this iteration's signal to the job's signal
                job["signal"].extend(noisy_signal)

                # add this iteration's signal only to the path that was chosen by the routing algorithm
                iter_route = routes[(job["jobid"], i)] 
                            
                for j in range(num_paths):
                    if j == iter_route: 
                        job["path_signals"][j].extend(noisy_signal)
                        add_to_signal(path_signals[j], noisy_signal, current_shift) 
                    else: 
                        job["path_signals"][j].extend([0] * len(noisy_signal))

                current_shift += len(noisy_signal)
    
        for metric in metrics: 
            metric_results = get_metric_for_routing(jobs, routes, path_signals, num_paths, metric)
            results[metric].append(metric_results)
            
    return results 
            
    
def main(): 
    jobs = [
        {
            "jobid": 1,
            "color": "red",
            "teeth_count": 1, 
            "tooth_period": 60,
            "teeth_on_length": 60,
            "total_iter_time": 200,
            "initial_shift": 0,
        }, 
        {
            "jobid": 2,
            "color": "blue",
            "teeth_count": 1,
            "tooth_period": 60, 
            "teeth_on_length": 60,
            "total_iter_time": 300,
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
        #     "tooth_period": 50,
        #     "teeth_on_length": 50,
        #     "total_iter_time": 200,
        #     "initial_shift": 0,
        # }, 
        # {
        #     "jobid": 2,
        #     "color": "blue",
        #     "teeth_count": 1,
        #     "tooth_period": 50, 
        #     "teeth_on_length": 50,
        #     "total_iter_time": 300,
        #     "initial_shift": 0,
        # },
        # {
        #     "jobid": 3,
        #     "color": "green",
        #     "teeth_count": 1,
        #     "tooth_period": 50, 
        #     "teeth_on_length": 50,
        #     "total_iter_time": 300,
        #     "initial_shift": 0,
        # },
    ]
    
    num_paths = 2
    rep_count = 1
    randomness = 0
    routing_protocols = ["randomfit", "firstfit", "stickyfit", "clockfit", "random", "round_robin"]
    all_metrics = ["max_path_util", "overflow_ratio", "repath_count"]
    
    
    # pack the jobs in the beginning
    accumulated_time = 0
    for i in range(len(jobs) - 1, -1, -1): 
        jobs[i]["initial_shift"] = accumulated_time
        accumulated_time += jobs[i]["tooth_period"]
    
    jobs[0]["initial_shift"] = 0
    jobs[1]["initial_shift"] = 60  
    jobs[2]["initial_shift"] = 20  

    # for job in jobs: 
    #     print("Job {} initial shift: {}".format(job["jobid"], job["initial_shift"]))
    
    # do_the_stuff(jobs, metrics=all_metrics, hyperperiod_multiplier=2)
    # plot_all_jobs(jobs)

    # plot_different_initial_shifts(jobs, all_metrics, hyperiod_multiplier=2, shifts=[50, 125, 175], height_mult=1, ylim=2.1)
    # plot_different_initial_shifts(jobs, all_metrics, hyperiod_multiplier=2, shifts=[0, 50], height_mult=1.5, ylim=3.1)

    # jobs[0]["initial_shift"] = 0
    # jobs[1]["initial_shift"] = 75
        
    for protocol in routing_protocols:
        print("Simulating Routing protocol: {}".format(protocol))
        metrics = do_the_stuff(jobs=jobs, 
                               randomness=randomness,
                               hyperperiod_multiplier=1, 
                               rep_exp_count=rep_count, 
                               num_paths=num_paths, 
                               routing_protocol=protocol,
                               metrics=all_metrics)
    
        plot_signals_with_paths(jobs, "plots/{}-signals-paths.png".format(protocol), num_paths=2)
        
if __name__ == "__main__": 
    main()
    
    
    
