import matplotlib.pyplot as plt
from pprint import pprint
from math import gcd
import os
from tqdm import tqdm
import math
import random
import copy 


SIM_LIMIT = 100000


script_dir = os.path.dirname(__file__)


# this function will generate a signal for a single iteration of a job. 
# the signal will be a list of 1s and 0s, where 1s represent the signal being
# on, and 0s represent the signal being off.
# doesn't change the job itself, just returns the signal.
def get_base_signal(job_info, alpha=0.0):
    whole_signal = []

    def rand(base_number):
        if alpha == 0:
            return base_number
        else: 
            return int(base_number * (1 + random.uniform(-alpha, alpha)))
    
    for chunk_period, chuck_rate in job_info["chunks"]:
        chunk_length = rand(chunk_period)
        chunk_signal = [chuck_rate] * chunk_length
        whole_signal.extend(chunk_signal)

    # add whatever's remaining to the end of the signal
    wait_time = rand(job_info["total_iter_time"] - len(whole_signal))
    whole_signal.extend([0] * (wait_time))
    
    return whole_signal
        
            
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

def plot_signals_with_paths(jobs, plot_path, num_paths):
    
    fig, ax = plt.subplots(num_paths + 1, 1, figsize=(6, 6), sharex=True) 
    
    plt.subplots_adjust(hspace=1)
    
    # draw a horizontal dashed line between the first plot and the rest 
    for i in range(num_paths + 1): 
        ax[i].set_ylim(0, 2.1)
        padded_signals = [] 
        
        if i == 0:         
            title = "Stackplot of all signals across all the paths"
            padded_signals = [job["signal"].copy() for job in jobs]
            max_length = max([len(signal) for signal in padded_signals])
            for signal in padded_signals:
                signal.extend([0] * (max_length - len(signal)))
            
        else: 
            title = "Stackplot of path {}".format((i - 1) + 1)
            path_num = i - 1
            padded_signals = [job["path_signals"][path_num].copy() for job in jobs]
            max_length = max([len(job["path_signals"][path_num]) for job in jobs])
            for signal in padded_signals:
                signal.extend([0] * (max_length - len(signal)))
                            
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
    if len(signal1) < len(signal2) + shift:
        signal1.extend([0] * (len(signal2) + shift - len(signal1)))
    
    for i in range(len(signal2)):
        signal1[i + shift] += signal2[i]


def does_path_have_capacity(path_signal, added_signal, shift):

    for i in range(len(added_signal)):
        
        # if the added signal lies outside the path signal, then it's free 
        # from that point on. we can just return early here. 
        if i + shift >= len(path_signal):
            return True 
        
        # if the path signal is already at capacity, then the path doesn't have
        # capacity for the added signal.
        if path_signal[i + shift] + added_signal[i] > 1:
            return False  
        
    # if we've reached this point, then the path has capacity for the signal
    return True         


# finds the paths that strictly have enough capacity for the job signal
# selects one of those paths based on the argument. 

# if no path with capacity is found, a random path is chosen.

def x_fit_route_jobs(jobs, path_num, fit_type):
    sum_path_signals = [[] for _ in range(path_num)] 
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
            
            routing_decisions[(job["jobid"], i)] = {"path": chosen_path, 
                                                    "time": curr_shift}
                        
            # add the signal to the chosen path, and add zeros to the other paths
            add_to_signal(sum_path_signals[chosen_path],
                          job["base_signal"],
                          curr_shift)
                                    
            curr_shift += job["total_iter_time"]
    
    return routing_decisions


def random_route_jobs(jobs, path_num):
    routing_decisions = {} 
    
    for job in jobs:
        current_shift = job["initial_shift"]
        
        for i in range(job["signal_rep_count"]):
            path = random.randint(0, path_num - 1)

            routing_decisions[(job["jobid"], i)] = {"path": path,  
                                                    "time": current_shift}

            current_shift += job["total_iter_time"] 
            
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
def schedule_jobs(jobs, path_num, protocol="random"):
    if protocol == "randomfit":
        schedule = x_fit_route_jobs(jobs, path_num, "random")
    elif protocol == "firstfit":
        schedule = x_fit_route_jobs(jobs, path_num, "first")
    elif protocol == "stickyfit":
        schedule = x_fit_route_jobs(jobs, path_num, "sticky")
    elif protocol == "clockfit":
        schedule = x_fit_route_jobs(jobs, path_num, "clock")
    elif protocol == "bestfit":
        schedule = x_fit_route_jobs(jobs, path_num, "best")
    elif protocol == "random":
        schedule = random_route_jobs(jobs, path_num)
    elif protocol == "round_robin":
        schedule = rr_route_jobs(jobs, path_num)
    else:
        raise ValueError("Invalid routing protocol")
    
    return schedule
    

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
    
    
# gets the LCM of the signals lengths, 
# multiplies it by the hyperperiod_multiplier,
# cap the result at SIM_LIMIT        
def get_simulation_length(jobs, hyperperiod_multiplier=1):
    signals_lengths = [] 

    for job in jobs: 
        signals_lengths.append(job["total_iter_time"])

    # go through enough iterations to get back to the initial state
    hyperperiod = lcm(signals_lengths) * hyperperiod_multiplier
    hyperperiod = min(hyperperiod, SIM_LIMIT)
    
    return hyperperiod
    

def simulate_jobs(jobs, schedule, num_paths, randomness=0.0):
    
    path_signals = [[] for _ in range(num_paths)] 
         
    for job in jobs:
        job["signal"] = [0] * job["initial_shift"] 
        job["path_signals"] = [[0] * job["initial_shift"] for _ in range(num_paths)]

        current_shift = job["initial_shift"]
        
        for i in range(job["signal_rep_count"]):
            noisy_signal = get_base_signal(job, alpha=randomness)

            # add this iteration's signal to the job's signal
            job["signal"].extend(noisy_signal)

            # add this iteration's signal only to the path that was chosen by the routing algorithm
            iter_route = schedule[(job["jobid"], i)]["path"]
            
                        
            for j in range(num_paths):
                if j == iter_route: 
                    job["path_signals"][j].extend(noisy_signal)
                    add_to_signal(path_signals[j], noisy_signal, current_shift) 
                else: 
                    job["path_signals"][j].extend([0] * len(noisy_signal))

            current_shift += len(noisy_signal)
            
    return path_signals


# this function should be called after the initial shift is set for the 
# jobs, i.e. the timing schedule should be done before. 
def run_experiment(jobs,
                   randomness=0.0, 
                   hyperperiod_multiplier=1, 
                   rep_exp_count=1,
                   num_paths=2, 
                   routing_protocol="random", 
                   metrics=None):   
    
    if metrics is None: 
        return {} 
    
    for job in jobs:
        job["base_signal"] = get_base_signal(job, alpha=randomness)
        
    # todo: so this is not the simulation length, since something will be 
    # added to it later. What's the name of this thing then? 
    simulation_length = get_simulation_length(jobs, hyperperiod_multiplier)
    
    # todo: there's the possibility that the last iteration of the job will be
    # cut off, if the simulation length is not a multiple of the job length.
    # so there will be a period where some of the signals are not present, which 
    # is not good. 
    for job in jobs: 
        job["signal_rep_count"] = simulation_length // job["total_iter_time"]

    results = {metric: [] for metric in metrics}
    
    for rep in range(rep_exp_count):

        # we schedule the jobs here. So the routing decisions are made here, but 
        # timing decisions can also be made here.
        
        # todo: the result of this should be a schedule. The schedule contains the 
        # routing decisions that are made for the signals, and the timing decisions
        # that are made for the signals. The timing decisions are the shifts that
        # are made for the signals. more than one shift can be made for a signal. 
        
        # some schedules have a random component, so to be safe, we should make the 
        # schedule every time. However, we could also make the schedule once, and
        # then use it for all the experiments. I assume making it every time gives us a
        # better view of the distribution of the metrics.
        schedule = schedule_jobs(jobs, 
                                 path_num=num_paths, 
                                 protocol=routing_protocol)
        

        if rep == 0: 
            pprint(schedule)
            
        # todo: at this point, we have a schedule for running the jobs, but now we
        # want to simulate the signals being generated and routed to the paths.
        # in the simulation part, there will be overutilization for the paths at 
        # some points, and that will result in some signals being stretched out, 
        path_signals = simulate_jobs(jobs, 
                                     schedule=schedule, 
                                     num_paths=num_paths, 
                                     randomness=randomness)
        
        for metric in metrics: 
            metric_results = get_metric_for_routing(jobs, schedule, path_signals, num_paths, metric)
            results[metric].append(metric_results)
            
    return results 
            
    
def main(): 
    jobs = [
        {
            "jobid": 1,
            "color": "red",
            "chunks": [(100, 1)],
            "total_iter_time": 300,
            "initial_shift": 0,
        }, 
        {
            "jobid": 2,
            "color": "blue",
            "chunks": [(100, 1)],
            "total_iter_time": 200,
            "initial_shift": 0,
        },
        {
            "jobid": 3,
            "color": "green",
            "chunks": [(100, 1)],   
            "total_iter_time": 300,
            "initial_shift": 0,
        },
    ]
    
    num_paths = 2
    rep_count = 100
    randomness = 0.01
    
    jobs[0]["initial_shift"] = 120
    jobs[1]["initial_shift"] = 60    

    routing_protocols = ["randomfit", "firstfit", "stickyfit", "clockfit", "random", "round_robin"]
    all_metrics = ["max_path_util", "overflow_ratio", "repath_count"]
    
    fig, axes = plt.subplots(len(all_metrics), 1, figsize=(10, 6))
    plt.subplots_adjust(hspace=0.5)
    
    for protocol in routing_protocols:
        print("Simulating Routing protocol: {}".format(protocol))
        
        metrics = run_experiment(jobs=jobs, 
                                 randomness=randomness,
                                 hyperperiod_multiplier=20, 
                                 rep_exp_count=rep_count, 
                                 num_paths=num_paths, 
                                 routing_protocol=protocol,
                                 metrics=all_metrics)
        
        # plot_signals(jobs, "signals.png")
        # plot_signals_with_paths(jobs, 
        #                         "plots/{}-signals-paths.png".format(protocol), 
        #                         num_paths=2)
        
        # plot the CDF of the metris  
        for key, metric_list in metrics.items(): 
            metric_list.sort()
            y = [i / len(metric_list) for i in range(len(metric_list))]
            ax = axes[all_metrics.index(key)]
            ax.plot(metric_list, y, label=protocol)
            ax.title.set_text(key)
        
    for ax in axes: 
        ax.legend(loc="upper left", bbox_to_anchor=(1.05, 1))
                
    plt.savefig("{}/routing-metric-cdf.png".format(script_dir), bbox_inches="tight", dpi=300)
    plt.clf()
            
if __name__ == "__main__": 
    main()
    
    
    
