import subprocess
import os
import sys 
import re
import signal 

import matplotlib.pyplot as plt
import numpy as np

from utils.util import *
from pprint import pprint 


# general paths
base_dir = get_base_dir()
build_path = base_dir + "/build"
run_path = base_dir + "/run/"
base_executable = build_path + "/psim"
input_dir = base_dir + "/input/"
workers_dir = run_path + "/workers/"


# run-specific paths
run_id = str(get_incremented_number())
executable = build_path + "/psim-" + run_id
ga_run_dir = run_path + "results/ga/{}/".format(run_id)
os.system("mkdir -p {}".format(ga_run_dir))
shuffle_path  = ga_run_dir + "shuffle.txt"
ga_rounds_dir = ga_run_dir + "rounds/"
ga_baselines_dir = ga_rounds_dir + "baselines/"


# input parameters
protocol_file_name = "candle128-simtime.txt"
# protocol_file_name = "build-ring"
# protocol_file_name = "transformer128-simtime+compute.txt"
# protocol_file_name = "vgg128-simtime+maxmem+mem.txt"
# protocol_file_name = "dlrm128-simtime.txt"

core_count = 4
min_rate_ratio = 0.1 # if the ratio isn't 1, then rep_count should be > 1
rep_count = 50
link_bandwidth = 400
sorting_metric = "max" # min, avg, max
population_size = 40 
tweak_tolerance_limit = 5
halving_steps = 1
halving_rounds_interval = 1000
do_shuffle = False
final_step_size = 1
final_perm_count_range = (1, 10)

# computed parameters
ga_rounds = halving_rounds_interval * halving_steps
max_multiplier = 2 ** (halving_steps - 1)
base_step_size = final_step_size * max_multiplier
permutation_count_low = final_perm_count_range[0] * max_multiplier
permutation_count_high = final_perm_count_range[1] * max_multiplier


# [18:22:53.193] [warning] flow 9925 core 0 crit false
lb_decisions_file_regex = re.compile(r".*flow \d+ core \d+ crit (true|false)")
# [19:19:49.369] [critical] flow 12125 old core 3 new core 1 regret score 296
regret_file_regex = re.compile(r".*flow \d+ old core \d+ new core \d+ regret score \d+")



# simulator options and parameters
options = {
    "protocol-file-dir": base_dir + "/input/128search-dpstart-2",
    "protocol-file-name": protocol_file_name,
    # "protocol-file-name": "transformer128-simtime+compute.txt",

    "step-size": base_step_size,
    "core-status-profiling-interval": int(max(base_step_size,1)),
    "rep-count": rep_count, 
    "console-log-level": 4,
    "file-log-level": 3,
    
    "initial-rate": link_bandwidth,
    "min-rate": int(link_bandwidth * min_rate_ratio), 
    "priority-allocator": "fairshare", #"priorityqueue",

    "network-type": "leafspine",    
    "link-bandwidth": link_bandwidth,
    "ft-server-per-rack": 8,
    "ft-rack-per-pod": 4,
    "ft-agg-per-pod": 4,
    "ft-core-count": core_count,
    "ft-pod-count": 4,
    "ft-server-tor-link-capacity-mult": 1,
    "ft-tor-agg-link-capacity-mult": 1,
    "ft-agg-core-link-capacity-mult": 1,
    
    
    "lb-scheme": "readfile",
    "load-metric": "utilization",
    "shuffle-device-map": do_shuffle,
    "shuffle-map-file": shuffle_path,
    "workers-dir": workers_dir,
    "regret-mode": "all", 
}

baseline_lb_schemes = [
    "futureload",
    "random",
    "roundrobin",
    "powerof2",
    "powerof3",
    "powerof4",
    "robinhood",
    "sita-e",
    "leastloaded",
]

load_metric_map = {
    "futureload": "utilization",
    "leastloaded": "flowsize",
    "powerof2": "flowsize",
    "powerof3": "flowsize",
    "powerof4": "flowsize",
    "random": "flowcount",
    "robinhood": "utilization",
    "roundrobin": "flowsize",
    "sita-e": "utilization",
}


# runtime stats
round_best_times = []
round_avg_times = [] 
round_median_times = []
round_top_score_avg_rank = []
round_permute_avg_rank = []
round_crossover_avg_rank = []
round_random_avg_rank = []
tweak_tolerance_counter = 0
no_improvement_ref = 10e12
best_so_far = 10e12


###########################################################
#################      helpers        #####################
###########################################################

def update_step_size(step_size):
    options["step-size"] = step_size
    options["core-status-profiling-interval"] = int(max(step_size,1))

def reset_runtime_stats(): 
    global round_best_times
    global round_avg_times
    global round_median_times
    global round_top_score_avg_rank
    global round_permute_avg_rank
    global round_crossover_avg_rank
    global round_random_avg_rank
    global tweak_tolerance_counter
    global no_improvement_ref
    global best_so_far
    
    round_best_times = []
    round_avg_times = [] 
    round_median_times = []
    round_top_score_avg_rank = []
    round_permute_avg_rank = []
    round_crossover_avg_rank = []
    round_random_avg_rank = []
    tweak_tolerance_counter = 0
    no_improvement_ref = 10e12
    best_so_far = 10e12
    
    
def dump_globals(file_path):
    with open(ga_run_dir + file_path, "w") as f:
        pprint("globals", stream=f)
        pprint(globals(), stream=f)
        
        
def get_banner(text, color):
    if color == "red":
        return "\033[91m" + text + "\033[0m"
    elif color == "green":
        return "\033[92m" + text + "\033[0m"
    elif color == "yellow":
        return "\033[93m" + text + "\033[0m"
    elif color == "blue":
        return "\033[94m" + text + "\033[0m"
    elif color == "purple":
        return "\033[95m" + text + "\033[0m"


def make_baseline_decisions(rep_count = 2): 
    os.system("mkdir -p {}".format(ga_baselines_dir))
    
    jobs = []    
    cmds = [] 
    for i, lb_scheme in enumerate(baseline_lb_schemes):
        job_options = options.copy()
        job_options["worker-id"] = i
        job_options["lb-scheme"] = lb_scheme
        job_options["load-metric"] = load_metric_map[lb_scheme]
        job_options["rep-count"] = rep_count
        job_options["file-log-level"] = 3
        job_options["regret-mode"] = "none" 
        
        cmd = make_cmd(executable, job_options)
        cmds.append(cmd)
        
        jobs.append(subprocess.Popen(cmd, 
                                     stdout=subprocess.PIPE, 
                                     stderr=subprocess.STDOUT, 
                                     shell=True, 
                                     preexec_fn=os.setsid))

    print("running the baseline methods ", end="")
    for job in jobs:
        job.wait()
        print(".", end="")
        sys.stdout.flush()
    print(" done")
    
    results = {}
    
    for i, job in enumerate(jobs):
        # save the lb decisions file 
        source_path = "{}/worker-{}/run-{}/lb-decisions.txt".format(workers_dir, i, rep_count)
        dest_path = ga_baselines_dir + "{}.txt".format(i)        
        os.system("cp {} {}".format(source_path, dest_path))        

        # get the timing stats 
        try:
            psim_time_stats = get_psim_time(job.stdout)
        except Exception as e:
            print("running cmd: ", cmds[i])
            print("error getting psim time stats for baseline {}:".format(baseline_lb_schemes[i]))
            print(e)
            exit(0)
            
        avg_psim_time = psim_time_stats["avg"]
        print("baseline {} avg psim time: {}".format(baseline_lb_schemes[i], avg_psim_time))
        results[baseline_lb_schemes[i]] = psim_time_stats
        
    return results    
        
                
    

def decisions_file_to_map(file_path):
    # line format: bluh... flow {} core {} crit T/F
    decisions = {}
    
    with open(file_path, "r") as f:
        for line in f:
            if not lb_decisions_file_regex.match(line):
                continue
            
            flow = int(line.split("flow ")[1].split(" ")[0])
            core = int(line.split("core ")[1].split(" ")[0])
            crit_str = line.split("crit ")[1].split(" ")[0].strip()
            crit = True if crit_str == "true" else False
            
            decisions[flow] = {"core": core, 
                               "crit": crit}
    
    return decisions

def decisions_deep_copy(decisions):
    new_decisions = {}
    for flow, flow_info in decisions.items():
        new_decisions[flow] = flow_info.copy()
    return new_decisions

def decisions_map_to_file(decisions, file_path):
    with open(file_path, "w") as f:
        for flow, flow_info in decisions.items():
            f.write("flow {} core {}\n".format(flow, flow_info["core"]))


def regrets_map_to_file(regrets_map, file_path):
    with open(file_path, "w") as f:
        for flow, flow_info in regrets_map.items():
            f.write("flow {} old core {} new core {} regret score {}\n".format(
                flow, flow_info["old-core"], flow_info["new-core"], flow_info["score"]))
    
# either change the core of each flow to a different random core, 
# or change the core of all permutated flows to the same random core
def genetic_permute(decisions_map, 
                    num_permutations=1, 
                    permute_method="change"):
    
    new_decisions = decisions_deep_copy(decisions_map)
    keys = list(decisions_map.keys()) 
    
    if permute_method == "change":
        for i in range(num_permutations):
            flow = np.random.choice(keys)    
            new_core = np.random.randint(0, core_count)
            new_decisions[flow] = {"core": new_core, 
                                   "crit": False}
    elif permute_method == "clump":
        new_core = np.random.randint(0, core_count)
        for i in range(num_permutations):
            flow = np.random.choice(keys)    
            new_decisions[flow] = {"core": new_core, 
                                   "crit": False}
            
    return new_decisions


def parse_regret_files(regret_file_paths_list):
    regret_map = {} 
    # [19:19:49.369] [critical] flow 12125 old core 3 new core 1 regret score 296

    for file_path in regret_file_paths_list:
        # if path doesn't exist, just skip it
        if not os.path.exists(file_path):
            continue
        
        with open(file_path, "r") as f:
            for line in f:
                if not regret_file_regex.match(line):
                    continue
                
                flow = int(line.split("flow ")[1].split(" ")[0])
                old_core = int(line.split("old core ")[1].split(" ")[0])
                new_core = int(line.split("new core ")[1].split(" ")[0])
                regret_score = int(line.split("regret score ")[1].strip())
                
                if flow in regret_map:
                    if regret_map[flow]["score"] < regret_score:
                        regret_map[flow] = {"old-core": old_core, 
                                            "new-core": new_core, 
                                            "score": regret_score}  
                else: 
                    regret_map[flow] = {"old-core": old_core, 
                                        "new-core": new_core, 
                                        "score": regret_score}             

    return regret_map


def genetic_permute_regret(decisions_map, regret_map, permute_count = 1): 
    new_decisions = decisions_deep_copy(decisions_map)
    
    actual_permute_count = min(permute_count, len(regret_map))
    
    for i in range(actual_permute_count):
        flow = np.random.choice(list(regret_map.keys()))

        regret_info = regret_map[flow]
        
        if decisions_map[flow]["core"] != regret_info["old-core"]:
            print("probable error in regret file, flow {} core {} to {}".format(
                flow, new_decisions[flow]["core"], regret_info["old-core"]))    
            continue
            
        new_decisions[flow] = {"core": regret_info["new-core"], 
                               "crit": False}

    return new_decisions, actual_permute_count

# keep the same keys, but randomize the values, for all the keys
def genetic_random(decisions_map):
    new_decisions = {}
    
    for flow in decisions_map.keys():
        core = np.random.randint(0, core_count)
        new_decisions[flow] = {"core": core, 
                               "crit": False}
        
    return new_decisions                

# combine the decisions from two maps, in different ways
# random - for each flow, choose one of the two maps
# combine-1-2 - for the first N flows, use map1, for the rest use map2
# combine-2-1 - for the first N flows, use map2, for the rest use map1
def genetic_crossover(decisions_map1, 
                      decisions_map2, 
                      crossover_method="random", 
                      crossover_ratio=0.5):
    
    if crossover_method == "random":
        new_decisions = {}

        for flow in decisions_map1.keys():
            if np.random.random() < crossover_ratio:
                new_decisions[flow] = decisions_map1[flow].copy()
            else:
                new_decisions[flow] = decisions_map2[flow].copy()
                    
        return new_decisions
    
    if crossover_method == "combine-1-2":
        new_decisions = {}
        
        keys = list(decisions_map1.keys())
        
        map1_key_num = int(len(keys) * crossover_ratio)
        
        map1_keys = keys[0:map1_key_num]
        map2_keys = keys[map1_key_num:]
        
        for i in map1_keys:
            new_decisions[i] = decisions_map1[i].copy()
        for i in map2_keys:
            new_decisions[i] = decisions_map2[i].copy()
            
        return new_decisions
    
    if crossover_method == "combine-2-1":
        new_decisions = {}
        
        keys = list(decisions_map1.keys())
        
        map1_key_num = int(len(keys) * crossover_ratio)
        
        map1_keys = keys[map1_key_num:]
        map2_keys = keys[0:map1_key_num]
        
        for i in map1_keys:
            new_decisions[i] = decisions_map1[i].copy()
        for i in map2_keys:
            new_decisions[i] = decisions_map2[i].copy()
            
        return new_decisions
            



# population is a map from worker id to decisions file path
def evaluate_population(population, round_counter=-1): 
    
    # run the jobs in parallel
    jobs = []
    job_options = options.copy()
    
    for i, specimen in population.items():
                
        job_options["worker-id"] = i
        job_options["lb-decisions-file"] = specimen["path"]
        
        cmd = make_cmd(executable, job_options)
        job = subprocess.Popen(cmd, 
                               stdout=subprocess.PIPE, 
                               stderr=subprocess.STDOUT, 
                               shell=True, 
                               preexec_fn=os.setsid)
        jobs.append(job)
    
    print("evaluating population ", end="")
    sys.stdout.flush()
    for job in jobs:
        job.wait()
        print(".", end="")
        sys.stdout.flush()
    print(" done")
    
    results = {}
    for i, job in enumerate(jobs):
        worker_run_dir = workers_dir + "/worker-{}/run-1".format(i)
        lb_decisions_output_path = worker_run_dir + "/lb-decisions.txt"
        
        regrets_paths = []
        for j in range(options["rep-count"]):
            worker_run_dir = workers_dir + "/worker-{}/run-{}".format(i, j + 1)
            regrets_paths.append(worker_run_dir + "/regrets.txt")
        
        regrets_map = parse_regret_files(regrets_paths)
        
        if ("regrets-output-path" in population[i] and 
            population[i]["regrets-output-path"] is not None):
            
            regrets_map_to_file(regrets_map, population[i]["regrets-output-path"])
        
        results[i] = {}
        results[i].update(population[i])
        results[i].update({
            "time": get_psim_time(job.stdout), 
            "lb-output-output-path": lb_decisions_output_path,
            "regrets": regrets_map,
        })    
        
    sorted_results = sorted(results.items(), 
                            key=lambda kv: kv[1]["time"][sorting_metric])
    
    print_results(round_counter, sorted_results)
    
    return sorted_results



def make_initial_population():
    make_baseline_decisions() 
    baseline_count = len(baseline_lb_schemes)
    
    population = {}
    initial_decisions = decisions_file_to_map(ga_baselines_dir + "/0.txt")
    
    for i in range(population_size):
        worker_decisions_path = ga_rounds_dir + "/0/{}.txt".format(i)
        regrets_output_path = ga_rounds_dir + "/0/{}-regrets.txt".format(i)
        
        if i < baseline_count:
            baseline_path = ga_baselines_dir + "/{}.txt".format(i)
            baseline_decisions = decisions_file_to_map(baseline_path)
            decisions_map_to_file(baseline_decisions, worker_decisions_path)

            population[i] = {"path" : worker_decisions_path, 
                             "regrets-output-path": regrets_output_path,
                             "decisions": baseline_decisions,
                             "method": "baseline",
                             "banner" : get_banner(baseline_lb_schemes[i], "green")}
            
        else: 
            worker_decisions = genetic_random(initial_decisions)
            decisions_map_to_file(worker_decisions, worker_decisions_path)
            
            population[i] = {"path" : worker_decisions_path,
                             "regrets-output-path": regrets_output_path,
                             "decisions": worker_decisions,
                             "method": "random",
                             "banner" : get_banner("random", "red")}
            
    return population


def make_next_population_item(i, new_round_dir, sorted_results, method_limits):
    keep_limit = int(population_size * method_limits[0])
    permute_limit = int(population_size * method_limits[1])
    crossover_limit = int(population_size * method_limits[2])
    
    decisions_path = "{}/{}.txt".format(new_round_dir, i)
    regrets_output_path = "{}/{}-regrets.txt".format(new_round_dir, i)
    decisions = {} 
    method = None
    banner = None
    
    if i < keep_limit:
        method = "topscore"

        prev_decisions = sorted_results[i][1]["decisions"]
        
        if i == 0:
            banner = get_banner("topscore_0", "green")
            decisions = decisions_deep_copy(prev_decisions)
        else: 
            # perm_cnt = np.random.randint(1, (permutation_count_high // 10) + 1) 
            # banner_text = "topscore_{}_{}".format(i, perm_cnt)
            # banner = get_banner(banner_text, "green")        
            # decisions = genetic_permute(prev_decisions, perm_cnt, "change")
            
            # perm_cnt = np.random.randint(1, permutation_count_high) 
            
            if np.random.random() < 0.5:
                perm_cnt = np.random.randint(1, permutation_count_high)
                regrets_map = sorted_results[i][1]["regrets"]
                decisions, regret_cnt = genetic_permute_regret(prev_decisions, 
                                                               regrets_map, perm_cnt)
                banner_text = "topscore_{}_regret_{}".format(i, regret_cnt)
                banner = get_banner(banner_text, "green")
            else: 
                # perm_cnt = 1 
                perm_cnt = np.random.randint(permutation_count_low, permutation_count_high) 
                decisions = genetic_permute(prev_decisions, perm_cnt, "change")
                banner_text = "topscore_{}_permute_{}".format(i, perm_cnt)
                banner = get_banner(banner_text, "green")
                
    elif i < permute_limit:
        
        perm_src = np.random.randint(0, keep_limit)
        perm_cnt = np.random.randint(permutation_count_low, 
                                     permutation_count_high)
        perm_method = np.random.choice(["change", "clump"])

        method = "permute"
        banner_text = "permute_{}_{}_{}".format(perm_src, 
                                                perm_cnt, 
                                                perm_method)
        banner = get_banner(banner_text, "yellow")
        
        src_decisions = sorted_results[perm_src][1]["decisions"]
        decisions = genetic_permute(src_decisions, perm_cnt, perm_method)
        
        
    elif i < crossover_limit:
        crossover_source1 = np.random.randint(0, keep_limit)
        crossover_source2 = np.random.randint(0, keep_limit)
        while crossover_source2 == crossover_source1:
            crossover_source2 = np.random.randint(0, keep_limit)
            
        crossover_ratio = np.random.random()
        crossover_method = np.random.choice(["combine-1-2", "combine-2-1"])
        
        method = "crossover"
        crossover_ratio_int = int(round(crossover_ratio, 2) * 100)
        banner_text = "crossover_{}_{}_{}_{}".format(crossover_source1,
                                                     crossover_source2,
                                                     crossover_ratio_int,
                                                     crossover_method)
        banner = get_banner(banner_text, "purple")

        worker_decisions1 = sorted_results[crossover_source1][1]["decisions"]
        worker_decisions2 = sorted_results[crossover_source2][1]["decisions"]
        
        decisions = genetic_crossover(worker_decisions1,
                                      worker_decisions2,
                                      crossover_method, 
                                      crossover_ratio)
        
    else:
        method = "random"
        banner = get_banner("random", "red")
        
        initial_decisions = decisions_file_to_map(ga_baselines_dir + "0.txt")
        worker_decisions = genetic_random(initial_decisions)
        
        decisions = worker_decisions

    # write the decisions to a file
    decisions_map_to_file(decisions, decisions_path)
    
    return {"banner": banner,
            "method": method, 
            "decisions": decisions,
            "path": decisions_path, 
            "regrets-output-path": regrets_output_path}



keep_ratio_memory = 0.25 

def get_population_bounds(): 
    global keep_ratio_memory       

    last_round_best = round_best_times[-1]
    last_round_median = round_median_times[-1]
    last_round_avg = round_avg_times[-1]
    
    best_avg_diff = last_round_avg - last_round_best  
    median_avg_diff = last_round_avg - last_round_median
    
    if best_avg_diff == 0:
        new_keep_ratio = 0.5
    else:    
        new_keep_ratio = (1 - max(median_avg_diff / best_avg_diff, 0)) * 0.5 + (1 / population_size)
    
    keep_ratio_memory = (keep_ratio_memory * 0.75) + (new_keep_ratio * 0.25)
    permute_ratio = keep_ratio_memory + 0.2
    crossover_ratio = permute_ratio + 0.2
    
    return [keep_ratio_memory, permute_ratio, crossover_ratio]

def make_next_population(new_round_dir, sorted_results):
    next_population = {}
    
    bounds = get_population_bounds()

    print("tweak_tolerance_counter: {}, using {}".format(tweak_tolerance_counter, bounds)) 
    print("making next population ", end="")

    for i in range(population_size):
        next_population[i] = make_next_population_item(i, new_round_dir, sorted_results, bounds)
        print(".", end="")
        sys.stdout.flush()
    print(" done")
    
    return next_population
    

def plot_runtime_stats(): 
    plt.plot(round_best_times, label="best")    
    plt.plot(round_avg_times, label="avg")
    plt.plot(round_median_times, label="median")
    plt.legend()
    plt.savefig("{}/ga-times-{}.png".format(ga_run_dir, base_step_size))
    plt.clf()
    
    plt.plot(round_top_score_avg_rank, label="topscore avg rank")
    plt.plot(round_permute_avg_rank, label="permute avg rank")
    plt.plot(round_crossover_avg_rank, label="crossover avg rank")
    plt.plot(round_random_avg_rank, label="random avg rank")
    plt.legend()
    plt.savefig("{}/ga-ranks-{}.png".format(ga_run_dir, base_step_size))
    plt.clf()
    
def update_stats(round_counter, sorted_results): 
    
    round_best = sorted_results[0][1]["time"][sorting_metric]   
    round_best_times.append(round_best)
    
    round_avg = np.mean([result[1]["time"][sorting_metric] for result in sorted_results])
    round_avg_times.append(round_avg)
    
    round_median = np.median([result[1]["time"][sorting_metric] for result in sorted_results])
    round_median_times.append(round_median)
    
    
    if round_counter != 0:    
        topscore_ranks_sum = 0
        permute_ranks_sum = 0
        crossover_ranks_sum = 0
        random_ranks_sum = 0
        topscore_count = 1
        permute_count = 1
        crossover_count = 1
        random_count = 1
        
        for i, result in enumerate(sorted_results):
            if result[1]["method"] == "topscore":
                topscore_ranks_sum += i
                topscore_count += 1
            if result[1]["method"] == "permute":
                permute_ranks_sum += i
                permute_count += 1
            if result[1]["method"] == "crossover":
                crossover_ranks_sum += i
                crossover_count += 1
            if result[1]["method"] == "random":
                random_ranks_sum += i
                random_count += 1
        
        
        round_top_score_avg_rank.append(topscore_ranks_sum / topscore_count)
        round_permute_avg_rank.append(permute_ranks_sum / permute_count)
        round_crossover_avg_rank.append(crossover_ranks_sum / crossover_count)
        round_random_avg_rank.append(random_ranks_sum / random_count)
            
                        
    # if round_counter % 10 == 0 and round_counter != 0:
    #     plot_runtime_stats()
        
    if round_counter != 0: 
        plot_runtime_stats()


def handle_stepsize_halving(round_counter, current_population): 
    global base_step_size
    global permutation_count_high
    global permutation_count_low
    
    base_step_size //= 2
    permutation_count_high //= 2  
    permutation_count_low //= 2  
    
    update_step_size(base_step_size)
    reset_runtime_stats()
    
    # for the new step size, make the baseline decisions again 
    # then change the last items in the population to the 
    # new baseline decisions generated for the new step size
    new_population = current_population.copy()
    
    make_baseline_decisions()

    replacing_index_start = population_size - len(baseline_lb_schemes)
    for i in range(replacing_index_start, population_size): 
        baseline_index = i - replacing_index_start
        baseline_name = baseline_lb_schemes[baseline_index] 
        
        baseline_path = ga_baselines_dir + "/{}.txt".format(baseline_index)
        worker_decisions_path = ga_rounds_dir + "/{}/{}.txt".format(round_counter, i)
        baseline_decisions = decisions_file_to_map(baseline_path)
        decisions_map_to_file(baseline_decisions, worker_decisions_path)

        new_population[i] = {"path" : worker_decisions_path, 
                             "regrets-output-path": None,
                             "decisions": baseline_decisions,
                             "method": "baseline",
                             "banner" : get_banner(baseline_name, "green")}
    
    return new_population
    
    

def handle_stepsize_tweaking_no_improvement(): 
    if len(round_best_times) == 0:
        return
    
    global tweak_tolerance_counter
    global no_improvement_ref    
    global best_so_far
    
    # if there were no improvements compared to the reference for a while,
    # change the step size a bit. 
    last_round_best = round_best_times[-1]
    
    is_improvement = (last_round_best < no_improvement_ref)
    tweak_tolerance_counter += (0 if is_improvement else 1)
    
    if is_improvement:
        no_improvement_ref = last_round_best
    
    if tweak_tolerance_counter > 0: 
        print("no improvement seen for {} rounds compared to ref {}, best so far: {}".format(
              tweak_tolerance_counter, no_improvement_ref, best_so_far))
        
    if tweak_tolerance_counter >= tweak_tolerance_limit:
        new_step_size = options["step-size"]
        while new_step_size == options["step-size"]:
            change = np.random.choice([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
            new_step_size = base_step_size + change
            
        update_step_size(new_step_size)
        tweak_tolerance_counter = 0
        no_improvement_ref = 10e12
        
        print("no improvement seen for {} rounds, changing step size to {}".format(
              tweak_tolerance_counter, new_step_size))

    if last_round_best < best_so_far:
        no_improvement_ref = last_round_best
        tweak_tolerance_counter = 0
        best_so_far = last_round_best
    
    
    
# if the median of the population is getting too close to the best,
# then it means that the population is converging, so we should
# change something to make it diverge again. 

# if the median remains close to the best for tweak_tolerance_limit rounds,
# then change the step size a bit.
def handle_stepsize_tweaking_with_median():
    if len(round_best_times) == 0:
        return

    global tweak_tolerance_counter
    
    last_round_best = round_best_times[-1]
    last_round_median = round_median_times[-1]
    last_round_avg = round_avg_times[-1]
    
    best_avg_diff = last_round_avg - last_round_best  
    median_avg_diff = last_round_avg - last_round_median
    
    print("best_avg_diff: {}, median_avg_diff: {}".format(best_avg_diff, median_avg_diff))
    
    if median_avg_diff > best_avg_diff * 0.65:
        tweak_tolerance_counter += 1
        
        print("median has been close to the best for {}/{} rounds".format(
              tweak_tolerance_counter, tweak_tolerance_limit)) 
        
        # if tweak_tolerance_counter >= tweak_tolerance_limit: 
        #     # if the step size is the base step size, then change it a bit
        #     # otherwise, reset it to the base step size
        #     if options["step-size"] == base_step_size:
        #         change = np.random.choice([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
        #         new_step_size = base_step_size + change
        #     else: 
        #         new_step_size = base_step_size
                            
        #     update_step_size(new_step_size)
        #     print("step size changed to {}".format(new_step_size))

        #     tweak_tolerance_counter = 0
                
    else: 
        tweak_tolerance_counter = 0
                
        
    

def print_results(round_counter, sorted_results):
    print("round {} evaluated with step size {}, results:".format(
        round_counter, options["step-size"]))
    
    for i, result in enumerate(sorted_results):
        print("rank: {:3d} time: [{:>7.2f} .. {:>7.2f} .. {:>7.2f}] prev_rank: {:3d} method: {:>15}".format(
              i, 
              result[1]["time"]["min"], 
              result[1]["time"]["avg"], 
              result[1]["time"]["max"], 
              result[0], 
              result[1]["banner"]))
    
        
def genetic_algorithm():
    print("starting genetic algorithm")
    os.system("mkdir -p {}/0".format(ga_rounds_dir))
    
    print("initializing population")
    population = make_initial_population()

    for round_counter in range(ga_rounds):
        # evaluate the current population
        if round_counter > 0: 
            if round_counter % halving_rounds_interval == 0: 
                population = handle_stepsize_halving(round_counter, population)
            else:
                # handle_stepsize_tweaking()
                handle_stepsize_tweaking_with_median()
                # pass 
            
        sorted_results = evaluate_population(population, round_counter)
        update_stats(round_counter, sorted_results)
        
        # prepare the next round
        prev_round_dir = ga_rounds_dir + "{}".format(round_counter - 1)
        new_round_dir = ga_rounds_dir + "{}".format(round_counter + 1)
        os.system("mkdir -p {}".format(new_round_dir))  
        population = make_next_population(new_round_dir, sorted_results)
        
        # delete the old round
        os.system("rm -rf {}".format(prev_round_dir))


def signal_handler(sig, frame):
    print("\n\nsaving stats\n\n")
    plot_runtime_stats()


# extremely bad code, just to get the plot done
# this is the worst code I've ever written. 
# just kidding, I've written worse.

def plot_compare_ga_with_baselines():
    
    final_results = []
    
    update_step_size(final_step_size)
    options["rep-count"] = 2  # futureload needs at least 2 reps to work
                              # the others are not affected by the rep count
                              # we get the "last" time from the second rep
    
    for rep in range(rep_count):
        round_results = []
        
        # Step 1: online baseline results
        online_baseline_results = make_baseline_decisions(rep_count=2)
        for key, value in online_baseline_results.items():
            online_key = key + "_online"
            round_results.append((online_key, value))


        # Step 2: offline baseline results

        # Step 2.1: make a population with the baseline decisions
        offline_population = {}
        offline_schemes = baseline_lb_schemes.copy() 
        offline_schemes.append("genetic")
        
        for j, offline_scheme in enumerate(offline_schemes):
            if offline_scheme == "genetic":
                # the best decisions from the last round
                # TODO: probably should keep the best decisions from all rounds
                worker_decisions_path = ga_rounds_dir + "/{}/0.txt".format(ga_rounds)
            else:
                # the baseline decisions, generated in the online phase
                worker_decisions_path = ga_baselines_dir + "/{}.txt".format(j)
            
            decisions = decisions_file_to_map(worker_decisions_path)
            
            offline_population[j] = {"path" : worker_decisions_path, 
                                     "regrets-output-path": None, 
                                     "decisions": decisions,
                                     "method": offline_scheme,
                                     "banner" : get_banner(offline_scheme, "green")}
        
        
        # Step 2.2: evaluate the population
        pop_results = evaluate_population(offline_population)
                
        # Step 2.3: add the results to the round results
        for _, result in enumerate(pop_results):
            offline_key = result[1]["method"] + "_offline"
            round_results.append((offline_key, result[1]["time"]))
            
            
        # Step 2.4: print the results    
        pprint(round_results)

        # Step 2.5: sort the results by key alphabetically, 
        # but the genetic_offline key should be the first
        sorted_results = sorted(round_results,
                                key=lambda kv: kv[0])
        sorted_results = sorted(sorted_results,
                                key=lambda kv: kv[0] != "genetic_offline")
        
        pprint(sorted_results)
        
        

        for j, result in enumerate(sorted_results): 
            scheme_key = result[0]
            scheme_times = result[1]
            
            if rep == 0: 
                final_results.append({"key": scheme_key,
                                      "min": scheme_times["last"],
                                      "max": scheme_times["last"]})
            else:
                final_results[j]["min"] = min(final_results[j]["min"], scheme_times["last"])
                final_results[j]["max"] = max(final_results[j]["max"], scheme_times["last"])
            

    print("final results")
    pprint(final_results)

    labels = []
    mins = []
    maxs = []

    for result in final_results:
        labels.append(result["key"])
        mins.append(result["min"])
        maxs.append(result["max"])
        

    # x range with some distance between every group of two bars
    x = [-3] 
    for i in range(int(len(labels) / 2) ):
        x.append(i * 4)
        x.append(i * 4 + 1)
    width = 1
    
    fig, ax = plt.subplots()
    ax.bar(x, maxs, width, 
                    label='max', edgecolor="black", linewidth=1, color = "white")
    ax.bar(x, mins, width, label='min', edgecolor="black", linewidth=1)
    
    ax.set_ylabel('time')
    ax.set_title('time by lb scheme')

    #rotate the xtick labels 90 degrees
    plt.xticks(rotation=90)
    ax.set_xticks(x)
    ax.set_xticklabels(labels)    
    
    plt_path = ga_run_dir + "baseline.png"
    plt.savefig(plt_path, bbox_inches='tight', dpi=300)
        


###########################################################
#################      setup        #######################
###########################################################
dump_globals("globals.txt")
signal.signal(signal.SIGQUIT, signal_handler) 
build_exec(executable, base_executable, build_path, run_path)
make_shuffle(128, shuffle_path)
set_memory_limit(10 * 1e9)


###########################################################
#################      run          #######################
###########################################################
genetic_algorithm()       
plot_compare_ga_with_baselines()


###########################################################
#################      clean        #######################
###########################################################
os.system("mv {} {}".format(executable, ga_run_dir))