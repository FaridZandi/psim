import os
from pprint import pprint 
import subprocess
import numpy as np
import sys 
import resource
from util import make_shuffle
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import matplotlib.pyplot as plt
import re
import signal 

run_id = os.popen("date +%s | sha256sum | base64 | head -c 8").read()

# setting up the basic paths
# read the env for the base dir 
base_dir = os.environ.get("PSIM_BASE_DIR")
print("base_dir:", base_dir)
build_path = base_dir + "/build"
run_path = base_dir + "/run/"
base_executable = build_path + "/psim"
executable = build_path + "/psim-" + run_id
input_dir = base_dir + "/input/"
shuffle_path  = input_dir + "/shuffle/shuffle-{}.txt".format(run_id)
workers_dir = run_path + "/workers/"
use_gdb = False


population_size = 30 
base_step_size = 10
core_count = 4
ga_rounds = 2000
rep_count = 20

baselines_path = "lb-decisions/baselines/"
line_regex = re.compile(r".*flow \d+ core \d+ crit (true|false)")


# build the executable, exit if build fails
os.chdir(build_path)
# run the make -j command, get the exit code
exit_code = os.system("make -j")
if exit_code != 0:
    print("make failed, exiting")
    sys.exit(1)
os.chdir(run_path)
os.system("cp {} {}".format(base_executable, executable))

make_shuffle(128, shuffle_path)


# get the parameters from the command line
params = {}
for i, arg in enumerate(sys.argv):
    if i == 0:
        continue
    p = arg.split("=")
    key = p[0][2:]
    if len(p) == 1:
        val = True
    else:
        val = p[1]
        if val == "true":
            val = True
        if val == "false":
            val = False
    params[key] = val
    
if "gdb" in params:
    use_gdb = True
    del params["gdb"]

options = {
    "protocol-file-dir": base_dir + "/input/128search-dpstart-2",
    "protocol-file-name": "candle128-simtime.txt",
    # "protocol-file-name": "transformer128-simtime+compute.txt",

    "step-size": base_step_size,
    "core-status-profiling-interval": 10,
    "rep-count": rep_count, 
    "console-log-level": 4,
    "file-log-level": 3,
    
    "initial-rate": 100,
    "min-rate": 10,
    "priority-allocator": "fairshare", #"priorityqueue",

    "network-type": "leafspine",    
    "link-bandwidth": 100,
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
    "shuffle-device-map": True,
    # "shuffle-map-file": input_dir + "/shuffle/shuffle-map.txt",
    # "shuffle-map-file": shuffle_path,
    "shuffle-map-file": run_path + "/round_416/shuffle-NmRjMzQ3.txt",
}

options.update(params)

baseline_lb_schemes = [
    # "futureload",
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


memory_limit_kb = 10 * 1e9
memory_limit_kb = int(memory_limit_kb)
resource.setrlimit(resource.RLIMIT_AS, (memory_limit_kb, memory_limit_kb))

round_best_times = []
round_avg_times = [] 
round_median_times = []

round_top_score_avg_rank = []
round_permute_avg_rank = []
round_crossover_avg_rank = []
round_random_avg_rank = []

no_improvement_counter = 0


def clean_up(): 
    os.system("rm -rf lb-decisions/*")
    
def make_cmd(executable, options):
    cmd = executable
    for option in options.items():
        if option[1] is False:
            continue
        elif option[1] is True:
            cmd += " --" + option[0]
        else: 
            cmd += " --" + option[0] + "=" + str(option[1])

    if use_gdb:
        cmd = "gdb -ex run --args " + cmd

    return cmd
    
    
def get_psim_time(job_output): 
    psim_times = []
    
    for line in iter(job_output.readline, b''):
        output = line.decode("utf-8")
        if "psim time:" in output:
            psim_time = float(output.split("psim time:")[1])
            psim_times.append(psim_time)
    
    result = {
        "avg": np.mean(psim_times), 
        "max": np.max(psim_times),
        "min": np.min(psim_times),
        "median": np.median(psim_times), 
    } 
    
    return result
        

def make_baseline_decisions(rep_count = 2): 

    jobs = []    
    for i, lb_scheme in enumerate(baseline_lb_schemes):
        job_options = options.copy()
        job_options["worker-id"] = i
        job_options["lb-scheme"] = lb_scheme
        job_options["load-metric"] = load_metric_map[lb_scheme]
        job_options["rep-count"] = rep_count
        job_options["file-log-level"] = 3
        
        cmd = make_cmd(executable, job_options)
        
        jobs.append(subprocess.Popen(cmd, 
                                     stdout=subprocess.PIPE, 
                                     stderr=subprocess.STDOUT, 
                                     shell=True, 
                                     preexec_fn=os.setsid))

    print("running the baseline method ", end="")
    for job in jobs:
        job.wait()
        print(".", end="")
        sys.stdout.flush()
    print(" done")

    os.system("mkdir -p {}".format(baselines_path))
    
    results = {}
    
    for i, job in enumerate(jobs):
        # save the lb decisions file 
        source_path = "{}/worker-{}/run-{}/lb-decisions.txt".format(workers_dir, i, rep_count)
        dest_path = "lb-decisions/baselines/{}.txt".format(i)        
        os.system("cp {} {}".format(source_path, dest_path))        

        # get the timing stats 
        psim_time_stats = get_psim_time(job.stdout)
        avg_psim_time = psim_time_stats["avg"]
        print("baseline {} avg psim time: {}".format(baseline_lb_schemes[i], avg_psim_time))
        results[baseline_lb_schemes[i]] = psim_time_stats
        
    return results    
        
                
    

def decisions_file_to_map(file_path):
    # line format: bluh... flow {} core {} crit T/F
    decisions = {}
    
    with open(file_path, "r") as f:
        for line in f:
            if not line_regex.match(line):
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


def genetic_permute(decisions_map, num_permutations=1):
    new_decisions = decisions_deep_copy(decisions_map)
    
    keys = list(decisions_map.keys()) 
    
    for i in range(num_permutations):
        flow = np.random.choice(keys)    
        new_core = np.random.randint(0, core_count)
        new_decisions[flow] = {"core": new_core, 
                               "crit": False}
    
    return new_decisions


def genetic_permute_crit(decisions_map, num_permutations=1):
    new_decisions = decisions_deep_copy(decisions_map)
    
    keys = list(decisions_map.keys()) 
    
    for i in range(num_permutations):
        # find a critical flow 
        attempts = 0
        while True:
            flow = np.random.choice(keys)    
            if decisions_map[flow]["crit"]:
                break
            attempts += 1
            if attempts > 100:
                break
        
        new_core = np.random.randint(0, core_count)
        new_decisions[flow] = {"core": new_core, 
                               "crit": False}
    
    return new_decisions

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
def evaluate_population(population): 
    
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
        results[i] = {}
        results[i].update(population[i])
        results[i].update({
            "time": get_psim_time(job.stdout), 
            "lb-output-path": workers_dir + "/worker-{}/run-1/lb-decisions.txt".format(i)
        })    
    return results



def make_initial_population():
    make_baseline_decisions() 
    baseline_count = len(baseline_lb_schemes)
    
    population = {}
    initial_decisions = decisions_file_to_map("lb-decisions/baselines/0.txt")
    
    for i in range(population_size):
        worker_decisions_path = "lb-decisions/round_0/decisions-{}.txt".format(i)
        
        if i < baseline_count:
            baseline_path = "lb-decisions/baselines/{}.txt".format(i)
            baseline_decisions = decisions_file_to_map(baseline_path)
            decisions_map_to_file(baseline_decisions, worker_decisions_path)

            population[i] = {"path" : worker_decisions_path, 
                             "decisions": baseline_decisions,
                             "method": "baseline",
                             "banner" : "\033[92m" + "baseline" + "\033[0m"}
            
        else: 
            worker_decisions = genetic_random(initial_decisions)
            decisions_map_to_file(worker_decisions, worker_decisions_path)
            
            population[i] = {"path" : worker_decisions_path, 
                             "decisions": worker_decisions,
                             "method": "random",
                             "banner" : "\033[91m" + "random" + "\033[0m"}
            
    return population


def make_next_population_item(i, new_round_dir, sorted_results):
    keep_limit = int(population_size * 0.3)
    permute_limit = int(population_size * 0.6)
    crossover_limit = int(population_size * 0.9)
    
    decisions_path = "{}/decisions-{}.txt".format(new_round_dir, i)
    decisions = {} 
    method = None
    banner = None
    
    if i < keep_limit:
        method = "topscore"
        banner = "\033[92m" + "topscore_" + str(i) + "\033[0m"

        prev_decisions = sorted_results[i][1]["decisions"]
        decisions = genetic_permute(prev_decisions, i)
        
        
    elif i < permute_limit:
        perm_src = np.random.randint(0, permute_limit)
        perm_cnt = np.random.randint(0, 10)

        method = "permute"
        banner = "\033[94m" + "permute_" + str(perm_src) + "_" + str(perm_cnt) + "\033[0m"
        
        src_decisions = sorted_results[perm_src][1]["decisions"]
        decisions = genetic_permute(src_decisions, perm_cnt)
        
        
    elif i < crossover_limit:
        crossover_source1 = np.random.randint(0, keep_limit)
        crossover_source2 = np.random.randint(0, population_size)
        crossover_ratio = np.random.random()
        crossover_method = np.random.choice(["combine-1-2", "combine-2-1"])
        
        method = "crossover"
        banner = "\033[93m" + "crossover_" + str(crossover_source1) + "_" + str(crossover_source2) + "\033[0m"
        
        worker_decisions1 = sorted_results[crossover_source1][1]["decisions"]
        worker_decisions2 = sorted_results[crossover_source2][1]["decisions"]
        
        decisions = genetic_crossover(worker_decisions1,
                                      worker_decisions2,
                                      crossover_method, 
                                      crossover_ratio)
        
    else:
        method = "random"
        banner = "\033[91m" + "random" + "\033[0m"
        
        initial_decisions = decisions_file_to_map("lb-decisions/baselines/0.txt")
        worker_decisions = genetic_random(initial_decisions)
        
        decisions = worker_decisions

    # write the decisions to a file
    decisions_map_to_file(decisions, decisions_path)
    
    return {"banner": banner,
            "method": method, 
            "decisions": decisions,
            "path": decisions_path} 


def make_next_population(new_round_dir, sorted_results):
    print("making next population ", end="")
    next_population = {}
    for i in range(population_size):
        next_population[i] = make_next_population_item(i, new_round_dir, sorted_results)
        print(".", end="")
        sys.stdout.flush()
    print(" done")
    return next_population
    

def plot_stats(): 
    plt.plot(round_best_times, label="best")    
    plt.plot(round_avg_times, label="avg")
    plt.plot(round_median_times, label="median")
    plt.legend()
    plt.savefig("ga.png")
    plt.clf()
    
    plt.plot(round_top_score_avg_rank, label="topscore avg rank")
    plt.plot(round_permute_avg_rank, label="permute avg rank")
    plt.plot(round_crossover_avg_rank, label="crossover avg rank")
    plt.plot(round_random_avg_rank, label="random avg rank")
    plt.legend()
    plt.savefig("ga-ranks.png")
    plt.clf()
    
def update_stats(round_counter, sorted_results): 
    
    round_best = sorted_results[0][1]["time"]   
    round_best_times.append(round_best)
    
    round_avg = np.mean([result[1]["time"] for result in sorted_results])
    round_avg_times.append(round_avg)
    
    round_median = np.median([result[1]["time"] for result in sorted_results])
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
            
                        
    if round_counter % 10 == 0 and round_counter != 0:
        plot_stats()
        
    
def update_options(round_counter): 
    global no_improvement_counter
    
    if round_counter > 2 and round_best_times[-1] >= round_best_times[-2]:
        no_improvement_counter += 1
    else:
        no_improvement_counter = 0
    
    if no_improvement_counter > 10:
        step_size_change = np.random.randint(-1, 2) # -1, 0, or 1
        options["step-size"] = base_step_size + step_size_change 
            
        print("no improvement for 10 rounds, changing step size to {}".format(options["step-size"]))
        no_improvement_counter = 0

    return 


def print_results(round_counter, sorted_results):
    print("round {} evaluated, results:".format(round_counter))
    for i, result in enumerate(sorted_results):
        print("rank: {:3d} time: {:6.2f} prev_rank: {:3d} method: {:>15}".format(
              i, result[1]["time"], 
              result[0], result[1]["banner"]))
    
        
def genetic_algorithm():
    clean_up()
    os.system("mkdir -p lb-decisions/round_0")
    population = make_initial_population()

    for round_counter in range(ga_rounds):
        # evaluate the current population
        update_options(round_counter)
        results = evaluate_population(population)
        sorted_results = sorted(results.items(), key=lambda kv: kv[1]["time"]["avg"])
        update_stats(round_counter, sorted_results)
        
        # print the results
        print_results(round_counter, sorted_results)
        
        # prepare the next round
        new_round_dir = "lb-decisions/round_{}".format(round_counter + 1)
        os.system("mkdir -p {}".format(new_round_dir))  
        population = make_next_population(new_round_dir, sorted_results)
        
        # delete the old round
        os.system("rm -rf lb-decisions/round_{}".format(round_counter))


def signal_handler(sig, frame):
    print("saving stats")
    plot_stats()
    
signal.signal(signal.SIGQUIT, signal_handler)


genetic_algorithm()


###########################################################
###########################################################
###########################################################
# some experimental stuff


def evaluate_stuff():
    clean_up() 

    results = make_baseline_decisions(rep_count=rep_count)
    
    # add online to every key in the results 
    keys = list(results.keys()) 
    for key in keys:
        online_key = key + "_online"
        results[online_key] = results[key]
        del results[key]
        
    # make a population with the baseline decisions
    population = {}
    
    lbs = baseline_lb_schemes.copy() 
    lbs .append("genetic")
    
    for i, lb_scheme in enumerate(lbs):
        if lb_scheme == "genetic":
            worker_decisions_path = "round_{}/decisions-0.txt".format(ga_rounds)
        else:
            worker_decisions_path = "lb-decisions/baselines/{}.txt".format(i)
        
        decisions = decisions_file_to_map(worker_decisions_path)
        
        population[i] = {"path" : worker_decisions_path, 
                         "decisions": decisions,
                         "method": "baseline",
                         "banner" : "\033[92m" + "baseline" + "\033[0m"}
        
    pop_results = evaluate_population(population)
    
    for i, result in pop_results.items():
        print("baseline {} avg psim time: {}".format(lbs[i], result["time"]))
        offline_key = lbs[i] + "_offline"
        results[offline_key] = result["time"]
        
    import matplotlib.pyplot as plt 
    # for each item in the results, plot a bar that shows the min and max 
    
    labels = []
    mins = []
    maxs = []
    avgs = []
    medians = []
    
    # sort the results by the key 
    results = {k: v for k, v in sorted(results.items(), key=lambda item: item[0])}    
    
    for key, value in results.items():
        labels.append(key)
        mins.append(value["min"])
        maxs.append(value["max"])
        avgs.append(value["avg"])
        medians.append(value["median"])    
        
    # x range with some distance between every group of two bars
    x = [-3] 
    for i in range(int(len(labels) / 2) ):
        x.append(i * 4)
        x.append(i * 4 + 1)
    print("x:", x)
    width = 1
    
    fig, ax = plt.subplots()
    rects4 = ax.bar(x, maxs, width, 
                    label='max', edgecolor="black", linewidth=1, color = "white")
    rects1 = ax.bar(x, mins, width, label='min', edgecolor="black", linewidth=1)
    
    ax.set_ylabel('time')
    ax.set_title('time by lb scheme')

    #rotate the xtick labels 90 degrees
    plt.xticks(rotation=90)
    ax.set_xticks(x)
    ax.set_xticklabels(labels)    
    
    plt.savefig("baseline.png", bbox_inches='tight', dpi=300)
        
    pprint(results)
        
    
    
evaluate_stuff()

os.system("rm {}".format(executable))