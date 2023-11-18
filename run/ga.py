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
use_gdb = False
worker_count = 30

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

    "step-size": 10,
    "core-status-profiling-interval": 10,
    "rep-count": 10, 
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
    "ft-core-count": 4,
    "ft-pod-count": 4,
    "ft-server-tor-link-capacity-mult": 1,
    "ft-tor-agg-link-capacity-mult": 1,
    "ft-agg-core-link-capacity-mult": 1,
    
    
    "lb-scheme": "readfile",
    "load-metric": "utilization",
    "shuffle-device-map": True,
    # "shuffle-map-file": input_dir + "/shuffle/shuffle-map.txt",
    "shuffle-map-file": shuffle_path,
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
    

core_count = options["ft-core-count"]

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

baselines_path = "lb-decisions/baselines/"

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
    
    
def get_avg_psim_time(job_output): 
    psim_times = []
    
    for line in iter(job_output.readline, b''):
        output = line.decode("utf-8")
        if "psim time:" in output:
            psim_time = float(output.split("psim time:")[1])
            psim_times.append(psim_time)
    
    avg_psim_time = np.mean(psim_times)
    return avg_psim_time
        

def make_baseline_decisions(): 
    rep_count = 2 

    jobs = []    
    for i, lb_scheme in enumerate(baseline_lb_schemes):
        job_options = options.copy()
        job_options["worker-id"] = i
        job_options["lb-scheme"] = lb_scheme
        job_options["load-metric"] = load_metric_map[lb_scheme]
        job_options["rep-count"] = rep_count
        job_options["step-size"] = 10
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
            
    for i, job in enumerate(jobs):
        avg_psim_time = get_avg_psim_time(job.stdout)
        print("baseline {} avg psim time: {}".format(baseline_lb_schemes[i], avg_psim_time))
        source_path = "workers/worker-{}/run-{}/lb-decisions.txt".format(i, rep_count)
        dest_path = "lb-decisions/baselines/{}.txt".format(i)        
        os.system("cp {} {}".format(source_path, dest_path))        
        
    

def decisions_file_to_map(file_path):
    # line format: bluh... flow {} core {}
    decisions = {}
    regex = r".*flow \d+ core \d+"
    
    with open(file_path, "r") as f:
        for line in f:
            if not re.match(regex, line):
                continue
            
            flow = int(line.split("flow ")[1].split(" ")[0])
            core = int(line.split("core ")[1].split(" ")[0])
            decisions[flow] = core
            
    return decisions


def decisions_map_to_file(decisions, file_path):
    with open(file_path, "w") as f:
        for flow, core in decisions.items():
            f.write("flow {} core {}\n".format(flow, core))


def genetic_permute(decisions_map, num_permutations=1):
    new_decisions = decisions_map.copy()
        
    for i in range(num_permutations):
        flow = np.random.choice(list(decisions_map.keys()))
        current_core = decisions_map[flow]
        new_core = np.random.randint(0, core_count)
            
        attempts = 0 
        # find another flow that has the same core as new_core
        for other_flow, other_core in decisions_map.items():
            attempts += 1
            if attempts > 100:
                break
            
            if other_core == new_core and other_flow != flow:
                new_decisions[other_flow] = current_core
                break
            
        new_decisions[flow] = new_core
    
    return new_decisions


# keep the same keys, but randomize the values, for all the keys
def genetic_random(decisions_map):
    new_decisions = decisions_map.copy()
    
    for flow in decisions_map.keys():
        core_count = options["ft-core-count"]
        core = np.random.randint(0, core_count)
        new_decisions[flow] = core
        
    return new_decisions                

def genetic_crossover(decisions_map1, 
                      decisions_map2, 
                      crossover_method="random", 
                      crossover_ratio=0.5):
    
    if crossover_method == "random":
        new_decisions = decisions_map1.copy()

        for flow in decisions_map1.keys():
            if np.random.random() < crossover_ratio:
                new_decisions[flow] = decisions_map2[flow]
                    
        return new_decisions
    
    if crossover_method == "combine-1-2":
        new_decisions = decisions_map1.copy()

        num_keys = len(decisions_map1.keys())
        keys = list(decisions_map1.keys())

        changed_keys = num_keys * crossover_ratio
        for i in range(int(changed_keys)):
            new_decisions[keys[i]] = decisions_map2[keys[i]]
            
        return new_decisions
    
    if crossover_method == "combine-2-1":
        new_decisions = decisions_map2.copy()
        
        num_keys = len(decisions_map2.keys())
        keys = list(decisions_map2.keys())
        
        changed_keys = num_keys * crossover_ratio
        for i in range(int(changed_keys)):
            new_decisions[keys[i]] = decisions_map1[keys[i]]
        
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

        jobs.append(subprocess.Popen(cmd, 
                                     stdout=subprocess.PIPE, 
                                     stderr=subprocess.STDOUT, 
                                     shell=True, 
                                     preexec_fn=os.setsid))
    
    print("evaluating population ", end="")
    sys.stdout.flush()
    for job in jobs:
        job.wait()
        print(".", end="")
        sys.stdout.flush()
    print(" done")
    
    results = {}
    for i, job in enumerate(jobs):
        results[i] = {
            "time": get_avg_psim_time(job.stdout), 
            "decisions": population[i]["path"], 
            "method": population[i]["method"], 
            "banner": population[i]["banner"]
        } 
    
    return results



def make_initial_population():
    population = {}
    
    baseline_count = len(baseline_lb_schemes)
    
    for i in range(worker_count):
        worker_decisions_path = "lb-decisions/round_0/decisions-{}.txt".format(i)
        
        if i < baseline_count:
            baseline_path = "lb-decisions/baselines/{}.txt".format(i)
            baseline_decisions = decisions_file_to_map(baseline_path)
            decisions_map_to_file(baseline_decisions, worker_decisions_path)

            population[i] = {"path" : worker_decisions_path, 
                             "method": "baseline",
                             "banner" : "\033[92m" + "baseline" + "\033[0m"}
            
        else: 
            initial_decisions = decisions_file_to_map("lb-decisions/baselines/0.txt")
            worker_decisions = genetic_random(initial_decisions)
            decisions_map_to_file(worker_decisions, worker_decisions_path)
            
            population[i] = {"path" : worker_decisions_path, 
                            "method": "random",
                            "banner" : "\033[91m" + "random" + "\033[0m"}
            
    return population


def make_next_population_item(i, new_round_dir, sorted_results):
    keep_limit = int(worker_count * 0.3)
    permute_limit = int(worker_count * 0.6)
    crossover_limit = int(worker_count * 0.9)
    
    new_path = "{}/decisions-{}.txt".format(new_round_dir, i)
    method = "" 
    
    if i < keep_limit:
        method = "topscore"
        banner = "\033[92m" + "topscore" + "\033[0m"
        
        worker_decisions_path = sorted_results[i][1]["decisions"]
        worker_decisions = decisions_file_to_map(worker_decisions_path)
        # keep the best one as is, permute the rest a lillte bit
        worker_decisions = genetic_permute(worker_decisions, i)
        decisions_map_to_file(worker_decisions, new_path)
        
    elif i < permute_limit:
        method = "permute"
        banner = "\033[94m" + "permute" + "\033[0m"
        
        permutation_source = np.random.randint(0, permute_limit)
        permutation_count = np.random.randint(0, 10)
        
        worker_decisions_path = sorted_results[permutation_source][1]["decisions"]
        worker_decisions = decisions_file_to_map(worker_decisions_path)
        new_worker_decisions = genetic_permute(worker_decisions, permutation_count)
        decisions_map_to_file(new_worker_decisions, new_path)
        
        
        
    elif i < crossover_limit:
        method = "crossover"
        banner = "\033[93m" + "crossover" + "\033[0m"
        
        crossover_source1 = np.random.randint(0, keep_limit)
        crossover_source2 = np.random.randint(0, worker_count)
        crossover_ratio = np.random.random()
        crossover_method = np.random.choice(["combine-1-2", "combine-2-1"])
        
        worker_decisions_path1 = sorted_results[crossover_source1][1]["decisions"]
        worker_decisions_path2 = sorted_results[crossover_source2][1]["decisions"]
        
        worker_decisions1 = decisions_file_to_map(worker_decisions_path1)
        worker_decisions2 = decisions_file_to_map(worker_decisions_path2)
        
        new_worker_decisions = genetic_crossover(worker_decisions1,
                                                 worker_decisions2,
                                                 crossover_method, 
                                                 crossover_ratio)
        
        decisions_map_to_file(new_worker_decisions, new_path)
        
    else:
        method = "random"
        banner = "\033[91m" + "random" + "\033[0m"
        
        initial_decisions = decisions_file_to_map("lb-decisions/baselines/0.txt")
        worker_decisions = genetic_random(initial_decisions)
        decisions_map_to_file(worker_decisions, new_path)

    return {"path": new_path, 
            "banner": banner,
            "method": method} 


def make_next_population(new_round_dir, sorted_results):
    print("making next population ", end="")
    next_population = {}
    for i in range(worker_count):
        next_population[i] = make_next_population_item(i, new_round_dir, sorted_results)
        print(".", end="")
        sys.stdout.flush()
    print(" done")
    return next_population
    



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
        
    
def update_options(round_counter): 
    global no_improvement_counter
    
    if round_counter > 2 and round_best_times[-1] >= round_best_times[-2]:
        no_improvement_counter += 1
    else:
        no_improvement_counter = 0
    
    if no_improvement_counter > 10:
        # choose a number between -1 or 1 
        step_size_change = np.random.randint(-1, 2)
        options["step-size"] += step_size_change 
        
        if options["step-size"] < 5:
            options["step-size"] = 5
        if options["step-size"] > 15:
            options["step-size"] = 15
            
        print("no improvement for 10 rounds, changing step size to {}".format(options["step-size"]))
        no_improvement_counter = 0

    # else:
    #     options["step-size"] = 10
    return 


def print_results(round_counter, sorted_results):
    print("round {} evaluated, results:".format(round_counter))
    for i, result in enumerate(sorted_results):
        print("rank: {:3d} time: {:6.2f} prev_rank: {:3d} method: {:>15}".format(
              i, result[1]["time"], 
              result[0], result[1]["banner"]))
    
        
def genetic_algorithm():
    num_rounds = 2000
    
    clean_up()
    make_baseline_decisions() 
    os.system("mkdir -p lb-decisions/round_0")
    population = make_initial_population()

    for round_counter in range(num_rounds):
        # evaluate the current population
        update_options(round_counter)
        results = evaluate_population(population)
        sorted_results = sorted(results.items(), key=lambda kv: kv[1]["time"])
        update_stats(round_counter, sorted_results)
        
        # print the results
        print_results(round_counter, sorted_results)
        
        
        # prepare the next round
        new_round_dir = "lb-decisions/round_{}".format(round_counter + 1)
        os.system("mkdir -p {}".format(new_round_dir))  
        population = make_next_population(new_round_dir, sorted_results)
        
        # delete the old round
        os.system("rm -rf lb-decisions/round_{}".format(round_counter))

genetic_algorithm()

os.system("rm {}".format(executable))