from utils.util import *
from utils.sweep_large_nethint_base import *
from utils.sweep_base import ConfigSweeper
import itertools

experiment_seed = 76
random_rep_count = 1
THREADS = 42

# this is an experiment that has one base setting, and then a bunch of comparisons.
# I'm attempting to keep the things that should be kept constant, constant.
# then compute the speedup of the other things with respect to the base setting.
def do_experiment(plot_stuff=False,
                  seed_range=1, 
                  placement_mode="random", 
                  machine_count=8,
                  rack_size=4,
                  oversub=1, 
                  job_sizes=(2, 2), 
                  sim_length=50000, 
                  punish_oversubscribed_min=0.5, 
                  min_rate=100, 
                  search_quota="a little", 
                  inflate=1.0, 
                  ring_mode="letitbe", 
                  desired_entropy=0.5,
                  cmmcmp_range=(0, 1),
                  comm_size=(12000, 60000, 6000),
                  comp_size=(200, 1000, 1),
                  layer_count=(1, 2, 1), 
                  fallback_threshold=0.5): 
    
    placement_options = 100
    
    cassini_mc_candidate_count = {
        "little": 10,
        "some": 50,
        "alot": 200,
    }[search_quota]
    
    cassini_overall_solution_count = {  
        "little": 5,
        "some": 10, 
        "alot": 3,
    }[search_quota]

    random.seed(experiment_seed)

    # things to eventually remove from this function. 
    placement_modes = [placement_mode]

    # choose one of the settings to run the experiments with.     
    selected_setting = { 
        "machine-count": machine_count,
        "ft-server-per-rack": rack_size,
        "jobs-machine-count-low": job_sizes[0], 
        "jobs-machine-count-high": job_sizes[1],
        "placement-seed-range": seed_range,
        "cmmcmp-range": cmmcmp_range,   
            
        "comm-size": list(range(comm_size[0], comm_size[1], comm_size[2])),
        "comp-size": list(range(comp_size[0], comp_size[1], comp_size[2])),
        "layer-count": list(range(layer_count[0], layer_count[1], layer_count[2])),
        
        "iter-count": [30], # iteration count
    }
    
    base_options = {
        "step-size": 1,
        "core-status-profiling-interval": 100000,
        "rep-count": 1, 
        "console-log-level": 4,
        "file-log-level": 1,
        
        "initial-rate": 100,
        # "min-rate": 10,
        "drop-chance-multiplier": 1, 
        "rate-increase": 1.1, 
        
        
        "priority-allocator": "maxmin", # "fairshare",
        "punish-oversubscribed": False,
        
        "network-type": "leafspine",    
        "link-bandwidth": 100,
        "ft-rack-per-pod": 1,
        "ft-agg-per-pod": 1,
        "ft-pod-count": -1,
        "ft-server-tor-link-capacity-mult": 1,
        "ft-tor-agg-link-capacity-mult": 1,
        "ft-agg-core-link-capacity-mult": 1,
        
        "shuffle-device-map": False,
        "regret-mode": "none",
        
        "machine-count": selected_setting["machine-count"],
        "ft-server-per-rack": selected_setting["ft-server-per-rack"],

        "simulation-seed": experiment_seed, 
        
        "print-flow-progress-history": True,
        # "export-dot": True,    
        
        "placement-mode": placement_mode,   
        
        
        "ring-mode": ring_mode,  
    }

    interesting_metrics = {
        "avg_ar_time": {
            "avg_cdf_plot": True,   
            "iter_avg_plot": False,  
            "compare_mode": "divide",
            "better": "lower",
            "type": "per_iter",
        },
        "avg_iter_time": {
            "avg_cdf_plot": True,   
            "iter_avg_plot": False,  
            "compare_mode": "divide",
            "better": "lower",
            "type": "per_iter",
        }, 
        "total_time": {
            "avg_cdf_plot": True,   
            "iter_avg_plot": False,  
            "compare_mode": "divide",
            "better": "lower",
            "type": "single_number",
        }, 
        "rolling_iter_time": {
            "avg_cdf_plot": False,   
            "iter_avg_plot": True,  
            "compare_mode": "divide",
            "better": "lower",
            "type": "per_iter",
        }, 
        "rolling_ar_time": {
            "avg_cdf_plot": False,   
            "iter_avg_plot": True,  
            "compare_mode": "divide",
            "better": "lower",
            "type": "per_iter",
        }, 
        "rolling_costs": {
            "avg_cdf_plot": False,   
            "iter_avg_plot": True,  
            "compare_mode": "subtract",
            "better": "lower",
            "type": "per_iter",
        },
        "rolling_ar_plus_cost": {
            "avg_cdf_plot": False,   
            "iter_avg_plot": True,  
            "compare_mode": "divide",
            "better": "lower",
            "type": "per_iter",
        },
        "subflow_ratio": {
            "avg_cdf_plot": True,   
            "iter_avg_plot": False,  
            "compare_mode": "divide",
            "better": "lower",
            "type": "single_number", 
        },
    } 

    core_count = int(base_options["ft-server-per-rack"] // oversub)

    if core_count == 1:
        profiled_throttle_factors = [1.0]
        considered_sub = []
    if core_count == 2: 
        profiled_throttle_factors = [1.0, 0.5]
        considered_sub = [2]     
    if core_count == 4 or core_count == 8:
        profiled_throttle_factors = [1.0, 0.75, 0.5, 0.25]
        considered_sub = [4]
            
    placement_seeds = list(range(1, selected_setting["placement-seed-range"] + 1))
    
    exp_sweep_config = {
        "placement-seed": placement_seeds,
    } 
    
    comparison_base = {
        "punish-oversubscribed": True, 
        "min-rate": min_rate,
        "punish-oversubscribed-min": punish_oversubscribed_min,   

        "timing-scheme": "zero", 
        "compat-score-mode": "time-no-coll",
        "throttle-search": False, 
        "farid-rounds": 6, 
        
        "fallback-threshold": 1e9, 

        "lb-scheme": "random", 
        "routing-fit-strategy": "best",    
        "subflows": 1,
        "inflate": inflate,   
        "protocol-file-name": "nethint-test",
        "ft-core-count": core_count,
    }

    comparisons = []
    
    # comparisons.append(("TS", {
    #                         "timing-scheme": "faridv2",
    #                         "throttle-search": False,
    #                         "lb-scheme": "random"
    #                     }))
    
    comparisons.append(("TS", {
                            "timing-scheme": "faridv2",
                            "throttle-search": True,
                            "lb-scheme": "random"
                        }))
    
    comparisons.append(("RO", {
                            "timing-scheme": "zero",
                            "routing-fit-strategy": "graph-coloring-v3",  
                            "lb-scheme": "readprotocol"
                        }))
    
    for subflow_count in considered_sub:
        comparisons.append((f"TS+SUB", {
                                "timing-scheme": "faridv2",
                                "subflows": subflow_count, 
                                "throttle-search": True,
                                "lb-scheme": "random"
                            }))
    
    # comparisons.append(("RO5", {
    #                         "timing-scheme": "zero",
    #                         "routing-fit-strategy": "graph-coloring-v5",  
    #                         "lb-scheme": "readprotocol"
    #                     }))
    
    for timing in ["faridv2", "faridv4"]:
        for subflow_count in list(set([1] + considered_sub)):
            for coloring in ["graph-coloring-v5"]:
                name = "TS+RO"
                
                if subflow_count > 1:
                    name += f"+SUB"
                    
                if timing == "faridv4":
                    name += "+REP"
                    
                comparisons.append((name, {
                                        "timing-scheme": timing,
                                        "throttle-search": True if subflow_count > 1 else False,   
                                        "routing-fit-strategy": coloring,     
                                        "subflows": subflow_count,     
                                        "fallback-threshold": fallback_threshold, 
                                        "lb-scheme": "readprotocol"
                                    }))
    
    comparisons.append(("Perfect", {
                            "timing-scheme": "zero",
                            "lb-scheme": "perfect"
                        }))

    # to be give to the CS, which will be used to populate the run_context.
    # the run_context will be then handed back to the custom functions. 
    # am I making this too complicated? I think I am.
    exp_context = {
        "sim-length": sim_length,

        "plot-iteration-graphs": False, 
        "plot-initial-timing": False,
        "plot-intermediate-timing": False,
        "plot-final-timing": False,
        "plot-routing-assignment": False, 
        "plot-merged-ranges": False, 
        "plot-runtime-timing": False,
        "plot-link-empty-times": False,
        
        "profiled-throttle-factors": profiled_throttle_factors, 
        
        # other stuff
        "random-rep-count": random_rep_count,
        "interesting-metrics": interesting_metrics,
        "all-placement-modes": placement_modes,
        "experiment-seed": experiment_seed,
        "oversub": oversub,
        
        "cassini-parameters": {  
            "link-solution-candidate-count": cassini_mc_candidate_count,   
            "link-solution-random-quantum": 10,
            "link-solution-top-candidates": 3,    
            "overall-solution-candidate-count": cassini_overall_solution_count,
            "save-profiles": True,
        },
        
        "routing-parameters": {},
        "placement-parameters": {
            "desired-entropy": desired_entropy,
            "placement-seed-limit": placement_options,
        },   
        "selected-setting": selected_setting,
        
        "comparison-base": comparison_base,              
        
        "comparisons": comparisons,
    } 
    
    cs = ConfigSweeper(
        base_options, exp_sweep_config, exp_context,
        run_command_options_modifier, 
        run_results_modifier, 
        custom_save_results_func, 
        result_extractor_function,
        exp_filter_function=exp_filter_function,
        exp_name="nethint_LB+{}_TS+{}_R+{}_{}_{}".format("", "", "",  
                                                            oversub, 
                                                            experiment_seed),
        worker_thread_count=30, 
        plot_cdfs=False,
    )
    
    summary = cs.sweep()
    results_dir = cs.get_results_dir()
    
    return summary, results_dir 
    
    
    
# Here, we iterate over things that will have different baselines to compare against.   
# the idea is that eventually, one plot should be generate for each of these setting combinations.   
if __name__ == "__main__":
    # make a backup of the current state of the repository.
    os.system("./git_backup.sh")
    
    original_exp_number = None
    seed_range = 4
    m = 10
    clean_up_sweep_files = False
    
    if original_exp_number is not None: 
        exp_number = original_exp_number
    else:
        exp_number = get_incremented_number() 
    
    exp_dir = f"results/exps/{exp_number}"
    os.makedirs(exp_dir, exist_ok=True)
    path = f"{exp_dir}/results.csv"     
    plot_commands_path = f"{exp_dir}/results_plot.sh"
    
    for plot_type in []: #["heatmap"]:  
        plot_command = f"python3 plot_compare.py \
                        --file_name {path} \
                        --plot_params metric \
                        --subplot_y_params machine_count \
                        --subplot_x_params comparison \
                        --subplot_hue_params rack_size \
                        --plot_x_params job_sizes \
                        --plot_y_param values \
                        --plot_type {plot_type}"

        with open(plot_commands_path, "a") as f:
            clean_plot_command = plot_command
            while "  " in clean_plot_command:
                clean_plot_command = clean_plot_command.replace("  ", " ") 
            f.write(clean_plot_command + "\n\n")
                        
    for plot_type in ["bar"]:
        plot_command = f"python3 plot_compare.py \
                        --file_name {path} \
                        --plot_params metric \
                        --subplot_y_params job_sizes \
                        --subplot_x_params rack_size \
                        --subplot_hue_params comparison \
                        --plot_x_params oversub \
                        --plot_y_param values \
                        --plot_type {plot_type}"
                    
        with open(plot_commands_path, "a") as f:
            clean_plot_command = plot_command
            while "  " in clean_plot_command:
                clean_plot_command = clean_plot_command.replace("  ", " ") 
            f.write(clean_plot_command + "\n\n")
    
    if original_exp_number is None:
        exp_dir = f"results/exps/{exp_number}"
        path = f"results/exps/{exp_number}/results.csv" 
        os.makedirs(f"results/exps/{exp_number}", exist_ok=True)

        os.system("rm -f last-exp-results-link-*") 
        os.system("ln -s {} {}".format(exp_dir, "last-exp-results-link-{}".format(exp_number)))

        exp_config = [
            ("sim_length", [400 * m]),
            
            ("machine_count", [48]),
            ("rack_size", [8]),
            
            ("job_sizes", [(4, 16)]),

            ("placement_mode", ["entropy"]), 
            ("ring_mode", ["letitbe"]), 
            
            ("desired_entropy", [0.7]),

            ("oversub", [1, 2, 4, 8]),
            # ("oversub", [8, 4, 2, 1]),
            
            ("cmmcmp_range", [(0, 2)]),
  
            # ("cmmcmp_range", [(0.5, 2)]),
            ("fallback_threshold", [0.5]),
            
            ("comm_size", [(120 * m, 360 * m, 60 * m)]),
            ("comp_size", [(2 * m, 10 * m, 1 * m)]),
            ("layer_count", [(1, 2, 1)]),
               
            ("punish_oversubscribed_min", [1]), 
            ("min_rate", [100]),
            ("search_quota", ["alot"]), 
            ("inflate", [1]),    
        ]

        relevant_keys = [key for key, options in exp_config if len(options) > 1]    
        
        all_results = [] 
        
        # go through all the possible combinations.
        keys, values = zip(*(dict(exp_config)).items())
        permutations_dicts = [dict(zip(keys, v)) for v in itertools.product(*values)]
        
        for perm in permutations_dicts:
            print("Running experiment with settings: ", perm)
            
            summary, results_dir = do_experiment(seed_range=seed_range, 
                                                 **perm) 
            
            for summary_item in summary:    
                all_results.append({**summary_item, **perm})
            
            all_results_df = pd.DataFrame(all_results)    
            all_results_df.to_csv(path, index=False)

            perm_key = "_".join([f"{key}_{perm[key]}" for key in relevant_keys])
            # remove the parantheses from the perm_key
            perm_key = perm_key.replace("(", "").replace(")", "").replace(" ", "")
            
            #make a link to the results of this experiment.
            print("results_dir: ", results_dir) 
            
            if clean_up_sweep_files:
                os.system("rm -rf {}".format(results_dir)) 
            else:
                os.system("ln -s {} {}".format(results_dir, f"last-exp-results-link-{exp_number}/last-exp-results-link-{perm_key}"))
                        

    os.system(f"chmod +x {plot_commands_path}")
    os.system(f"cat {plot_commands_path}")
    os.system(f"echo 'running plot commands'")
    os.system(f"./{plot_commands_path}")
    
    
