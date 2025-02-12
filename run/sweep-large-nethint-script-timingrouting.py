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
                  machine_count=6,
                  rack_size=4,
                  oversub=1, 
                  job_count=1, 
                  sim_length=50000, 
                  punish_oversubscribed_min=0.5, 
                  search_quota="a little", 
                  inflate=1.0, 
                  ring_mode="letitbe"): 
    
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
        "jobs-machine-count-low": machine_count // job_count,
        "jobs-machine-count-high": machine_count // job_count,
        "placement-seed-range": seed_range,
        "comm-size": list(range(1600, 10000, 200)),
        "comp-size": list(range(50, 150, 10)), 
        "layer-count": list(range(1, 2)),   
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

    core_count = base_options["ft-server-per-rack"] // oversub

    # profiled_throttle_factors = [1.0, 0.66, 0.5, 0.33]
    profiled_throttle_factors = [1.0, 0.75, 0.5, 0.25]
    
    # if core_count == 2: 
        # profiled_throttle_factors = [1.0, 0.5]
    # elif core_count == 3: 
    #     profiled_throttle_factors = [1.0, 0.66, 0.33]
    # elif core_count == 4:
    #     profiled_throttle_factors = [1.0, 0.75, 0.5, 0.25]
            
    placement_seeds = list(range(1, selected_setting["placement-seed-range"] + 1))
    
    exp_sweep_config = {
        "placement-seed": placement_seeds,
    } 
    
    comparison_base = {
        "punish-oversubscribed": True, 
        "min-rate": 100,
        "punish-oversubscribed-min": punish_oversubscribed_min,   

        "timing-scheme": "zero", 
        "compat-score-mode": "time-no-coll",
        "throttle-search": False, 
        "farid-rounds": 30, 
        
        "lb-scheme": "random", 
        "routing-fit-strategy": "best",    
        "subflows": 1,
        "inflate": inflate,   
        "protocol-file-name": "nethint-test",
        "ft-core-count": core_count,
    }

    comparisons = []
    
    comparisons.append(("faridv2-no-throt-perfect",
                        {"timing-scheme": "faridv2",
                         "lb-scheme": "perfect"}))
    
    for timing in ["faridv2", "faridv3"]:
        for subflow_count in [1, 2, 4]:  
            comparisons.append(("{}-graph-coloring-v5-sub-{}".format(timing, subflow_count),   
                                {"timing-scheme": timing,
                                "throttle-search": True,   
                                "routing-fit-strategy": "graph-coloring-v5",  
                                "subflows": subflow_count,     
                                "lb-scheme": "readprotocol"}))
        
    comparisons.append(("faridv2-throt-perfect",
                        {"timing-scheme": "faridv2",
                         "throttle-search": True,
                         "lb-scheme": "perfect"}))
    
    comparisons.append(("zero-perfect",
                        {"timing-scheme": "zero",
                         "lb-scheme": "perfect"}))
    
    comparisons.append(("zero-leastloaded",
                        {"timing-scheme": "zero",
                         "lb-scheme": "leastloaded"}))

    
    # to be give to the CS, which will be used to populate the run_context.
    # the run_context will be then handed back to the custom functions. 
    # am I making this too complicated? I think I am.
    exp_context = {
        "sim-length": sim_length,

        "plot-iteration-graphs": False, 
        "visualize-timing": placement_seeds if plot_stuff else [],
        "visualize-routing": plot_stuff,
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
        worker_thread_count=40, 
        plot_cdfs=False,
    )
    
    summary = cs.sweep()
    return summary 
    
    
    
# Here, we iterate over things that will have different baselines to compare against.   
# the idea is that eventually, one plot should be generate for each of these setting combinations.   
if __name__ == "__main__":
    exp_number = None 

    if exp_number is None:
        exp_number = get_incremented_number()
        exp_dir = f"results/exps/{exp_number}"
        path = f"results/exps/{exp_number}/results.csv" 
        os.makedirs(f"results/exps/{exp_number}", exist_ok=True)

        os.system("rm -f last-exp-results-link-*") 
        os.system("ln -s {} {}".format(exp_dir, "last-exp-results-link-{}".format(exp_number)))

        plot_stuff = False
        seed_range = 10
        
        exp_config = [
            ("sim_length", [10000]),
            
            ("machine_count", [48]),
            ("job_count", [4]),
            ("rack_size", [8]),
            
            ("placement_mode", ["random", "semirandom_4", "semirandom_2"]), 
            ("ring_mode", ["random", "letitbe"]), 
            ("oversub", [2]),
            
            ("punish_oversubscribed_min", [1.0]), 
            ("search_quota", ["alot"]), 
            ("inflate", [1.0, 1.1]),    
        ]

        all_results = [] 
        
        # go through all the possible combinations.
        keys, values = zip(*(dict(exp_config)).items())
        permutations_dicts = [dict(zip(keys, v)) for v in itertools.product(*values)]
        
        for perm in permutations_dicts:
            print("Running experiment with settings: ", perm)
            summary = do_experiment(plot_stuff=plot_stuff, 
                                    seed_range=seed_range, 
                                    **perm) 
            
            for summary_item in summary:    
                all_results.append({**summary_item, **perm})
            
            all_results_df = pd.DataFrame(all_results)    
            all_results_df.to_csv(path, index=False)
    
    # parser.add_argument("--file_name", type=str, required=True)
    # parser.add_argument("--plot_params", type=str, required=False)
    # parser.add_argument("--subplot_x_params", type=str, required=False)
    # parser.add_argument("--subplot_y_params", type=str, required=False)
    # parser.add_argument("--subplot_hue_params", type=str, required=False)
    # parser.add_argument("--plot_x_params", type=str, required=False)
    # parser.add_argument("--plot_y_param", type=str, required=False)
    
    exp_dir = f"results/exps/{exp_number}"
    path = f"{exp_dir}/results.csv"     

    plot_command = "python3 plot_compare.py \
        --file_name {} \
        --plot_params metric \
        --subplot_y_params placement_mode \
        --subplot_x_params ring_mode \
        --subplot_hue_params comparison \
        --plot_x_params inflate \
        --plot_y_param values".format(path)
            
    print("running the plot command: ") 
    print(plot_command) 
      
    os.system(plot_command)
    
        
    