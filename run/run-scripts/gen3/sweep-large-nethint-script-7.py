from utils.util import *
from utils.sweep_large_nethint_base import *
from utils.sweep_base import ConfigSweeper
import itertools

experiment_seed = 76
random_rep_count = 1


# this is an experiment that has one base setting, and then a bunch of comparisons.
# I'm attempting to keep the things that should be kept constant, constant.
# then compute the speedup of the other things with respect to the base setting.
def do_experiment(placement_mode="random", 
                  machine_count=6,
                  oversub=1, 
                  sim_length=50000, 
                  punish_oversubscribed_min=0.5, 
                  search_quota="a little"):
    
    viz = False
    seed_range = 6
    placement_options = 100
    
    cassini_mc_candidate_count = {
        "little": 10,
        "some": 50,
        "alot": 200,
    }[search_quota]
    
    cassini_overall_solution_count = {  
        "little": 5,
        "some": 10, 
        "alot": 15,
    }[search_quota]

    random.seed(experiment_seed)

    # things to eventually remove from this function. 
    placement_modes = [placement_mode]

    # choose one of the settings to run the experiments with.     
    selected_setting = { 
        "machine-count": machine_count,
        "ft-server-per-rack": 8,
        "jobs-machine-count-low": 4,
        "jobs-machine-count-high": 8,
        "placement-seed-range": seed_range,
        "comm-size": [16000, 32000],
        "comp-size": [100, 200, 300],
        "layer-count": [1, 2],
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
        
        "punish-oversubscribed-min": punish_oversubscribed_min,   
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
    if core_count == 2: 
        profiled_throttle_factors = [1.0, 0.5]
    elif core_count == 3: 
        profiled_throttle_factors = [1.0, 0.66, 0.33]
    elif core_count == 4:
        profiled_throttle_factors = [1.0, 0.75, 0.5, 0.25]
            
    placement_seeds = list(range(1, selected_setting["placement-seed-range"] + 1))
    
    timing_viz = []
    if viz: 
        timing_viz = placement_seeds
    else:
        timing_viz = []  
        
    exp_sweep_config = {
        "protocol-file-name": ["nethint-test"],

        # placement and workload parameters.
        # these will be different lines in the cdf plot.
        "lb-scheme": ["random", "leastloaded", "ideal", "perfect", "readprotocol"], #[lb, "ideal", "leastloaded"],   
        "timing-scheme": ["cassini", "farid", "random", "zero"],    #["cassini", "farid", "random"],
        "ring-mode": ["random"],
        "subflows": [1, core_count],    
        "min-rate": [10, 100],  
        "punish-oversubscribed": [False, True],   

        # some dynamic parameters.                             
        "ft-core-count": [core_count],
        "placement-seed": placement_seeds,
        
        # parameters for the scheduling algorithm. 
        "routing-fit-strategy": ["first", "best", "random", "useall"],
        "compat-score-mode": ["time-no-coll"], # ["under-cap", "time-no-coll", "max-util-left"], 
        "throttle-search": [True, False],
        
        "farid-rounds": [1, 2, 3, 4, 5, 20],         
    } 
    
    comparison_base = {
        "timing-scheme": "zero", 
        "ring-mode": "random",  
        "lb-scheme": "random", 
        "subflows": 1, 
        "throttle-search": False, 
        "farid-rounds": 1, 
        "punish-oversubscribed": True, 
        "routing-fit-strategy": "first",    
        "min-rate": 100
    }

    comparisons = []

    for rounds in [1, 2, 3, 4, 5]:                               
        # comparisons.append(("farid-routed-{}-rounds".format(rounds),
        #                     {"timing-scheme": "farid", 
        #                     "farid-rounds": rounds, 
        #                     "lb-scheme": "readprotocol",
        #                     "routing-fit-strategy": "first",    
        #                     "throttle-search": True, 
        #                     "subflows": core_count, 
        #                     "punish-oversubscribed": True,
        #                 }))
        
        comparisons.append(("farid-perfect-{}-rounds".format(rounds),
                                {"timing-scheme": "farid", 
                                "farid-rounds": rounds, 
                                "lb-scheme": "perfect",
                                "throttle-search": True, 
                                "punish-oversubscribed": True,}
                            ))  
          
    comparisons.append(("zero-perfect",
                        {"timing-scheme": "zero", 
                            "lb-scheme": "perfect", 
                            "subflows": 1, 
                            "punish-oversubscribed": True}))
        
    # to be give to the CS, which will be used to populate the run_context.
    # the run_context will be then handed back to the custom functions. 
    # am I making this too complicated? I think I am.
    exp_context = {
        "sim-length": sim_length,

        "plot-iteration-graphs": False, 
        "visualize-timing": timing_viz, 
        "visualize-routing": False, 
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
    
    sane, reason = check_comparison_sanity(exp_context, exp_sweep_config)

    if not sane:
        print("Comparison sanity check failed.")
        input("Press Enter to continue...") 
        
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
        worker_thread_count=42, 
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

        os.system("rm -f last-exp-results") 
        os.system("ln -s {} {}".format(exp_dir, "last-exp-results"))

        exp_config = [
            ("machine_count", [24, 48]),
            ("placement_mode", ["random"]),
            ("oversub", [2, 4]),
            ("sim_length", [15000]),
            ("punish_oversubscribed_min", [1.0]),  
            ("search_quota", ["little", "some", "alot"]) 
        ]

        all_results = [] 
        
        # go through all the possible combinations.
        keys, values = zip(*(dict(exp_config)).items())
        permutations_dicts = [dict(zip(keys, v)) for v in itertools.product(*values)]
        
        for perm in permutations_dicts:
            print("Running experiment with settings: ", perm)
            summary = do_experiment(**perm) 
            
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
        --subplot_x_params machine_count \
        --subplot_y_params oversub \
        --subplot_hue_params comparison \
        --plot_x_params search_quota \
        --plot_y_param values".format(path)
            
    print("running the plot command: ") 
    print(plot_command) 
      
    os.system(plot_command)
    
        
    