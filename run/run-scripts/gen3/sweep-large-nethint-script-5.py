from utils.util import *
from utils.sweep_large_nethint_base import *
from utils.sweep_base import ConfigSweeper

experiment_seed = 75
random_rep_count = 1

viz = False
sim_length = 10000
seed_range = 3
placement_options = 100
farid_rounds = 30 


# this is an experiment that has one base setting, and then a bunch of comparisons.
# I'm attempting to keep the things that should be kept constant, constant.
# then compute the speedup of the other things with respect to the base setting.
def do_experiment(placement_mode="random", 
                  machine_count=6,
                  oversub=1):
    random.seed(experiment_seed)

    # things to eventually remove from this function. 
    placement_modes = [placement_mode]

    # choose one of the settings to run the experiments with.     
    selected_setting = { 
        "machine-count": machine_count,
        "ft-server-per-rack": 6,
        "jobs-machine-count-low": 6,
        "jobs-machine-count-high": 6,
        "placement-seed-range": seed_range,
        "comm-size": [16000],
        "comp-size": [200],
        "layer-count": [1],
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

    punish_oversubscribed_min_values = [1]  
    # profiled_throttle_factors = [1.0, 0.66, 0.5, 0.33]
    profiled_throttle_factors = [1.0, 0.60]
    
    placement_seeds = list(range(1, selected_setting["placement-seed-range"] + 1))
    core_count = base_options["ft-server-per-rack"] // oversub
    
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

        # each placement strategy will be a different plot, drawn next to each other.
        "placement-mode": placement_modes, 
        
        # some dynamic parameters.                             
        "ft-core-count": [core_count],
        "placement-seed": placement_seeds,
        
        # parameters for the scheduling algorithm. 
        "routing-fit-strategy": ["first"],
        "compat-score-mode": ["time-no-coll"], # ["under-cap", "time-no-coll", "max-util-left"], 
        "throttle-search": [True, False],
        
        "farid-rounds": [1, farid_rounds], 
        
        "punish-oversubscribed-min" : punish_oversubscribed_min_values,
    } 
    
    comparison_base = {
        "timing-scheme": "zero", 
        "ring-mode": "random",  
        "lb-scheme": "random", 
        "subflows": 1, 
        "throttle-search": False, 
        "farid-rounds": farid_rounds, 
        "punish-oversubscribed": True, 
        "punish-oversubscribed-min": 1, 
        "min-rate": 10
    }

    comparisons = []

    for i in punish_oversubscribed_min_values:
        comparisons.append(("zero-{}-perfect".format(i), 
                            {"timing-scheme": "zero", 
                             "lb-scheme": "perfect", 
                             "subflows": 1, 
                             "punish-oversubscribed": True, 
                             "punish-oversubscribed-min": i})) 
             
    for i in punish_oversubscribed_min_values:
        comparisons.append(("random-{}-perfect".format(i), 
                            {"timing-scheme": "random", 
                             "lb-scheme": "perfect", 
                             "subflows": 1, 
                             "punish-oversubscribed": True, 
                             "punish-oversubscribed-min": i}))  
        
    for i in punish_oversubscribed_min_values:
        comparisons.append(("random-{}-routed-subf".format(i), 
                            {"timing-scheme": "random", 
                             "lb-scheme": "readprotocol", 
                             "subflows": core_count, 
                             "punish-oversubscribed": True, 
                             "punish-oversubscribed-min": i}))  
    
    for i in punish_oversubscribed_min_values:
        comparisons.append(("farid-{}-routed".format(i),
                            {"timing-scheme": "farid", 
                             "farid-rounds": farid_rounds, 
                             "lb-scheme": "readprotocol", 
                             "throttle-search": True, 
                             "subflows": 1, 
                             "punish-oversubscribed": True,
                             "punish-oversubscribed-min": i})) 
    
    for i in punish_oversubscribed_min_values:
        comparisons.append(("farid-{}-routed-subf".format(i),
                            {"timing-scheme": "farid", 
                             "farid-rounds": farid_rounds, 
                             "lb-scheme": "readprotocol", 
                             "throttle-search": True, 
                             "subflows": core_count, 
                             "punish-oversubscribed": True,
                             "punish-oversubscribed-min": i})) 
            
    for i in punish_oversubscribed_min_values:
        comparisons.append(("farid-{}-random".format(i),
                            {"timing-scheme": "farid", 
                             "farid-rounds": farid_rounds, 
                             "lb-scheme": "random", 
                             "throttle-search": True, 
                             "subflows": 1, 
                             "punish-oversubscribed": True,
                             "punish-oversubscribed-min": i}))  
        
    for i in punish_oversubscribed_min_values:
        comparisons.append(("farid-{}-random-subf".format(i),
                            {"timing-scheme": "farid", 
                             "farid-rounds": farid_rounds, 
                             "lb-scheme": "random", 
                             "throttle-search": True, 
                             "subflows": core_count, 
                             "punish-oversubscribed": True,
                             "punish-oversubscribed-min": i}))  
    
    for i in punish_oversubscribed_min_values:
        comparisons.append(("farid-{}-perfect".format(i),
                            {"timing-scheme": "farid", 
                             "farid-rounds": farid_rounds, 
                             "lb-scheme": "perfect", 
                             "throttle-search": True, 
                             "subflows": 1, 
                             "punish-oversubscribed": True,
                             "punish-oversubscribed-min": i}))
          
    for i in punish_oversubscribed_min_values:
        comparisons.append(("random-{}-random".format(i),
                            {"timing-scheme": "random", 
                             "lb-scheme": "random", 
                             "subflows": core_count, 
                             "punish-oversubscribed": True,
                             "punish-oversubscribed-min": i}))  
        
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
            "link-solution-candidate-count": 50,
            "link-solution-random-quantum": 10,
            "link-solution-top-candidates": 3,    
            "overall-solution-candidate-count": 4,
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
        level_names = ["machine_count", "placement", "oversub", "metric", "placement_mode", "comparison", "stat"]
        machine_counts = [12, 18]
        oversubs = [1, 2]
        placement_modes = ["semirandom_2", "semirandom_4"]  
        
        all_results = {} 
        
        for oversub in oversubs:    
            for placement_mode in placement_modes: 
                for machine_count in machine_counts: 
                    # run the config.
                    summary = do_experiment(placement_mode=placement_mode, 
                                            machine_count=machine_count,
                                            oversub=oversub)
                    
                    if machine_count not in all_results:
                        all_results[machine_count] = {}
                    
                    if placement_mode not in all_results[machine_count]:
                        all_results[machine_count][placement_mode] = {}
                    
                    if oversub not in all_results[machine_count][placement_mode]:
                        all_results[machine_count][placement_mode][oversub] = summary
                    
                    flat_results = flatten_dict(all_results, level_names=level_names)
                    
                    # save to csv.
                    os.makedirs(f"results/exps/{exp_number}", exist_ok=True)
                    flat_results_df = pd.DataFrame(flat_results)    
                    path = f"results/exps/{exp_number}/results.csv" 
                    flat_results_df.to_csv(path, index=False)
    

    # parser.add_argument("--file_name", type=str, required=True)
    # parser.add_argument("--plot_params", type=str, required=False)
    # parser.add_argument("--subplot_x_params", type=str, required=False)
    # parser.add_argument("--subplot_y_params", type=str, required=False)
    # parser.add_argument("--subplot_hue_params", type=str, required=False)
    # parser.add_argument("--plot_x_params", type=str, required=False)
    # parser.add_argument("--plot_y_param", type=str, required=False)
    
    path = f"results/exps/{exp_number}/results.csv" 
    
    plot_command = "python3 plot_compare.py \
        --file_name {} \
        --plot_params metric \
        --subplot_x_params placement \
        --subplot_y_params oversub \
        --subplot_hue_params comparison \
        --plot_x_params machine_count \
        --plot_y_param value".format(path)
            
    print("running the plot command: ") 
    print(plot_command) 
      
    os.system(plot_command)
    
        
    