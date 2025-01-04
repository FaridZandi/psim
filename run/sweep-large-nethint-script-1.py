from utils.util import *
from utils.sweep_large_nethint_base import *
from utils.sweep_base import ConfigSweeper

experiment_seed = 74
random_rep_count = 1

viz = False
sim_length = 10000
seed_range = 20
placement_options = 100

def main():
    # choose one of the settings to run the experiments with.     
    selected_setting = { 
        "machine-count": 18,
        "ft-server-per-rack": 6,
        "jobs-machine-count-low": 3,
        "jobs-machine-count-high": 5,
        "placement-seed-range": seed_range,
        "comm-size": [8000, 4000, 16000],
        "comp-size": [200, 100],
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
        
        "print-flow-progress-history": viz,
        # "export-dot": True,    
    }

    interesting_metrics = ["avg_ar_time", 
                           "avg_iter_time", 
                           "rolling_iter_time", 
                           "rolling_ar_time", 
                           "rolling_costs",
                           "rolling_ar_plus_cost"] 
    
    oversub = 3
    placement_modes = ["random", "compact"]
    
    random.seed(experiment_seed)
    
    # iterating over the different settings. Each setting will create a different experiment. 
    # the results is one plot for each setting.
    
    # to control the ratio of uplinks and downlinks.
    # higher oversub means less uplinks 
    core_count = base_options["ft-server-per-rack"] // oversub
    
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
        "placement-seed": list(range(1, selected_setting["placement-seed-range"] + 1)),
        
        # parameters for the scheduling algorithm. 
        "routing-fit-strategy": ["first"],
        "compat-score-mode": ["time-no-coll"], # ["under-cap", "time-no-coll", "max-util-left"], 
        "throttle-search": [True, False],
        
        "farid-rounds": [1, 3, 5, 10, 15],
    } 
    
    
    # to be give to the CS, which will be used to populate the run_context.
    # the run_context will be then handed back to the custom functions. 
    # am I making this too complicated? I think I am.
    exp_context = {
        "sim-length": sim_length,
        
        "plot-iteration-graphs": False, 
        "visualize-timing": viz, 
        "visualize-routing": False, 
        "profiled-throttle-factors": [1.0, 0.66, 0.33], 
        
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
            "overall-solution-candidate-count": 1,
            "rounds": 10,    
            "save-profiles": True,
        },
        "routing-parameters": {},
        "placement-parameters": {
            "placement-seed-limit": placement_options,
        },   
        "selected-setting": selected_setting,
        
        "comparison-base": {"timing-scheme": "zero", 
                            "ring-mode": "random",  
                            "lb-scheme": "random", 
                            "subflows": 1, 
                            "throttle-search": False, 
                            "farid-rounds": 1, 
                            "punish-oversubscribed": True, 
                            "min-rate": 10},              
        
        "comparisons": [
            ("zero-subf", {"timing-scheme": "zero", "subflows": core_count}), 
            ("zero-routed", {"timing-scheme": "zero", "lb-scheme": "readprotocol"}), 
            # ("farid-10-routed", {"timing-scheme": "farid", "farid-rounds": 10, "lb-scheme": "readprotocol"}), 
            # ("farid-10-throt-routed", {"timing-scheme": "farid", "farid-rounds": 10, "lb-scheme": "readprotocol", "throttle-search": True}), 
            ("farid-10-nothr-routed", {"timing-scheme": "farid", "farid-rounds": 10, "lb-scheme": "readprotocol", "throttle-search": False, "subflows": 1}),     
            ("farid-10-throt-random", {"timing-scheme": "farid", "farid-rounds": 10, "lb-scheme": "random", "throttle-search": True, "subflows": 1}),     
            ("farid-10-throt-routed", {"timing-scheme": "farid", "farid-rounds": 10, "lb-scheme": "readprotocol", "throttle-search": True, "subflows": 1}),     
            ("farid-10-throt-routed-subf", {"timing-scheme": "farid", "farid-rounds": 10, "lb-scheme": "readprotocol", "throttle-search": True, "subflows": core_count}),     
            ("perfectLB", {"lb-scheme": "perfect", "timing-scheme": "zero"}), 
            
            # all the same with min rate 100
            # ("base-GoodNW", {"timing-scheme": "zero", "lb-scheme": "random", "min-rate": 100, "punish-oversubscribed": False}),    
            # ("zero-routed-GoodNW", {"timing-scheme": "zero", "lb-scheme": "readprotocol", "min-rate": 100, "punish-oversubscribed": False}),    
            # ("farid-10-throt-routed-subf-GoodNW", {"timing-scheme": "farid", "farid-rounds": 10, "lb-scheme": "readprotocol", "throttle-search": True, "subflows": core_count, "min-rate": 100, "punish-oversubscribed": False}),
            # ("perfectLB-GoodNW", {"lb-scheme": "perfect", "timing-scheme": "zero", "min-rate": 100, "punish-oversubscribed": False}), 
            
            # ("farid-10-routed-100", {"timing-scheme": "farid", "farid-rounds": 10, "lb-scheme": "readprotocol", "min-rate": 100, "punish-oversubscribed": False}),
            # ("farid-10-throt-routed-100", {"timing-scheme": "farid", "farid-rounds": 10, "lb-scheme": "readprotocol", "throttle-search": True, "min-rate": 100, "punish-oversubscribed": False}),
            # ("farid-1-throt-routed-subf-GoodNW", {"timing-scheme": "farid", "farid-rounds": 1, "lb-scheme": "readprotocol", "throttle-search": True, "subflows": core_count, "min-rate": 100, "punish-oversubscribed": False}),
            # ("farid-5-throt-routed-subf-GoodNW", {"timing-scheme": "farid", "farid-rounds": 5, "lb-scheme": "readprotocol", "throttle-search": True, "subflows": core_count, "min-rate": 100, "punish-oversubscribed": False}),
        ]
    } 
    
    if base_options["print-flow-progress-history"] and not exp_context["visualize-timing"]:
        print("Warning: print-flow-progress-history is enabled, but visualize-timing is not.")
        print("turn off print-flow-progress-history or enable visualize-timing.")
        return
        
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
    )
    
    cs.sweep()

if __name__ == "__main__":
    
    main() 
    