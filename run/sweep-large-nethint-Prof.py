from utils.util import *
from utils.sweep_large_nethint_base import *
from utils.sweep_base import ConfigSweeper

experiment_seed = 58
random_rep_count = 1

def main():
    # choose one of the settings to run the experiments with.     
    selected_setting = nethint_settings[5]
    
    base_options = {
        "step-size": 1,
        "core-status-profiling-interval": 100000,
        "rep-count": 1, 
        "console-log-level": 4,
        "file-log-level": 1,
        
        "initial-rate": 100,
        "min-rate": 100,
        "drop-chance-multiplier": 0, 
        "rate-increase": 1, 
        
        "priority-allocator": "maxmin", # "fairshare",

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
    }

    interesting_metrics = ["avg_ar_time", "avg_iter_time"] # "iter_minus_ar_time", 
    placement_modes = ["random"] #["random", "semirandom_4", "semirandom_2", "compact"]
    # placement_modes = ["random", "compact"] 
    
    oversub = 2
    
    cassini_parameters = {  
        "sim-length": 10000000,
        "link-solution-candidate-count": 3,
        "link-solution-random-quantum": 10,
        "link-solution-top-candidates": 3,    
        "overall-solution-candidate-count": 3,
        "save-profiles": True,
    }    
    
    random.seed(experiment_seed)
    
    # iterating over the different settings. Each setting will create a different experiment. 
    # the results is one plot for each setting.
    for lb in ["random"]:                
        exp_sweep_config = {
            "protocol-file-name": ["nethint-test"],

            # placement and workload parameters.
            # these will be different lines in the cdf plot.
            "lb-scheme": [lb], 
            # "timing-scheme": ["zero", "farid", "random", "inc_100", "inc_200", "inc_400", "inc_500", "cassini"],
            "timing-scheme": ["farid"],
            # "timing-scheme": ["inc_100"],
            "ring-mode": ["random"],
            "subflows": [1],

            # each placement strategy will be a different plot, drawn next to each other.
            "placement-mode": placement_modes, 
            
            # some dynamic parameters.                             
            "ft-core-count": [base_options["ft-server-per-rack"] // oversub],
            "placement-seed": list(range(1, selected_setting["placement-seed-range"] + 1)), 
        } 
        
        
        # to be give to the CS, which will be used to populate the run_context.
        # the run_context will be then handed back to the custom functions. 
        # am I making this too complicated? I think I am.
        exp_context = {
            # other stuff
            "random-rep-count": random_rep_count,
            "interesting-metrics": interesting_metrics,
            "all-placement-modes": placement_modes,
            "experiment-seed": experiment_seed,
            "oversub": oversub,
            
            "cassini-parameters": cassini_parameters,
            "selected-setting": selected_setting,
            
            "comparison-base": {"ring-mode": "random", 
                                "lb-scheme": lb, 
                                "timing-scheme": "zero",
                                "subflows": 1} ,  
            
            "comparisons": [
                ("random", {"timing-scheme": "random"}),  
                ("farid", {"timing-scheme": "farid"}),  
                ("inc_100", {"timing-scheme": "inc_100"}),  
                ("inc_200", {"timing-scheme": "inc_200"}),  
                ("inc_400", {"timing-scheme": "inc_400"}),  
                ("inc_500", {"timing-scheme": "inc_500"}),  
                ("cassini", {"timing-scheme": "cassini"}),  
                ("Ideal", {"lb-scheme": "ideal"}),    
            ]
        } 

            
        cs = ConfigSweeper(
            base_options, exp_sweep_config, exp_context,
            run_command_options_modifier, 
            run_results_modifier, 
            custom_save_results_func, 
            result_extractor_function,
            exp_name="nethint_LB+{}_TS+{}_R+{}_{}_{}".format(lb, "", "",  
                                                            oversub, 
                                                            experiment_seed),
            worker_thread_count=1, 
        )
        
        cs.sweep()


if __name__ == "__main__":
    main() 
    