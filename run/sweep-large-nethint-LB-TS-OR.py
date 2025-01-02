from utils.util import *
from utils.sweep_large_nethint_base import *
from utils.sweep_base import ConfigSweeper

experiment_seed = 58
random_rep_count = 1

def main():
    # choose one of the settings to run the experiments with.     
    selected_setting = nethint_settings[4]
    
    base_options = {
        "step-size": 1,
        "core-status-profiling-interval": 100000,
        "rep-count": 1, 
        "console-log-level": 4,
        "file-log-level": 3,
        
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

    interesting_metrics = ["avg_ar_time", "avg_iter_time"] 
    placement_modes = ["random", "semirandom_8", "semirandom_4", "semirandom_2", "semirandom_1", "compact"]
    # placement_modes = ["random", "compact"] 
    
    base_lb_scheme = "random"
    base_timing_scheme = "random"
    base_ring_mode = "random" 
    base_subflow_count = 1
    
    compared_lb_schemes = ["perfect"] #"powerof2", "roundrobin", "ecmp", "perfect",
    compared_timing_schemes = ["inc"] # "random" "inc"
    compared_ring_modes = ["optimal"]
    compared_subflow_counts = [16]    
    
    oversubs = [4]
    
    cassini_parameters = {  
        "link-solution-candidate-count": 10,
        "link-solution-random-quantum": 10,
        "link-solution-top-candidates": 10,    
        "overall-solution-candidate-count": 10,
    }    
    
    random.seed(experiment_seed)
    
    
    # iterating over the different settings. Each setting will create a different experiment. 
    # the results is one plot for each setting.
    for oversub in oversubs: 
        for compared_subflow_count in compared_subflow_counts:
            for compared_lb_scheme in compared_lb_schemes: 
                for compared_timing_scheme in compared_timing_schemes:
                    for compared_ring_mode in compared_ring_modes:
                        
                        exp_sweep_config = {
                            "protocol-file-name": ["nethint-test"],

                            # placement and workload parameters.
                            # these will be different lines in the cdf plot.
                            "lb-scheme": [base_lb_scheme, compared_lb_scheme, "ideal"],
                            "timing-scheme": [base_timing_scheme, compared_timing_scheme],
                            "ring-mode": [base_ring_mode, compared_ring_mode],
                            "subflows": [base_subflow_count, compared_subflow_count],

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
                            "sim-length": 1000000,
                            
                            # other stuff
                            "random-rep-count": random_rep_count,
                            "interesting-metrics": interesting_metrics,
                            "all-placement-modes": placement_modes,
                            "experiment-seed": experiment_seed,
                            "oversub": oversub,
                            
                            "cassini-parameters": cassini_parameters,
                            "selected-setting": selected_setting,
                            
                            "comparison-base": {"ring-mode": base_ring_mode, 
                                                "lb-scheme": base_lb_scheme, 
                                                "timing-scheme": base_timing_scheme,
                                                "subflows": base_subflow_count} ,  
                            
                            "comparisons": [
                                ("OR", {"ring-mode": compared_ring_mode}), 
                                ("LB", {"lb-scheme": compared_lb_scheme}),
                                ("TS", {"timing-scheme": compared_timing_scheme}),
                                ("SUB", {"subflows": compared_subflow_count}),
                                                
                                ("OR+LB", {"ring-mode": compared_ring_mode, "lb-scheme": compared_lb_scheme}),
                                ("OR+TS", {"ring-mode": compared_ring_mode, "timing-scheme": compared_timing_scheme}),
                                ("LB+TS", {"lb-scheme": compared_lb_scheme, "timing-scheme": compared_timing_scheme}),
                                
                                ("OR+LB+TS", {"ring-mode": compared_ring_mode, 
                                            "lb-scheme": compared_lb_scheme, 
                                            "timing-scheme": compared_timing_scheme}),
                                
                                ("OR+LB+TS+SUB", {"ring-mode": compared_ring_mode, 
                                                "lb-scheme": compared_lb_scheme, 
                                                "timing-scheme": compared_timing_scheme, 
                                                "subflows": compared_subflow_count}),
                                    
                                ("Ideal", {"lb-scheme": "ideal"}),    
                            ]
                        } 

                            
                        cs = ConfigSweeper(
                            base_options, exp_sweep_config, exp_context,
                            run_command_options_modifier, 
                            run_results_modifier, 
                            custom_save_results_func, 
                            result_extractor_function,
                            exp_name="nethint_LB+{}_TS+{}_R+{}_{}_{}".format(compared_lb_scheme, 
                                                                             compared_timing_scheme,
                                                                             compared_ring_mode,  
                                                                             oversub, 
                                                                             experiment_seed),
                            worker_thread_count=40, 
                        )
                        
                        cs.sweep()


if __name__ == "__main__":
    main() 
    