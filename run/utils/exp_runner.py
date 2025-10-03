from utils.util import *
from utils.sweep_large_nethint_base import *
from utils.sweep_base import ConfigSweeper

all_metrics = {
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
        "compare_mode": "self",
        "better": "lower",
        "type": "single_number",
    }, 
    "total_congested_time": {
        "avg_cdf_plot": True,   
        "iter_avg_plot": False,  
        "compare_mode": "self",
        "better": "lower",
        "type": "single_number",
    },
    "job_slowdown_fairness": {
        "avg_cdf_plot": True,   
        "iter_avg_plot": False,  
        "compare_mode": "self",
        "better": "lower",
        "type": "single_number",
    },
    "job_slowdowns": {
        "avg_cdf_plot": True,   
        "iter_avg_plot": False,  
        "compare_mode": "self",
        "better": "lower",
        "type": "single_list",
    },
    "job_times": {
        "avg_cdf_plot": True,
        "iter_avg_plot": False,
        "compare_mode": "self",
        "better": "lower",
        "type": "single_list",
    }, 
    "job_costs": {
        "avg_cdf_plot": True,
        "iter_avg_plot": False,
        "compare_mode": "self",
        "better": "lower",
        "type": "single_list",
    }, 
    "job_periods": {
        "avg_cdf_plot": True,
        "iter_avg_plot": False,
        "compare_mode": "self",
        "better": "lower",
        "type": "single_list",
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
        "compare_mode": "self",
        "better": "lower",
        "type": "single_number", 
    },
    "fixing_rounds": {  
        "avg_cdf_plot": True,   
        "iter_avg_plot": False,  
        "compare_mode": "self",
        "better": "lower",
        "type": "single_number",
    },
    "cmmcmp_ratio": {
        "avg_cdf_plot": True,   
        "iter_avg_plot": False,  
        "compare_mode": "self",
        "better": "lower",
        "type": "single_number",
    },  
    "final_entropy": {  
        "avg_cdf_plot": True,   
        "iter_avg_plot": False,  
        "compare_mode": "self",
        "better": "higher",
        "type": "single_number",
    },
    "remaining_bad_range_ratio": {  
        "avg_cdf_plot": True,   
        "iter_avg_plot": False,  
        "compare_mode": "self",
        "better": "lower",
        "type": "single_number",
    },
    "remaining_bad_range_ratios": { 
        "avg_cdf_plot": True,   
        "iter_avg_plot": False,  
        "compare_mode": "self",
        "better": "lower",
        "type": "single_list",
    },
    "fixed_bad_range_ratio": {  
        "avg_cdf_plot": True,   
        "iter_avg_plot": False,  
        "compare_mode": "self",
        "better": "higher",
        "type": "single_number",
    },
    "fixed_bad_range_ratios": { 
        "avg_cdf_plot": True,   
        "iter_avg_plot": False,  
        "compare_mode": "self",
        "better": "lower",
        "type": "single_list",
    },
    "average_fct": {
        "avg_cdf_plot": True,   
        "iter_avg_plot": False,  
        "compare_mode": "divide",
        "better": "lower",
        "type": "single_number",
    },
    "average_flow_bw": {
        "avg_cdf_plot": True,   
        "iter_avg_plot": False,  
        "compare_mode": "self",
        "better": "lower",
        "type": "single_number",
    },   
    "accel_util_rate": {
        "avg_cdf_plot": True,   
        "iter_avg_plot": False,  
        "compare_mode": "self",
        "better": "higher",
        "type": "single_number",
    }
}


# this is an experiment that has one base setting, and then a bunch of comparisons.
# I'm attempting to keep the things that should be kept constant, constant.
# then compute the speedup of the other things with respect to the base setting.
def do_experiment(seed_range=1, 
                  placement_mode="random", 
                  machine_count=8,
                  rack_size=4,
                  oversub=1, 
                  force_core_count=None,
                  job_count=None,
                  job_sizes=(2, 2), 
                  sim_length=50000, 
                  punish_oversubscribed_min=0.5, 
                  min_rate=100, 
                  inflate=1.0, 
                  ring_mode="letitbe", 
                  desired_entropy=0.5,
                  cmmcmp_range=(0, 1),
                  comm_size=(12000, 60000, 6000),
                  comp_size=(200, 1000, 1),
                  layer_count=(1, 2, 1), 
                  fallback_threshold=0.5, 
                  experiment_seed=77, 
                  recorded_metrics=[], 
                  added_comparisons=[], 
                  placement_seeds=None, 
                  plot_stuff=False, 
                  farid_rounds=12,  
                  worker_thread_count=40,
                  throttle_search=True,
                  run_cassini_timing_in_subprocess=True,
                  use_inflation=False,
                  throttle_levels=None,
                  ): 
    
    
    random.seed(experiment_seed)

    if isinstance(rack_size, str) and rack_size.startswith("/"):
        rack_size = machine_count // int(rack_size[1:])
        assert rack_size > 0
        
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
        "iter-count": [30], # placeholder. defined later based on the job length.
        "job-count": job_count,
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
        "placement-mode": placement_mode,   
        "ring-mode": ring_mode,  
    }
    
    interesting_metrics = {}
    if len(recorded_metrics) == 0:
        interesting_metrics = all_metrics
    else:   
        for metric in recorded_metrics: 
            interesting_metrics[metric] = all_metrics[metric]

    if force_core_count is not None and oversub is not None: 
        print("Warning: both force_core_count and oversub are set.")
        print("exiting.")
        sys.exit(1) 
    if force_core_count is not None:
        core_count = force_core_count   
        oversub = base_options["ft-server-per-rack"] // core_count
    else:
        core_count = int(base_options["ft-server-per-rack"] // oversub)



    if throttle_levels is not None: 
        if throttle_levels == 1 or core_count == 1:
            profiled_throttle_factors = [1.0]
            subflow_count = 1
        elif throttle_levels == 2 or core_count <= 3:
            profiled_throttle_factors = [1.0, 0.5]
            subflow_count = 2   
        elif throttle_levels == 4 or core_count >= 4:
            profiled_throttle_factors = [1.0, 0.75, 0.5, 0.25]
            subflow_count = 4  
    else: 
        if not throttle_search:
            profiled_throttle_factors = [1.0]
            subflow_count = 1   
        else: 
            if core_count == 1:
                profiled_throttle_factors = [1.0]
                subflow_count = 1
            if core_count == 2 or core_count == 3:
                profiled_throttle_factors = [1.0, 0.5]
                subflow_count = 2 
            elif core_count >= 4:
                profiled_throttle_factors = [1.0, 0.75, 0.5, 0.25]
                subflow_count = 4

    if placement_seeds is None:
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
        "farid-rounds": farid_rounds, 
        
        "fallback-threshold": fallback_threshold, 

        "lb-scheme": "random", 
        "routing-fit-strategy": "best",    
        "subflows": 1,
        "inflate": inflate,   
        "protocol-file-name": "nethint-test",
        "ft-core-count": core_count,
        "use_inflation": use_inflation, 
    }

    comparisons = []
    
    if len(added_comparisons) == 0:
        add_all = True
    else:
        add_all = False 
        
    if "TS" in added_comparisons or add_all:
        comparisons.append(("TS", {
                                "timing-scheme": "faridv2",
                                "throttle-search": False,
                                "lb-scheme": "random"
                            }))
    if "RO" in added_comparisons or add_all:
        comparisons.append(("RO", {
                                "timing-scheme": "zero",
                                "routing-fit-strategy": "graph-coloring-v3",  
                                "lb-scheme": "readprotocol"
                            }))
        
    if "conga" in added_comparisons or add_all:
        comparisons.append(("conga", {
                                "timing-scheme": "zero",
                                "throttle-search": False,
                                "lb-scheme": "leastloaded",
                            }))
    
    if "spray" in added_comparisons or add_all: 
        comparisons.append(("spray", {
                                "timing-scheme": "zero",
                                "throttle-search": False,
                                "lb-scheme": "perfect",
                            }))
    
    if "TS+SUB" in added_comparisons or add_all:
        comparisons.append((f"TS+SUB", {
                                "timing-scheme": "faridv2",
                                "subflows": subflow_count, 
                                "throttle-search": True if subflow_count > 1 else False,    
                                "lb-scheme": "random"
                            }))
    
    if "TS+RO" in added_comparisons or add_all:
        comparisons.append(("TS+RO", {
                                "timing-scheme": "faridv2",
                                "throttle-search": False,
                                "routing-fit-strategy": "graph-coloring-v5",  
                                "lb-scheme": "readprotocol"
                            }))
        
    
    if "TS+RO+SUB" in added_comparisons or add_all:   
        comparisons.append(("TS+RO+SUB", {
                                "timing-scheme": "faridv2",
                                "throttle-search": True if subflow_count > 1 else False,
                                "subflows": subflow_count, 
                                "routing-fit-strategy": "graph-coloring-v5",  
                                "lb-scheme": "readprotocol"
                            }))
        
    if "TS+RO+REP" in added_comparisons or add_all:
        comparisons.append(("TS+RO+REP", {
                                "timing-scheme": "faridv4",
                                "throttle-search": False,
                                "routing-fit-strategy": "graph-coloring-v5",  
                                "lb-scheme": "readprotocol"
                            }))

    if "TS+RO+SUB+REP" in added_comparisons or add_all:
        comparisons.append(("TS+RO+SUB+REP", {
                                "timing-scheme": "faridv4",
                                "throttle-search": True if subflow_count > 1 else False,
                                "subflows": subflow_count, 
                                "routing-fit-strategy": "graph-coloring-v5",  
                                "lb-scheme": "readprotocol"
                            }))
        
    if "perfect" in added_comparisons or add_all:
        comparisons.append(("Perfect", {
                                "timing-scheme": "zero",
                                "lb-scheme": "perfect"
                        }))

    if "coloring-v5" in added_comparisons or add_all:
        comparisons.append(("coloring-v5", {
                                "timing-scheme": "faridv5",
                                "throttle-search": True if subflow_count > 1 else False,
                                "subflows": subflow_count, 
                                "farid-rounds": farid_rounds,   
                                "routing-fit-strategy": "graph-coloring-v5",  
                                "lb-scheme": "readprotocol"
                            })) 
        
    if "coloring-v6" in added_comparisons or add_all:
        comparisons.append(("coloring-v6", {
                                "timing-scheme": "faridv4",
                                "throttle-search": True if subflow_count > 1 else False,
                                "subflows": subflow_count, 
                                "routing-fit-strategy": "graph-coloring-v6",  
                                "lb-scheme": "readprotocol"
                            }))
        
    if "coloring-v7" in added_comparisons or add_all:   
        comparisons.append(("coloring-v7", {
                                "timing-scheme": "faridv5",
                                "throttle-search": True if subflow_count > 1 else False,
                                "subflows": subflow_count, 
                                "farid-rounds": farid_rounds,   
                                "routing-fit-strategy": "graph-coloring-v7",  
                                "lb-scheme": "readprotocol"
                            }))
        
    if "coloring-v8" in added_comparisons or add_all:   
        comparisons.append(("coloring-v8", {
                                "timing-scheme": "faridv6",
                                "throttle-search": True if subflow_count > 1 else False,
                                "subflows": subflow_count, 
                                "farid-rounds": farid_rounds,   
                                "routing-fit-strategy": "graph-coloring-v8",  
                                "lb-scheme": "readprotocol"
                            }))
        
    if "zero-v7" in added_comparisons or add_all:   
        comparisons.append(("zero-v7", {
                                "timing-scheme": "zero",
                                "throttle-search": True if subflow_count > 1 else False,
                                "subflows": subflow_count, 
                                "farid-rounds": farid_rounds,   
                                "routing-fit-strategy": "graph-coloring-v7",
                                "lb-scheme": "readprotocol"
                            }))
        
    if "zero-v3" in added_comparisons or add_all:   
        comparisons.append(("zero-v3", {
                                "timing-scheme": "zero",
                                "throttle-search": True if subflow_count > 1 else False,
                                "subflows": subflow_count, 
                                "farid-rounds": 0,   
                                "routing-fit-strategy": "graph-coloring-v3",
                                "lb-scheme": "readprotocol"
                            }))
        
    if "rounds" in added_comparisons or "rounds-v5" in added_comparisons or add_all:
        for rounds in range(0, 101, 10):
            comparisons.append(("foresight-v5-{}".format(rounds), {
                                "timing-scheme": "faridv5",
                                "throttle-search": True if subflow_count > 1 else False,
                                "subflows": subflow_count, 
                                "routing-fit-strategy": "graph-coloring-v5",  
                                "lb-scheme": "readprotocol", 
                                "farid-rounds": rounds, 
                            }))

    if "rounds-v7" in added_comparisons or add_all:
        for rounds in range(0, 101, 10):
            comparisons.append(("foresight-v7-{}".format(rounds), {
                                "timing-scheme": "faridv5",
                                "throttle-search": True if subflow_count > 1 else False,
                                "subflows": subflow_count, 
                                "routing-fit-strategy": "graph-coloring-v7",  
                                "lb-scheme": "readprotocol", 
                                "farid-rounds": rounds, 
                            }))

    if "rounds-v8" in added_comparisons or add_all:
        for rounds in range(0, 101, 10):
            comparisons.append(("foresight-v8-{}".format(rounds), {
                                "timing-scheme": "faridv6",
                                "throttle-search": True if subflow_count > 1 else False,
                                "subflows": subflow_count, 
                                "routing-fit-strategy": "graph-coloring-v8",  
                                "lb-scheme": "readprotocol", 
                                "farid-rounds": rounds, 
                            }))
            
    if "rounds-v7-new" in added_comparisons or add_all:
        for rounds in range(0, 21, 2):
            comparisons.append(("foresight-v7-{}".format(rounds), {
                                # "timing-scheme": "faridv6",
                                # "throttle-search": True,
                                # "subflows": subflow_count,
                                # "routing-fit-strategy": "graph-coloring-v7",
                                # "farid-rounds": farid_rounds,
                                # "use_inflation": True,  
                                # "lb-scheme": "readprotocol"
                
                
                                "timing-scheme": "faridv6",
                                "throttle-search": True,
                                "subflows": subflow_count, 
                                "routing-fit-strategy": "graph-coloring-v7",
                                "use_inflation": True,    
                                "lb-scheme": "readprotocol", 
                                "farid-rounds": rounds, 
                            }))
            
    if "rounds-fb-v8" in added_comparisons or add_all:
        for rounds in range(0, 101, 10):
            comparisons.append(("foresight-fb-v8-{}".format(rounds), {
                                "timing-scheme": "faridv6",
                                "throttle-search": True if subflow_count > 1 else False,
                                "subflows": subflow_count, 
                                "routing-fit-strategy": "graph-coloring-v8",  
                                "lb-scheme": "readprotocol", 
                                "farid-rounds": rounds, 
                                "fallback-threshold": fallback_threshold, 
                            }))
            
    if "coloring-v8-temp" in added_comparisons or add_all:   
        for i in range(10):  
            comparisons.append((f"coloring-v8-temp-{i}", {
                                    "timing-scheme": "faridv6",
                                    "throttle-search": True if subflow_count > 1 else False,
                                    "subflows": subflow_count, 
                                    "farid-rounds": i,   
                                    "routing-fit-strategy": "graph-coloring-v8",  
                                    "lb-scheme": "readprotocol",
                                }))
            
            
    ###################
    
    if "TS-new" in added_comparisons or add_all:
        comparisons.append(("TS-new", {
                                "timing-scheme": "faridv2",
                                "subflows": 1,
                                "throttle-search": False,
                                "farid-rounds": 0,
                                "lb-scheme": "random"
                            }))
        
    if "RO-new" in added_comparisons or add_all:
        comparisons.append(("RO-new", {
                                "timing-scheme": "zero",
                                "routing-fit-strategy": "graph-coloring-v3",
                                "subflows": 1,
                                "throttle-search": False, 
                                "farid-rounds": 0,
                                "lb-scheme": "readprotocol"
                            }))
        
    if "TS+SUB-new" in added_comparisons or add_all:
        comparisons.append((f"TS+SUB-new", {
                                "timing-scheme": "faridv2",
                                "subflows": subflow_count, 
                                "throttle-search": True,
                                "farid-rounds": 0,
                                "lb-scheme": "random"
                            }))
    
    if "TS+RO-new" in added_comparisons or add_all:
        comparisons.append(("TS+RO-new", {
                                "timing-scheme": "faridv6",
                                "throttle-search": False,
                                "subflows": 1,
                                "routing-fit-strategy": "graph-coloring-v7",  
                                "farid-rounds": 0,   
                                "lb-scheme": "readprotocol"
                            }))
        
    
    if "TS+RO+SUB-new" in added_comparisons or add_all:   
        comparisons.append(("TS+RO+SUB-new", {
                                "timing-scheme": "faridv6",
                                "throttle-search": True, 
                                "subflows": subflow_count, 
                                "routing-fit-strategy": "graph-coloring-v7",
                                "farid-rounds": 0,  
                                "lb-scheme": "readprotocol"
                            }))
        
    if "TS+RO+REP-new" in added_comparisons or add_all:
        comparisons.append(("TS+RO+REP-new", {
                                "timing-scheme": "faridv6",
                                "throttle-search": False,
                                "subflows": 1,
                                "routing-fit-strategy": "graph-coloring-v7",
                                "farid-rounds": farid_rounds, 
                                "lb-scheme": "readprotocol"
                            }))
    if "TS+RO+REP-inf-new" in added_comparisons or add_all:
        comparisons.append(("TS+RO+REP-inf-new", {
                                "timing-scheme": "faridv6",
                                "throttle-search": False,
                                "subflows": 1,
                                "routing-fit-strategy": "graph-coloring-v7",
                                "farid-rounds": farid_rounds, 
                                "lb-scheme": "readprotocol", 
                                "use_inflation": True,
                            }))

    if "TS+RO+SUB+REP-new" in added_comparisons or add_all:
        comparisons.append(("TS+RO+SUB+REP-new", {
                                "timing-scheme": "faridv6",
                                "throttle-search": True,
                                "subflows": subflow_count,
                                "routing-fit-strategy": "graph-coloring-v7",
                                "farid-rounds": farid_rounds,
                                "lb-scheme": "readprotocol"
                            }))
        
    if "TS+RO+SUB+REP-inf-new" in added_comparisons or add_all:
        comparisons.append(("TS+RO+SUB+REP-inf-new", {
                                "timing-scheme": "faridv6",
                                "throttle-search": True,
                                "subflows": subflow_count,
                                "routing-fit-strategy": "graph-coloring-v7",
                                "farid-rounds": farid_rounds,
                                "use_inflation": True,  
                                "lb-scheme": "readprotocol"
                            }))
        
    ######################
    
    
    # to be give to the CS, which will be used to populate the run_context.
    # the run_context will be then handed back to the custom functions. 
    # am I making this too complicated? I think I am.
    exp_context = {
        "sim-length": sim_length,

        "plot-iteration-graphs": plot_stuff, 
        "plot-initial-timing": plot_stuff,
        "plot-intermediate-timing": plot_stuff,
        "plot-final-timing": plot_stuff,
        "plot-routing-assignment": plot_stuff, 
        "plot-merged-ranges": plot_stuff, 
        "plot-runtime-timing": plot_stuff,
        "plot-link-empty-times": plot_stuff,
        
        "profiled-throttle-factors": profiled_throttle_factors, 
        
        # other stuff
        "random-rep-count": 1,
        "interesting-metrics": interesting_metrics,
        "all-placement-modes": [placement_mode],
        "experiment-seed": experiment_seed,
        "oversub": oversub,
        
        "cassini-parameters": {  
            "link-solution-candidate-count": 100,   
            "link-solution-random-quantum": 10,
            "link-solution-top-candidates": 3,    
            "overall-solution-candidate-count": 10,
            "save-profiles": True,
        },
        
        "routing-parameters": {},
        "placement-parameters": {
            "desired-entropy": desired_entropy,
            "placement-seed-limit": 100,
        },   
        
        "selected-setting": selected_setting,
        "comparison-base": comparison_base,              
        "comparisons": comparisons,
        "profiling-core-count": core_count,
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
        worker_thread_count=worker_thread_count, 
        plot_cdfs=False,
        store_outputs=False,
        run_cassini_timing_in_subprocess=run_cassini_timing_in_subprocess,
    )
    
    summary = cs.sweep()
    results_dir = cs.get_results_dir()
    
    return summary, results_dir 
    
    
def create_command(plot_args, plot_commands_path):  
    plot_command = "python3 plot_compare.py " + " ".join([f"--{key} {value}" for key, value in plot_args.items()])
    
    with open(plot_commands_path, "a") as f:
        clean_plot_command = plot_command
        while "  " in clean_plot_command:
            clean_plot_command = clean_plot_command.replace("  ", " ") 
        f.write(clean_plot_command + "\n\n")
        
def get_global_config(): 
    return {
        "machines": 48,
        "mult": 20, 
        "sim": 400, 
        "seed_range": 1,
    }