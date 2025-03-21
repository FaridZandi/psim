from utils.util import *
from utils.sweep_large_nethint_base import *
from utils.sweep_base import ConfigSweeper
import itertools
from utils.exp_runner import do_experiment, create_command, get_global_config

# Here, we iterate over things that will have different baselines to compare against.   
# the idea is that eventually, one plot should be generate for each of these setting combinations.   
if __name__ == "__main__":
    original_exp_number = 5841
    seed_range = 400
    m = 100
    clean_up_sweep_files = True
    
    os.system("./git_backup.sh")

    exp_number = get_incremented_number() if original_exp_number is None else original_exp_number
    
    exp_dir = f"results/exps/{exp_number}"
    os.makedirs(exp_dir, exist_ok=True)
    results_path = f"{exp_dir}/results.csv"     
    backup_results_path = f"{exp_dir}/results_backup_{get_incremented_number()}.csv"    
    plot_commands_path = f"{exp_dir}/results_plot.sh"
    os.system(f"touch {results_path}")
    
    if original_exp_number is not None: 
        # make a copy of the previous results at results_path   
        os.system(f"cp {results_path} {backup_results_path}")
        
        # read the previous results and continue from there.
        all_results_df = pd.read_csv(f"results/exps/{original_exp_number}/results.csv")
        print("Continuing from previous results")
        exit(1)
        
    if original_exp_number is None:
        os.system("rm -f last-exp-results-link-*") 
        os.system("ln -s {} {}".format(exp_dir, "last-exp-results-link-{}".format(exp_number)))
        
        for plot_type in ["cdf"]:
            plot_args = {
                "file_name": results_path,
                "plot_params": "metric",
                "subplot_y_params": "machine_count",
                "subplot_x_params": "oversub",
                "subplot_hue_params": "desired_entropy",
                "plot_x_params": "job_sizes",
                "plot_y_param": "values",
                "sharex": True, 
                "sharey": True,
                "subplot_width": 3,
                "subplot_height": 2,
                "plot_type": plot_type, 
                "ext": "png", 
                "values_name": "Speedup", 
                "exclude_base": True,   
            }
            create_command(plot_args, plot_commands_path)
        os.system(f"chmod +x {plot_commands_path}")

    exp_config = [
        ("sim_length", [400 * m]),
        ("machine_count", [48]),
        ("rack_size", [8]),
        ("job_sizes", [(4, 48)]),
        ("placement_mode", ["entropy"]), 
        ("ring_mode", ["letitbe"]), 
        ("desired_entropy", [0.3, 0.4, 0.5, 0.6, 0.7]),
        ("oversub", [1, 2]),
        ("cmmcmp_range", [(0, 2)]), 
        ("fallback_threshold", [0.5]),
        ("comm_size", [(120 * m, 360 * m, 60 * m)]),
        ("comp_size", [(2 * m, 10 * m, 1 * m)]),
        ("layer_count", [(1, 2, 1)]),
        ("punish_oversubscribed_min", [1]), 
        ("min_rate", [100]),
        ("inflate", [1]),    
    ]

    # comparisons = ["TS", "TS+SUB", "TS+RO", "TS+RO+SUB", "TS+RO+REP", "TS+RO+SUB+REP"]
    comparisons = ["TS+RO+SUB+REP"]
    
    relevant_keys = [key for key, options in exp_config if len(options) > 1]    
    
    all_results = [] 
    
    # go through all the possible combinations.
    keys, values = zip(*(dict(exp_config)).items())
    permutations_dicts = [dict(zip(keys, v)) for v in itertools.product(*values)]
    
    for perm in permutations_dicts:
        print("Running experiment with settings: ", perm)
        
        
        
        summary, results_dir = do_experiment(seed_range=seed_range, 
                                                added_comparisons=comparisons,
                                                experiment_seed=777, 
                                                **perm) 
        
        for summary_item in summary:    
            all_results.append({**summary_item, **perm})
        
        all_results_df = pd.DataFrame(all_results)    
        all_results_df.to_csv(results_path, index=False)
        
        if clean_up_sweep_files:
            os.system("rm -rf {}".format(results_dir)) 
        else:
            perm_key = "_".join([f"{key}_{perm[key]}" for key in relevant_keys])
            perm_key = perm_key.replace("(", "").replace(")", "").replace(" ", "")
            os.system("ln -s {} {}".format(results_dir, f"last-exp-results-link-{exp_number}/last-exp-results-link-{perm_key}"))
                        

    os.system(f"echo 'running plot commands'")
    os.system(f"./{plot_commands_path}")
    
    
