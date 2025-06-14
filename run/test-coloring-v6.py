from utils.util import *
from utils.sweep_large_nethint_base import *
import itertools
from utils.exp_runner import do_experiment, create_command, get_global_config

# Here, we iterate over things that will have different baselines to compare against.   
# the idea is that eventually, one plot should be generate for each of these setting combinations.   
if __name__ == "__main__":
    # make a backup of the current state of the repository.
    os.system("./git_backup.sh")
    
    g = get_global_config()
    
    seed_range = 1
    m = 10
    
    clean_up_sweep_files = False

    original_exp_number = None
    if original_exp_number is not None: 
        exp_number = original_exp_number
    else:
        exp_number = get_incremented_number() 
    
    exp_dir = f"results/exps/{exp_number}"
    os.makedirs(exp_dir, exist_ok=True)
    path = f"{exp_dir}/results.csv"     
    plot_commands_path = f"{exp_dir}/results_plot.sh"
                        
    for plot_type in []:
        plot_args = {
            "file_name": path,
            "plot_params": "metric",
            "subplot_y_params": "desired_entropy",
            "subplot_x_params": "oversub",
            "subplot_hue_params": "rack_size",
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
            "legend_side": "none",
        }
        create_command(plot_args, plot_commands_path)
        
    os.system(f"touch {plot_commands_path}")
    os.system(f"chmod +x {plot_commands_path}")
            
    if original_exp_number is None:
        exp_dir = f"results/exps/{exp_number}"
        path = f"results/exps/{exp_number}/results.csv" 
        os.makedirs(f"results/exps/{exp_number}", exist_ok=True)

        os.system("rm -f last-exp-results-link-*") 
        os.system("ln -s {} {}".format(exp_dir, "last-exp-results-link-{}".format(exp_number)))

        exp_config = [
            ("sim_length", [50 * m]),
            ("machine_count", [12]),
            ("rack_size", [4]),
            ("job_sizes", [(4, 4)]),
            ("placement_mode", ["entropy"]), 
            ("ring_mode", ["letitbe"]), 
            ("desired_entropy", [0.7]),
            ("oversub", [2]),
            ("cmmcmp_range", [(1.5, 2)]),
            ("fallback_threshold", [0.5]),
            ("comm_size", [(120 * m, 360 * m, 60 * m)]),
            ("comp_size", [(2 * m, 10 * m, 1 * m)]),
            ("layer_count", [(1, 2, 1)]),
            ("punish_oversubscribed_min", [1]), 
            ("min_rate", [100]),
            ("inflate", [1]),    
        ]

        # comparisons = ["TS", "TS+SUB", "TS+RO", "TS+RO+SUB", "TS+RO+REP", "TS+RO+SUB+REP"]
        # comparisons = ["TS", "TS+RO+SUB+REP", "Perfect"]
        comparisons = ["TS", "coloring-v6", "Perfect"]
        
        relevant_keys = [key for key, options in exp_config if len(options) > 1]    
        
        all_results = [] 
        
        # go through all the possible combinations.
        keys, values = zip(*(dict(exp_config)).items())
        permutations_dicts = [dict(zip(keys, v)) for v in itertools.product(*values)]
        
        for perm in permutations_dicts:
            print("Running experiment with settings: ", perm)
            
            summary, results_dir = do_experiment(seed_range=seed_range, 
                                                 added_comparisons=comparisons,
                                                 experiment_seed=77, 
                                                 farid_rounds=4,
                                                 plot_stuff=True, 
                                                 **perm) 
            
            for summary_item in summary:    
                all_results.append({**summary_item, **perm})
            
            all_results_df = pd.DataFrame(all_results)    
            all_results_df.to_csv(path, index=False)
            
            if clean_up_sweep_files:
                os.system("rm -rf {}".format(results_dir)) 
            else:
                perm_key = "_".join([f"{key}_{perm[key]}" for key in relevant_keys])
                perm_key = perm_key.replace("(", "").replace(")", "").replace(" ", "")
                os.system("ln -s {} {}".format(results_dir, f"last-exp-results-link-{exp_number}/last-exp-results-link-{perm_key}"))
                        

    os.system(f"echo 'running plot commands'")
    os.system(f"./{plot_commands_path}")
    
    
