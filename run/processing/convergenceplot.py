import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns 
import sys, os 

def plot_csv_data(df, metric, plot_path):
    
    sns.lineplot(
        data=df, 
        x='Job 2 inital shift',
        y=metric,
        hue='Job 1 Comm. Duty Cycle',
        palette='tab10',
    )
    plt.xlabel('Job 2 inital shift')
    plt.ylabel(metric)
    plt.title(f'{metric} vs Job 2 inital shift')
    
    plt.legend(loc='upper left', bbox_to_anchor=(1.05, 1), title='Job 1 Comm. Duty Cycle')
    
    plt.savefig(plot_path, bbox_inches='tight', dpi=300)
    plt.clf()

if __name__ == "__main__": 
    results_dir = "results/"
    
    if len(sys.argv) < 2:
        #inside the results directory, there are a bunch of directories with names like 
        # 345-run, 346-run, etc. find the latest one and use that to generate the plots
        all_runs = [int(x.split("-")[0]) for x in os.listdir(results_dir) if x.endswith("-run")]
        max_run = max(all_runs)
        csv_file_path = f"{results_dir}{max_run}-run/results.csv"
        print(f"Using the latest run: {max_run}")        
    else:
        if not os.path.isfile(sys.argv[1]):
            csv_file_path = f"{results_dir}{sys.argv[1]}-run/results.csv"
            print(f"Using the specified file path: {csv_file_path}")
        else:
            csv_file_path = sys.argv[1]
            print(f"Using the specified file path: {csv_file_path}") 
    
    df = pd.read_csv(csv_file_path)
    df = df.sort_values('general-param-1')
    
    m = {
        'general-param-1': 'Job 2 inital shift',
        'general-param-2': 'Job 1 Comm. Duty Cycle',
        'general-param-7': 'Job 2 Comm. Duty Cycle',
    }
    df = df.rename(columns=m)
    
    
    def change_in_first_iter(row):
        iter_diff = row["job_1_iter_1"] - row["job_2_iter_1"]
        base_iter_diff = row["Job 1 Comm. Duty Cycle"] - row["Job 2 Comm. Duty Cycle"]
        return iter_diff - base_iter_diff   
    
    metrics = [
            # "j1_conv_point", 
            #    "j2_conv_point", 
            #    "j1_conv_value", 
            #    "j2_conv_value", 
            #    "drifts_conv_point", 
            #    "drifts_conv_value", 
            #    "max_psim_time", 
               "job_1_iter_1", 
               "job_2_iter_1", 
            #    ("change_in_first_iter", change_in_first_iter),
               ]
    
    
    for i, metric in enumerate(metrics): 
        
        if isinstance(metric, tuple):
            metric_name, metric_func = metric
            df[metric_name] = df.apply(metric_func, axis=1)
            metric = metric_name
            
        if ('Job 2 inital shift' not in df.columns or
            'Job 1 Comm. Duty Cycle' not in df.columns or 
            metric not in df.columns):
            
            print("The specified columns are not found in the CSV file.")

        results_dir = csv_file_path[:csv_file_path.rfind("/")] + "/"
        plot_path = "{}/convergence-{}.png".format(results_dir, metric)
        plot_csv_data(df, metric, plot_path)
    

    