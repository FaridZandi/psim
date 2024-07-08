import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns 
import sys 

def plot_csv_data(df, metric, plot_path):
    
    sns.lineplot(
        data=df, 
        x='Job 2 inital shift',
        y=metric,
        hue='Comm. Duty Cycle',
        palette='tab10',
    )
    plt.xlabel('Job 2 inital shift')
    plt.ylabel(metric)
    plt.title(f'{metric} vs Job 2 inital shift')
    
    plt.legend(loc='upper left', bbox_to_anchor=(1.05, 1), title='Comm. Duty Cycle')
    
    plt.savefig(plot_path, bbox_inches='tight', dpi=300)
    plt.clf()

if __name__ == "__main__": 
    csv_file_path = sys.argv[1] 
    
    df = pd.read_csv(csv_file_path)
    df = df.sort_values('general-param-1')
    
    m = {
        'general-param-1': 'Job 2 inital shift',
        'general-param-2': 'Comm. Duty Cycle',
    }
    df = df.rename(columns=m)
    
    metrics = ["j1_conv_point", "j2_conv_point", 
               "j1_conv_value", "j2_conv_value", 
               "drifts_conv_point", "drifts_conv_value", 
               "max_psim_time", 
               "job_1_iter_1", "job_2_iter_1"]
    
    
    for i, metric in enumerate(metrics): 
        if ('Job 2 inital shift' not in df.columns or
            'Comm. Duty Cycle' not in df.columns or 
            metric not in df.columns):
            
            print("The specified columns are not found in the CSV file.")
            exit(0)
        
        results_dir = csv_file_path[:csv_file_path.rfind("/")] + "/"
        plot_path = "{}/convergence-{}.png".format(results_dir, metric)
        plot_csv_data(df, metric, plot_path)
    

    