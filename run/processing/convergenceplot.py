import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns 

def plot_csv_data(df, metric, ax):
    sns.lineplot(
        ax=ax,
        data=df, 
        x='Job 2 inital shift',
        y=metric,
        hue='Comm. Duty Cycle',
        palette='tab10',
    )
    ax.set_xlabel('Job 2 inital shift')
    ax.set_ylabel(metric)
    ax.set_title(f'{metric} vs Job 2 inital shift')
    
    ax.legend(loc='upper left', bbox_to_anchor=(1.05, 1), title='Comm. Duty Cycle')

                
import sys 

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
               "drifts_conv_point", "drifts_conv_value"]
    
    # create subplots for each metric
    fig, axs = plt.subplots(6, 1, figsize=(10, 40), sharex=True)
    # increase the space between the subplots
    plt.subplots_adjust(hspace=0.5)
    
    for i, metric in enumerate(metrics): 
        if ('Job 2 inital shift' not in df.columns or
            'Comm. Duty Cycle' not in df.columns or 
            metric not in df.columns):
            
            print("The specified columns are not found in the CSV file.")
            exit(0)
        
        plot_csv_data(df, metric, axs[i])
    
    results_dir = csv_file_path[:csv_file_path.rfind("/")] + "/"
    plt.savefig("{}/convergence.png".format(results_dir), bbox_inches='tight', dpi=300)
    