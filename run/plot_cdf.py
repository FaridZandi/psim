import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

import sys 
import os 
import json  

csv_path = sys.argv[1]
results_dir = csv_path[:csv_path.rfind("/")] + "/"
plots_dir = results_dir + "cdfs/"
os.makedirs(plots_dir, exist_ok=True)

separating_params = sys.argv[2].split(",")  # each combination will create a separate plot
same_plot_param = sys.argv[3] # all the combinations will be plotted on the same plot #TODO: this should be a list
cdf_params = sys.argv[4].split() # these columns should contain a list, for which the cdf will be plotted

# load the data
df = pd.read_csv(csv_path)
# combine the separate params in a column looking like "param1=value1,param2=value2"
df["combined"] = df.apply(lambda row: ",".join([f"{param}={row[param]}" for param in separating_params]), axis=1)

# unique values of the combined column
unique_combined = df["combined"].unique()

# for each unique value, plot the cdf
for combined in unique_combined:

    # filter the data
    filtered_df = df[df["combined"] == combined]

    fig, axes = plt.subplots(2, 2, figsize=(10, 10), sharex=True, sharey=True)
    
    unique_same_plot_param = filtered_df[same_plot_param].unique()
    
    if len(unique_same_plot_param) != 4:
        print("there are not 4 unique values for the same_plot_param which is {}".format(same_plot_param))
        exit(0)
        
    for i, same_plot_value in enumerate(unique_same_plot_param): 
        this_ax = axes[i // 2, i % 2]

        # filter the data
        
        this_param_df = filtered_df[filtered_df[same_plot_param] == same_plot_value]
        this_param_df = this_param_df[cdf_params]
        
        # get the first line 
        first_line = this_param_df.iloc[0] 
        
        # plot the cdf
        for cdf_param in cdf_params:
            values = first_line[cdf_param]
            # values is a string. We need to convert it to a list of floats
            values = json.loads(values)
            
            values = sorted(values)
            yvals = np.arange(len(values))/float(len(values))
            this_ax.plot(values, yvals, label=cdf_param)
        
        this_ax.set_title(same_plot_value)
        this_ax.set_xlabel("Value")
        this_ax.set_ylabel("CDF")
        
        if i == 3: 
            this_ax.legend(loc="lower right")
            
        

    plot_path = "{}/{}.png".format(plots_dir, combined)
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    plt.clf()
    
    