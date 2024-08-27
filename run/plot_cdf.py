import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

import sys 
import os 
import json  

compact_csv_path = sys.argv[1]
random_csv_path = sys.argv[2]

results_dir = compact_csv_path[:compact_csv_path.rfind("/")] + "/"

separating_params = sys.argv[3].split(",")  # each combination will create a separate plot
cdf_params = sys.argv[4].split(",") # these columns should contain a list, for which the cdf will be plotted

plots_dir = sys.argv[5]

os.makedirs(plots_dir, exist_ok=True)

# load the data
compact_df = pd.read_csv(compact_csv_path)
random_df = pd.read_csv(random_csv_path)

# combine the separate params in a column looking like "param1=value1,param2=value2"
compact_df["combined"] = compact_df.apply(lambda row: ",".join([f"{param}={row[param]}" for param in separating_params]), axis=1)
random_df["combined"] = random_df.apply(lambda row: ",".join([f"{param}={row[param]}" for param in separating_params]), axis=1)

# unique values of the combined column
unique_combined = compact_df["combined"].unique()

# for each unique value, plot the cdf
for combined in unique_combined:
    # filter the data
    compact_filtered_df = compact_df[compact_df["combined"] == combined]
    random_filtered_df = random_df[random_df["combined"] == combined]
    
    fig, axes = plt.subplots(1, 2, figsize=(9, 3), sharex=True, sharey=True)
    
    for i, exp in enumerate([("Random Placement", random_filtered_df), ("Compact Placement", compact_filtered_df)]): 
        placement_type, placement_df = exp
        this_ax = axes[i]

        # how many items are in the placement_df? print: 
        if placement_df.shape[0] != 1:
            print(f"Error: {placement_type} has more than one row for {combined}")
        
        # get the first line 
        first_line = placement_df.iloc[0]
        
        # plot the cdf
        for cdf_param in cdf_params:
            values = first_line[cdf_param]
            # values is a string. We need to convert it to a list of floats
            values = json.loads(values)
            
            values = sorted(values)
            yvals = np.arange(len(values))/float(len(values) - 1)
            
            this_ax.plot(values, yvals, label=cdf_param)
        
        
        # tick vertical line at x = 1 
        this_ax.axvline(x=1, color="black", linestyle="-")
        
        this_ax.set_ylim(0, 1)
        this_ax.set_title(placement_type)
        this_ax.set_xlabel("Value")
        this_ax.set_ylabel("CDF")
        
        
        if i == 1: 
            this_ax.legend(loc="lower right")
        
    plt.suptitle(combined, y=1.05)
        
    plot_path = "{}/{}.png".format(plots_dir, combined)
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    plt.clf()
    
    