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

marker_cache = {} 
marker_options = ["o", "s", "D", "v", "^", "<", ">", "p", "P", "*", "h", "H", "+", "x", "X", "|", "_"]
def get_marker(param):
    global marker_cache
    if param not in marker_cache:
        marker_cache[param] = marker_options.pop(0)
    return marker_cache[param]

style_cache = {}
linestyle_tuple = [
     ('loosely dotted',        (0, (1, 10))),
     ('dotted',                (0, (1, 1))),
     ('densely dotted',        (0, (1, 1))),
     ('long dash with offset', (5, (10, 3))),
     ('loosely dashed',        (0, (5, 10))),
     ('dashed',                (0, (5, 5))),
     ('densely dashed',        (0, (5, 1))),

     ('loosely dashdotted',    (0, (3, 10, 1, 10))),
     ('dashdotted',            (0, (3, 5, 1, 5))),
     ('densely dashdotted',    (0, (3, 1, 1, 1))),

     ('dashdotdotted',         (0, (3, 5, 1, 5, 1, 5))),
     ('loosely dashdotdotted', (0, (3, 10, 1, 10, 1, 10))),
     ('densely dashdotdotted', (0, (3, 1, 1, 1, 1, 1)))]
# style_options = ['-', '--', '-.', ':', 'solid', 'dashed', 'dashdot', 'dotted']
def get_style(param):
    global style_cache
    if param not in style_cache:
        style_cache[param] = linestyle_tuple.pop(0)[1]
    return style_cache[param]

    
# for each unique value, plot the cdf
for combined in unique_combined:
    # filter the data
    compact_filtered_df = compact_df[compact_df["combined"] == combined]
    random_filtered_df = random_df[random_df["combined"] == combined]
    
    fig, axes = plt.subplots(1, 2, figsize=(9, 3), sharey=True)
    

    for i, exp in enumerate([("Random Placement", random_filtered_df), ("Compact Placement", compact_filtered_df)]): 
        placement_type, placement_df = exp
        this_ax = axes[i]

        # how many items are in the placement_df? print: 
        if placement_df.shape[0] != 1:
            print(f"Error: {placement_type} has more than one row for {combined}")
        
        # get the first line 
        first_line = placement_df.iloc[0]
            
        max_value = 1
        min_value = 1
        
        avg_values = {} 
        
        # plot the cdf
        for cdf_param in cdf_params:
            values = first_line[cdf_param]
            # values is a string. We need to convert it to a list of floats
            values = json.loads(values)
            
            values = sorted(values)
            yvals = np.arange(len(values))/float(len(values) - 1)
            
            avg_value = np.mean(values)
            
            max_value = max(max_value, max(values)) 
            min_value = min(min_value, min(values)) 
                    
            label = cdf_param 
            if cdf_param.endswith("_values"):
                # remove the _values suffix
                label = cdf_param[:-7]
            
            label = f"{label} ({avg_value:.2f} X)"
            
            avg_values[label] = avg_value
                
            this_ax.plot(values, yvals, 
                         label=label, 
                         marker=get_marker(cdf_param),
                         markevery=len(values) // 10,
                         markersize=3,
                         linewidth=1,
                        #  linestyle=get_style(cdf_param)
                         )     
        
        
        # tick vertical line at x = 1 
        this_ax.axvline(x=1, color="black", linestyle="-")
        
        this_ax.set_ylim(0, 1)
        this_ax.set_xlim(min_value - 0.1, max_value + 0.1)
        
        this_ax.set_title(placement_type)
        this_ax.set_xlabel("Value")
        this_ax.set_ylabel("CDF")
        
        
        # sort the legend iterms based on the average value (that was calculated above with an X mark)
        handles, labels = this_ax.get_legend_handles_labels()
        labels, handles = zip(*sorted(zip(labels, handles), key=lambda t: avg_values[t[0]]))
        this_ax.legend(handles, labels, loc='upper center', bbox_to_anchor=(0.5, -0.25))
        
        
    plt.suptitle(combined, y=1.05)
        
    plot_path = "{}/{}.png".format(plots_dir, combined)
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    plt.clf()
    
    