import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

import sys 
import os 
import json  

placement_names = sys.argv[1].split(",") 
placement_csv_paths = sys.argv[2].split(",")
separating_params = sys.argv[3].split(",")  # each combination will create a separate plot
cdf_params = sys.argv[4].split(",") # these columns should contain a list, for which the cdf will be plotted
plots_dir = sys.argv[5]

os.makedirs(plots_dir, exist_ok=True)

# load the data


# combine the separate params in a column looking like "param1=value1,param2=value2"
# random_df["combined"] = random_df.apply(lambda row: ",".join([f"{param}={row[param]}" for param in separating_params]), axis=1)

placement_dfs = [] 

for placement_name, placement_csv_path in zip(placement_names, placement_csv_paths):
    placement_df = pd.read_csv(placement_csv_path)
    placement_df["combined"] = placement_df.apply(lambda row: ",".join([f"{param}={row[param]}" for param in separating_params]), axis=1)
    placement_dfs.append((placement_name, placement_df))        


# unique values of the combined column
unique_combined = placement_dfs[0][1]["combined"].unique()

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

def get_style(param):
    global style_cache
    if param not in style_cache:
        style_cache[param] = linestyle_tuple.pop(0)[1]
    return style_cache[param]

    
# for each unique value, plot the cdf
for combined in unique_combined:
    filtered_placement_dfs = [] 
    for placement_name, placement_df in placement_dfs:
        filtered_placement_df = placement_df[placement_df["combined"] == combined]
        filtered_placement_dfs.append((placement_name, filtered_placement_df))         
    
    placement_count = len(filtered_placement_dfs) 
    
    fig, axes = plt.subplots(1, placement_count, figsize=(3 * placement_count * 1.5, 3), sharey=True, sharex=True)
    
    max_value = 1
    min_value = 1
    
    for i, exp in enumerate(filtered_placement_dfs): 
        placement_name, placement_df = exp
        
        if placement_count == 1:
            this_ax = axes
        else:
            this_ax = axes[i]

        # how many items are in the placement_df? print: 
        if placement_df.shape[0] != 1:
            print(f"Error: {placement_name} has more than one row for {combined}")
        
        # get the first line 
        first_line = placement_df.iloc[0]
            
        
        avg_values = {} 
        
        # plot the cdf
        for cdf_param in cdf_params:
            # values is a string. We need to convert it to a list of floats
            values = json.loads(first_line[cdf_param])
            
            values = sorted(values)
            
            if len(values) > 1:
                yvals = np.arange(len(values)) / float(len(values) - 1)
            else: 
                yvals = [1]
                
            avg_value = np.mean(values)
            max_value = max(max_value, max(values)) 
            min_value = min(min_value, min(values)) 
                    
            label = cdf_param 
            if cdf_param.endswith("_values"):
                # remove the _values suffix
                label = cdf_param[:-7]
            label = f"{label} ({avg_value:.2f} X)"
            avg_values[label] = avg_value
            
            markevery = len(values) // 10 
            if markevery == 0:
                markevery = 1
                
            this_ax.plot(values, yvals, 
                         label=label, 
                         marker=get_marker(cdf_param),
                         markevery=markevery,
                         markersize=3,
                         linewidth=1,
                         #  linestyle=get_style(cdf_param)
            )     
        
        
        # tick vertical line at x = 1 
        this_ax.axvline(x=1, color="black", linestyle="-")
        
        this_ax.set_ylim(0, 1)
        
        this_ax.set_title(placement_name)
        this_ax.set_xlabel("Value")
        this_ax.set_ylabel("CDF")
        
        
        # sort the legend iterms based on the average value (that was calculated above with an X mark)
        handles, labels = this_ax.get_legend_handles_labels()
        labels, handles = zip(*sorted(zip(labels, handles), key=lambda t: avg_values[t[0]]))
        this_ax.legend(handles, labels, loc='upper center', bbox_to_anchor=(0.5, -0.25))
        
        # if this is the last: 
        if i == placement_count - 1:
            this_ax.set_xlim(min_value - 0.1, max_value + 0.1)    
    
    plt.suptitle(combined, y=1.05)
        
    plot_path = "{}/{}.png".format(plots_dir, combined)
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    plt.clf()
    
    