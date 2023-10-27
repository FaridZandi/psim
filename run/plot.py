
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import sys
from pprint import pprint


############################################################################################
############################################################################################

bar_width = 0.2
# the params that might vary in the experiments     
all_sweep_params = ["min-rate", 
                    "ft-core-count", 
                    "ft-agg-core-link-capacity-mult",
                    "shuffle-device-map", 
                    "priority-allocator", 
                    "load-metric", 
                    "lb-scheme"]

colors = {
    "random": "red",
    "roundrobin": "orange",
    "leastloaded": "yellow",
    "powerof2": "gray",
    "powerof3": "wheat",
    "powerof4": "black",
    "robinhood": "pink",
    "futureload": "blue",
}


############################################################################################
############################################################################################

# read the data from the csv
csv_path = sys.argv[1]
results_dir = csv_path[:csv_path.rfind("/")] + "/"
pd_frame = pd.read_csv(csv_path)

# convert param columns to string
for param in all_sweep_params:
    if param in pd_frame:
        is_number = pd_frame[param].dtype.kind in 'bifc'
        if is_number:
            max_value = pd_frame[param].max()
            digits = len(str(max_value))
            # convert to string, pad with zeros to make sure the sorting is correct
            pd_frame[param] = pd_frame[param].astype(str).str.zfill(digits)

sweep_params = [] 
# find which of the params are constant in the csv. remove them from the list
for param in all_sweep_params:
    if param in pd_frame:
        if len(pd_frame[param].unique()) > 1:
            sweep_params.append(param)


# combine these params into a single column
pd_frame["params"] = ""
for param in sweep_params:
    pd_frame["params"] += pd_frame[param] + ","
pd_frame["params"] = pd_frame["params"].str.strip(",")
pd_frame = pd_frame.sort_values(by=["protocol-file-name", "params"])


# find the protocols and params
pd_frame.reindex()
protocols = pd_frame["protocol-file-name"].unique()
params = pd_frame["params"].unique()
print("protocols:", protocols)
print("params:", params)



############################################################################################
############################################################################################
############################################################################################
# some magical stuff to find the proper offset for each bar.
# overall the idea is to covert the params into a bunch of numbers, which shows the 
# combinations of the params. Then, for each combination, find the offset of the bar
# in the plot. 
# It seems to be working, but I'm not sure I know why. 
############################################################################################
max_group_width = 0 
tick_labels = {}

group_sizes = []
group_sizes_labels = []

for param in sweep_params: 
    unique_params = len(pd_frame[param].unique())
    group_sizes.append(unique_params)
    group_sizes_labels.append(param)
    
group_sizes.reverse()
group_sizes_labels.reverse()

print(group_sizes_labels)
print(group_sizes[0])
 
all_params_count = np.prod(group_sizes)
param_offset = {}

for i in range (all_params_count):

    this_param = i
    param_combination = []

    for j in range(len(group_sizes)):
        param_combination.append(this_param % group_sizes[j])
        this_param = this_param // group_sizes[j]

    total_offset = 0
    sub_group_width = bar_width
    # param_combination.reverse()
    
    grouping_label = ""
    if param_combination[0] == 0:
        # this is the first item in some subgroup. What is the description of this grouping? 
        # the label should contain anything after this (indices 1 to end). For everyone, add 
        # the label and its corresponding value to the grouping label. 
        for k in range(1, len(param_combination)):
            value = params[i].split(",")[len(param_combination) - k - 1]
            grouping_label += group_sizes_labels[k] + "  =  " + value + "\n"
            
        grouping_label = grouping_label.strip("\n")
        
            
    for k, param_comb in enumerate(param_combination):
        total_offset += (sub_group_width * param_comb)
        sub_group_width *= (group_sizes[k] + 1)
            
    param_offset[i] = total_offset    
    print(i, param_combination, params[i], total_offset)
    
    if grouping_label != "":
        tick_labels[total_offset] = grouping_label
            
    max_group_width = max(max_group_width, total_offset)
    
# pprint(tick_labels)
############################################################################################
############################################################################################
############################################################################################
############################################################################################

def get_color(mech):
    for color in colors:
        if color in mech:
            return colors[color]
    else:
        return None

# for each protocol, normalize the times, the max max_time is 1, and everything else is relative to that
pd_frame["rel_max_psim_time"] = 0
pd_frame["rel_min_psim_time"] = 0
pd_frame["rel_last_psim_time"] = 0

for protocol in protocols:
    protocol_data = pd_frame[pd_frame["protocol-file-name"] == protocol]
    max_max_time = protocol_data["max_psim_time"].max()

    for index, row in protocol_data.iterrows():
        pd_frame.loc[index, "rel_max_psim_time"] = row["max_psim_time"] / max_max_time
        pd_frame.loc[index, "rel_min_psim_time"] = row["min_psim_time"] / max_max_time
        pd_frame.loc[index, "rel_last_psim_time"] = row["last_psim_time"] / max_max_time

        if row["rel_last_psim_time"] > 1:
            print("error: rel_last_psim_time > 1")
            print(row)

inner_grouping_size = group_sizes[0]
sub_group_width = bar_width * inner_grouping_size
group_width = float(max_group_width + bar_width)  # bar_width * len(params)
group_spacing = float(max_group_width) / 2

# len(protocols) items, with (group_width + group_spacing) space between each two items
x = np.arange(len(protocols)) * (group_width + group_spacing)

print(max_group_width, group_width, group_spacing)
print(x)

plt.figure(figsize=(len(protocols) * len(params) / 4, 5))

for i, param in enumerate(params):

    param_data = pd_frame[pd_frame["params"] == param]
    x_offset = x + (param_offset[i] - group_width / 2)

    if "futureload" in param:
        plt.bar(x_offset, param_data["rel_last_psim_time"],
            width=bar_width, label=param,
            color=get_color(param), edgecolor="black", linewidth=2)

    else:
        plt.bar(x_offset, param_data["rel_max_psim_time"],
            width=bar_width, color="white",
            edgecolor="black", hatch="///", linewidth=2)


        plt.bar(x_offset, param_data["rel_min_psim_time"],
                width=bar_width, label=param,
                color=get_color(param), edgecolor="black", linewidth=2)


# xticks on the top of the plot 
plt.xticks(x, protocols)
plt.tick_params(axis='x', which='both', bottom=False, top=True, labelbottom=False, labeltop=True)

# the labels
plt.xlabel("Protocol")
plt.ylabel("Normalized Psim Time")

# keep the first inner_grouping_size items in the legend
handles, labels = plt.gca().get_legend_handles_labels()
limited_handles = handles[:inner_grouping_size]
limited_labels = labels[:inner_grouping_size]
for i, label in enumerate(limited_labels):
    limited_labels[i] = limited_labels[i].split(",")[-1]
plt.legend(limited_handles, limited_labels, bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)

# annotate the tick_labels on the bottom of the plot
start_offset = 0
for i, protocol in enumerate(protocols):
    for offset, text in tick_labels.items():
        this_offset = start_offset + offset - group_width / 2 + sub_group_width / 2
        plt.annotate(text, (this_offset, 0), xytext=(0, -20), textcoords="offset points",
                 rotation=45, ha='center', va='top')
    start_offset += (group_width + group_spacing)
    

plot_name_pdf = results_dir + "plot.pdf"
plot_name_png = results_dir + "plot.png"
plt.savefig(plot_name_pdf, bbox_inches="tight", dpi=300)
plt.savefig(plot_name_png, bbox_inches="tight", dpi=300)
