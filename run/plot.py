
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import sys
from pprint import pprint


############################################################################################
############################################################################################

# the params that might vary in the experiments     
all_sweep_params_default = [
    "lb-scheme", 
    
    "min-rate",
    "ft-core-count",
    "machine-count", 
    # "ft-agg-core-link-capacity-mult",
    "shuffle-device-map",
    "priority-allocator",
    # "load-metric",
    "general-param-2", 
    "general-param-3", 
    "general-param-1", 
    "general-param-4", 
    "general-param-5",
    "general-param-6",
    "general-param-7",
    "general-param-8",
    "general-param-9",
    "general-param-10",
    "placement-mode",
    
]


############################################################################################
############################################################################################

# read the data from the csv
csv_path = sys.argv[1]
results_dir = csv_path[:csv_path.rfind("/")] + "/"
pd_frame = pd.read_csv(csv_path)

print(pd_frame.columns)


if len(sys.argv) > 2:
    plot_path = sys.argv[2]
else:
    plot_path = results_dir + "/plot.png" 
    
if len(sys.argv) > 3:
    all_sweep_params = sys.argv[3].split(",")
else:
    all_sweep_params = all_sweep_params_default


if len(sys.argv) > 5:
    plotted_key_min = sys.argv[4]
    plotted_key_max = sys.argv[5]
else:
    plotted_key_min = "rel_max_psim_time"
    plotted_key_max = "rel_min_psim_time"
    input("warning: no plotted key specified, press enter to continue. rel_max_psim_time is going to be used, which will probably lead to errors.")

if len(sys.argv) > 6:
    title = sys.argv[6] 
else:
    title = "Comparison of Numbers"

if len(sys.argv) > 7:
    random_seed = int(sys.argv[7])
else:
    random_seed = 0
    

    
np.random.seed(random_seed)    
    
# pad the numbers with zeros to make sure the sorting is correct
# lexicographically, 10 will come before 2. 
# but 02 will come before 10, which is what we want 
for param in all_sweep_params:
    if param in pd_frame:
        is_number = pd_frame[param].dtype.kind in 'bifc'
        if is_number:
            max_value = pd_frame[param].max()
            digits = len(str(max_value))
            # convert to string, pad with zeros to make sure the sorting is correct
            pd_frame[param] = pd_frame[param].astype(str).str.zfill(digits)


# convert param columns to string
################################

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
################################


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
# overall the idea is to convert the params into a bunch of numbers, which shows the
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
    sub_group_width = 1
    # param_combination.reverse()

    grouping_label = ""
    if param_combination[0] == 0:
        # this is the first item in some subgroup. What is the description of this grouping?
        # the label should contain anything after this (indices 1 to end). For everyone, add
        # the label and its corresponding value to the grouping label.
        for k in range(1, len(param_combination)):
            value = params[i].split(",")[len(param_combination) - k - 1]
            grouping_label += group_sizes_labels[k] + " = " + value + ",    "

        grouping_label = grouping_label.strip("\n")

    for k, param_comb in enumerate(param_combination):
        total_offset += (sub_group_width * param_comb)
        if k == 0: 
            sub_group_width *= (group_sizes[k] + 3)
        else: 
            sub_group_width *= (group_sizes[k] + 2) 
            
    param_offset[i] = total_offset    
    print(i, param_combination, params[i], total_offset)

    if grouping_label != "":
        tick_labels[total_offset] = grouping_label

    max_group_width = max(max_group_width, total_offset)

pprint(tick_labels)

############################################################################################
############################################################################################
############################################################################################
############################################################################################

colorings = {}

 
def get_color(color_specifier):
    if color_specifier in colorings:
        return colorings[color_specifier]
    else:
        # get a random color
        colorings[color_specifier] = np.random.rand(3,)
        
        return colorings[color_specifier]

plot_ylim_min = pd_frame[plotted_key_min].min()
plot_ylim_max = pd_frame[plotted_key_max].max()

inner_grouping_size = group_sizes[0]
sub_group_width = inner_grouping_size
group_width = float(max_group_width + 1) 
group_spacing = float(max_group_width) / 2
total_width = group_width * len(protocols) + group_spacing * (len(protocols) - 1)

# len(protocols) items, with (group_width + group_spacing) space between each two items
x = np.arange(len(protocols)) * (group_width + group_spacing)

# print(max_group_width, group_width, group_spacing)
print ("max_group_width:", max_group_width, ",group_width:", group_width, ",group_spacing:", group_spacing, ", total_width:", total_width)
plt.figure(figsize=(total_width / 10, 5))


for i, param in enumerate(params):

    param_data = pd_frame[pd_frame["params"] == param]
    x_offset = x + (param_offset[i] - group_width / 2)
    coloring_basis = param.split(",")[-1]
    
    plt.bar(x_offset, param_data[plotted_key_max] - param_data[plotted_key_min],
            bottom=param_data[plotted_key_min],
            width=1, label=param,
            color=get_color(coloring_basis), edgecolor="black")
    

# xticks on the top of the plot
plt.xticks(x, protocols)
plt.tick_params(axis='x', which='both', bottom=False, top=True, labelbottom=False, labeltop=True)
plt.xticks(rotation=90)
plt.ylim(plot_ylim_min * 0.9, plot_ylim_max * 1.1)

# the labels
plt.xlabel("Protocol")
plt.ylabel("Normalized Psim Time")

# keep the first inner_grouping_size items in the legend
handles, labels = plt.gca().get_legend_handles_labels()
limited_handles = handles[:inner_grouping_size]
limited_labels = labels[:inner_grouping_size]
for i, label in enumerate(limited_labels):
    limited_labels[i] = limited_labels[i].split(",")[-1]
plt.legend(limited_handles, limited_labels, bbox_to_anchor=(1, 1), loc='upper left', borderaxespad=0.)

# annotate the tick_labels on the bottom of the plot
start_offset = 0
for i, protocol in enumerate(protocols):
    
    for offset, text in tick_labels.items():
        
        this_offset = start_offset + offset - group_width / 2 + sub_group_width / 2
        text = text.strip()
        a = plt.annotate(text, xy=(this_offset, plot_ylim_min * 0.9), xytext=(0, -20), textcoords="offset points",
                     rotation=45, ha='right', va='top') 
    
        # plot vertical lines to separate the groups on either sides of the tick_labels
        # plt.axvline(x=this_offset - sub_group_width, color="black", linestyle="--")  
        # plt.axvline(x=this_offset + sub_group_width, color="black", linestyle="--")  
    
            
    start_offset += (group_width + group_spacing)


plt.savefig(plot_path, bbox_inches="tight", dpi=300)
