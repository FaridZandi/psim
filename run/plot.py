
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import sys



############################################################################################
############################################################################################

bar_width = 0.2
# the params that might vary in the experiments
all_sweep_params = ["lb-scheme", "priority-allocator", "load-metric"]

colors = {
    "random": "red",
    "roundrobin": "orange",
    "leastloaded": "yellow",
    "powerof2": "gray",
    "powerof3": "wheat",
    "powerof4": "black",
    "robinhood": "pink",
    "futureload-register": "purple",
    "futureload-allocated": "blue",
    "futureload-utilization": "darkgreen",
}


############################################################################################
############################################################################################

# read the data from the csv
csv_path = sys.argv[1]
results_dir = csv_path[:csv_path.rfind("/")] + "/"
pd_frame = pd.read_csv(csv_path)

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
# some magical stuff to find the proper offset for each bar.
# It seems to be working, but I'm not I know why.
############################################################################################
max_group_width = 0

group_sizes = []
for param in sweep_params:
    unique_params = len(pd_frame[param].unique())
    group_sizes.append(unique_params)
group_sizes.reverse()
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

    for k, param_comb in enumerate(param_combination):
        total_offset += (sub_group_width * param_comb)
        sub_group_width *= (group_sizes[k] + 1)
    param_offset[i] = total_offset
    print(i, param_combination, params[i], total_offset)

    max_group_width = max(max_group_width, total_offset)

############################################################################################
############################################################################################

def get_color(mech):
    if mech in colors:
        return colors[mech]
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


group_width = float(max_group_width + bar_width)  # bar_width * len(params)
group_spacing = float(max_group_width) / 2

# len(protocols) items, with (group_width + group_spacing) space between each two items
x = np.arange(len(protocols)) * (group_width + group_spacing)

print(max_group_width, group_width, group_spacing)
print(x)

plt.figure(figsize=(len(protocols) * len(params) / 2, 10))

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


plt.xticks(x, protocols)
plt.xticks(rotation=45)
plt.xlabel("Protocol")
# legend outside the plot
plt.ylabel("Normalized Psim Time")

plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)


plot_name_pdf = results_dir + "results.pdf"
plot_name_png = results_dir + "results.png"
plt.savefig(plot_name_pdf, bbox_inches="tight", dpi=300)
plt.savefig(plot_name_png, bbox_inches="tight", dpi=300)
