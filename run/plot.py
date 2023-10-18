
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import sys

csv_path = sys.argv[1]
results_dir = csv_path[:csv_path.rfind("/")] + "/"

all_pd_frame = pd.read_csv(csv_path)
all_pd_frame = all_pd_frame.sort_values(by=["protocol-file-name", "core-selection-mechanism"])

colors = {
    "random": "red",
    "roundrobin": "orange",
    "leastloaded": "yellow",
    "powerof2": "gray",
    "robinhood": "pink",
    "futureload-register": "purple",
    "futureload-allocated": "blue",
    "futureload-utilization": "darkgreen",
}

def get_color(mech):
    if mech in colors:
        return colors[mech]
    else:
        return None

all_pd_frame.reindex()
protocols = all_pd_frame["protocol-file-name"].unique()
core_selection_mechanisms = all_pd_frame["core-selection-mechanism"].unique()
priority_allocators = all_pd_frame["priority-allocator"].unique()

for allocator in priority_allocators:
    pd_frame = all_pd_frame[all_pd_frame["priority-allocator"] == allocator]
    print(pd_frame)

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

    print(pd_frame)


    print("protocols:", protocols)
    print("core selection mechanisms:", core_selection_mechanisms)

    bar_width = 0.2
    group_width = bar_width * len(core_selection_mechanisms)
    group_spacing = 1

    # len(protocols) items, with (group_width + group_spacing) space between each two items
    x = np.arange(len(protocols)) * (group_width + group_spacing)

    plt.figure(figsize=(len(protocols) * 2, 10))

    for i, mech in enumerate(core_selection_mechanisms):

        mech_data = pd_frame[pd_frame["core-selection-mechanism"] == mech]
        x_offset = x + (i * bar_width - group_width / 2)

        if mech.startswith("futureload"):
            plt.bar(x_offset, mech_data["rel_last_psim_time"],
                width=bar_width, label=mech,
                color=get_color(mech), edgecolor="black", linewidth=2)

        else:
            plt.bar(x_offset, mech_data["rel_max_psim_time"],
                width=bar_width, color="white",
                edgecolor="black", hatch="///", linewidth=2)

            plt.bar(x_offset, mech_data["rel_max_psim_time"],
                    width=bar_width, color=get_color(mech), alpha=0.5,
                    edgecolor="black", hatch="\\\\\\", linewidth=2)

            plt.bar(x_offset, mech_data["rel_min_psim_time"],
                    width=bar_width, label=mech,
                    color=get_color(mech), edgecolor="black", linewidth=2)

            plt.plot(x_offset, mech_data["rel_last_psim_time"],
                    marker="o", color="white", markersize=4, markeredgecolor="black", linestyle="None")


    plt.xticks(x, protocols)
    plt.xticks(rotation=90)
    plt.legend()
    plt.ylabel("Normalized Psim Time")

    plot_name = results_dir + "{}.pdf".format(allocator)
    plt.savefig(plot_name, bbox_inches="tight", dpi=300)
