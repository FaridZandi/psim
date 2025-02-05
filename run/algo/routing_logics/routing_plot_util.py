import matplotlib.pyplot as plt
import numpy as np  

from copy import deepcopy

import os
import sys 

def get_color(job_id):  
    return plt.cm.tab20.colors[job_id % 20] 

def plot_link_usage(ax, time_range, 
                    usage, smoothed_usage,   
                    link_label,
                    leaf, spine, direction, 
                    min_affected_time, max_affected_time):
    
    # draw plt.stackplot for the jobs
    usage_stacks = []   
    job_ids = []
    
    for job_id, job_usage in usage.items():
        usage_stacks.append(job_usage[leaf][spine][direction])  
        job_ids.append(job_id) 
        
    ax.stackplot(time_range, usage_stacks, 
                 labels=[f'Job {job_id}' for job_id in job_ids], 
                 colors=[get_color(job_id) for job_id in job_ids])      
    
    # plot a horizontal line at y=100 
    ax.axhline(y=100, color='black', linestyle='--')
    
    ax.set_title(f'{link_label} - {direction}')
    ax.set_xlabel('Time')
    ax.set_ylabel('Remaining Capacity')
    
    ax.grid(True)
    ax.set_xlim(min_affected_time - 100, max_affected_time + 100)

def plot_routing(run_context, rem, usage, all_job_ids, num_leaves, 
               num_spines, routing_time, min_affected_time, 
               max_affected_time, plots_dir, smoothing_window=1, suffix=""): 
    
    if "visualize-routing" in run_context and not run_context["visualize-routing"]:
        return  
    
    sys.stderr.write("plotting for smoothing window {} ...\n".format(smoothing_window)) 
    
    time_range = range(routing_time)
    smoothed_rem = deepcopy(rem) 
    smoothed_usage = deepcopy(usage)    
    
    if smoothing_window > 1:
        for leaf in range(num_leaves):
            for spine in range(num_spines):
                for direction in ["up", "down"]:
                    smthed = np.convolve(smoothed_rem[leaf][spine][direction], 
                                         np.ones(smoothing_window) / smoothing_window, 
                                         mode='same')

                    smoothed_rem[leaf][spine][direction] = smthed 
                    
                    for job in all_job_ids: 
                        smthed = np.convolve(smoothed_usage[job][leaf][spine][direction], 
                                             np.ones(smoothing_window) / smoothing_window, 
                                             mode='same')
                        smoothed_usage[job][leaf][spine][direction] = smthed                    
            

    # Create a figure with multiple subplots
    total_subplots = num_leaves * num_spines * 2  # Two plots (up and down) per leaf-spine pair
    fig, axes = plt.subplots(num_leaves, num_spines * 2, 
                             sharey=True, sharex=True,
                             figsize=(num_spines * 8, num_leaves * 3), 
                             constrained_layout=True, squeeze=False)

    # Iterate over each leaf, spine pair to plot its remaining capacity
    for leaf in range(num_leaves):
        for spine in range(num_spines):
            # Plot "up" direction
            ax_up = axes[leaf, spine]
            
            plot_link_usage(ax_up, time_range,  
                            usage, smoothed_usage, 
                            f'Leaf {leaf}, Spine {spine}', leaf, spine, 'up', 
                            min_affected_time, max_affected_time)   

            # Plot "down" direction
            ax_down = axes[leaf, spine + num_spines] 
            
            plot_link_usage(ax_down, time_range,    
                            usage, smoothed_usage, 
                            f'Leaf {leaf}, Spine {spine}', leaf, spine, 'down', 
                            min_affected_time, max_affected_time)   
            
    # Give a super title to the whole figure
    fig.suptitle('Remaining Bandwidth for Each Link (Up and Down)', fontsize=16)

    # Save the entire subplot grid
    for ext in ['pdf', 'png']:
        plt_path = os.path.join(plots_dir, 'remaining_{}_{}.{}'.format(smoothing_window, suffix, ext))
        plt.savefig(plt_path)
    plt.close(fig)

    sys.stderr.write("Combined subplot figure has been saved in the directory: {}\n".format(plots_dir))



def plot_time_ranges(ranges_dict, merged_ranges_dict, plot_path):
    # two plots on top of each other
    
    fig, axes = plt.subplots(2, 1, figsize=(10, 5), sharex=True)   
    
    def plot_stuff(ax, data, other_ax=None):
        y = 0
        for key, ranges in data.items():
            for i, (start, end) in enumerate(ranges):
                ax.plot([start, end], [y, y], marker='|', label=f"{key}" if y == 0 else "")
                if other_ax is not None:
                    ax.axvline(x=start, color='gray', linestyle='--', linewidth=0.5)
                    other_ax.axvline(x=start, color='gray', linestyle='--', linewidth=0.5)
                    ax.axvline(x=end, color='gray', linestyle='--', linewidth=0.5)
                    other_ax.axvline(x=end, color='gray', linestyle='--', linewidth=0.5)
            y += 1
            
            
        
        ax.set_yticks(range(len(data)))
        ax.set_yticklabels(list(data.keys()))
        ax.set_xlabel("Time")
        ax.set_title("Time Ranges by Key")
        
    plot_stuff(axes[0], ranges_dict)    
    plot_stuff(axes[1], merged_ranges_dict, axes[0])
    
    plt.savefig(plot_path, bbox_inches='tight', dpi=300)
    