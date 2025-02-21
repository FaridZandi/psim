import sys
import pandas as pd
import seaborn as sns   
from matplotlib import pyplot as plt    
import argparse 

plot_params = None
subplot_x_params = None 
subplot_y_params = None 
subplot_hue_params = None
plot_x_params = None
plot_y_param = None
plot_type = None 

ext = "pdf" 

hue_color_options = ["blue", "red", "green", "orange", "purple", "brown", 
                     "pink", "gray", "olive", "cyan", "black", "yellow"] * 100

def annotate(ax):
    for p in ax.patches:
        
        # if the patch is dark, use white text color
        # otherwise, use black text color
        # being dark means the sum of the RGB values is less than 1
        if p.get_facecolor()[0] + p.get_facecolor()[1] + p.get_facecolor()[2] < 1.5:
            color = 'white'
        else:
            color = 'black'
               
        if p.get_height() > 0:      
            ax.annotate(
                f'{p.get_height():.2f}',  # Format to display integer
                (p.get_x() + p.get_width() / 2., p.get_height()),  # Position above the bar
                ha='center',  # Center horizontally
                va='top',  # Align to bottom of text
                rotation=90,  # Rotate text 90 degrees
                fontsize=6,  # Font size
                color=color,  # Font color
            )
        
        
def draw_subplot(df, x_value, y_value, ax, hue_order, legend, subplot_y_len, plot_type, val_range):        
    if x_value is not None: 
        df = df[df[subplot_x_params] == x_value]     
    if y_value is not None: 
        df = df[df[subplot_y_params] == y_value]
    
    if len(df) == 0:
        return  
    
    if plot_type == "line": 
        sns.lineplot(x=plot_x_params, y=plot_y_param, 
                    hue=subplot_hue_params, hue_order=hue_order, 
                    palette=hue_color_options[:len(hue_order)],    
                    data=df, ax=ax, legend=legend, errorbar=('ci', 50))
        ax.axhline(y=1, color='black', linestyle='--')

    elif plot_type == "heatmap":  
        # the rows will be the plot_x_params 
        # the columns will be subplot_hue_params
        # the values will be plot_y_param
        
        
        df_pivoted = df.pivot_table(
            index=plot_x_params,
            columns=subplot_hue_params,
            values=plot_y_param,
            aggfunc='mean'  # or 'sum', 'median', etc.
        )        
        
        # with borders on the heatmap
        
        df_pivoted -= 1
        df_pivoted *= 100 
        df_pivoted = df_pivoted.round(0)
        
        cbar_min = int((val_range[0] - 1) * 100)
        cbar_max = int((val_range[1] - 1) * 100)
        
        sns.heatmap(df_pivoted, ax=ax,
                    fmt=".0f", 
                    cmap="YlGnBu", annot=True, 
                    annot_kws={"size": 6},
                    linewidths=0.5, 
                    vmin=cbar_min, vmax=cbar_max
                    )        
        
        ax.set_ylabel(plot_x_params)
        ax.set_xlabel(subplot_hue_params)
        
        legend = False
        
            
    elif plot_type == "bar":    
        sns.barplot(x=plot_x_params, y=plot_y_param, 
                    hue=subplot_hue_params, hue_order=hue_order, 
                    palette=hue_color_options[:len(hue_order)],    
                    data=df, ax=ax, errorbar=None)     
        
        annotate(ax)
        ax.axhline(y=1, color='black', linestyle='--')
        
        if not legend:
            ax.get_legend().remove()
    
    elif plot_type == "box":    
        sns.boxplot(x=plot_x_params, y=plot_y_param, 
                    hue=subplot_hue_params, hue_order=hue_order, 
                    palette=hue_color_options[:len(hue_order)],    
                    data=df, ax=ax)
        
        if not legend:
            ax.get_legend().remove()
    
    if plot_type == "violin":
        sns.violinplot(x=plot_x_params, y=plot_y_param,
                        hue=subplot_hue_params, hue_order=hue_order, 
                        palette=hue_color_options[:len(hue_order)],    
                        data=df, ax=ax, inner="quartile")
        
        if not legend:
            ax.get_legend().remove()
    
    elif plot_type == "cdf":
        # draw a cdf plot
        for i, hue in enumerate(hue_order):
            
            data = df[df[subplot_hue_params] == hue][plot_y_param]  

            sns.kdeplot(data, cumulative=True, ax=ax, label=hue, warn_singular=False, 
                        color=hue_color_options[i])
            
            # sns.kdeplot(data, fill=True, common_norm=False, alpha=0.5, 
            #             ax=ax, label=hue, warn_singular=False, 
            #             color=hue_color_options[i])

            
        ax.axvline(x=1, color='black', linestyle='--')  
        
    if legend:
        ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.3), ncol=1)  
            
    # draw a horizontal line at y=1
    
    if y_value is not None and subplot_y_params is not None:
        ax.set_title(f"{subplot_x_params}={x_value}\n {subplot_y_params}={y_value}")
    elif x_value is not None and subplot_x_params is not None:
        ax.set_title(f"{subplot_x_params}={x_value}")
    elif y_value is not None and subplot_y_params is not None:
        ax.set_title(f"{subplot_y_params}={y_value}")   

    ax.title.set_size(8)

    if plot_type != "heatmap":
        ax.set_xlabel(plot_x_params)
    

def draw_plot(df, value, file_dir, hue_order, plot_type):   
    if value is not None: 
        df = df[df[plot_params] == value]
    
    min_value = df["values"].min()
    max_value = df["values"].max()
    val_range = (min_value, max_value)
    
    if subplot_x_params is None: 
        subplot_x_len = 1 
        subplot_x_values = [None]
    else:
        subplot_x_len = len(df[subplot_x_params].unique())  
        subplot_x_values = df[subplot_x_params].unique() 
        
    if subplot_y_params is None:
        subplot_y_len = 1
        subplot_y_values = [None]   
    else:
        subplot_y_len = len(df[subplot_y_params].unique())
        subplot_y_values = df[subplot_y_params].unique()    
    
    if plot_x_params is not None:
        plot_x_len = len(df[plot_x_params].unique()) 
    else:
        plot_x_len = 1
    
    if subplot_hue_params is not None:
        hue_len = len(df[subplot_hue_params].unique()) 
    else:
        hue_len = 1 
        
    width = subplot_x_len * 3
    height = subplot_y_len * 3 
    
    # print(f"width: {width}, height: {height}")
    
    fig, axes = plt.subplots(subplot_y_len, subplot_x_len,
                             sharey=True,   
                             sharex=True,
                             squeeze=False)
    
    fig.set_figwidth(width) 
    fig.set_figheight(height)
      
    plt.subplots_adjust(hspace=0.5)
    # plt.subplots_adjust(wspace=0.5)
    
    for i, x_value in enumerate(subplot_x_values):
        for j, y_value in enumerate(subplot_y_values):
            ax = axes[j, i]
            legend = False
            
            if i == len(subplot_x_values) // 2:
                # if j == len(subplot_y_values) - 1:
                # select the middle subplot to draw the legend
                if j == len(subplot_y_values) - 1:
                    legend = True   
                
            draw_subplot(df, x_value, y_value, ax, hue_order, legend, subplot_y_len, plot_type, val_range)  
            
    plt.savefig(f"{file_dir}/plot_{value}_{plot_type}.{ext}", bbox_inches='tight', dpi=200)        
                
    
def main(file_name, file_dir, plot_type): 
    # read the csv file into pd dataframe
    df = pd.read_csv(file_name)
    
    df["values"] = df["values"].apply(lambda x: [float(i) for i in x[1:-1].split(",")])
    df = df.explode("values")
    

    
    df["values"] = df["values"].astype(float)
    
    if subplot_hue_params is not None:
        hue_order = df[subplot_hue_params].unique() 
    else:
        hue_order = None
        
    if plot_params is None:  
        unique_values = [None]
    else:   
        unique_values = df[plot_params].unique()

    for value in unique_values: 
        print(f"value: {value}, plot_type: {plot_type}")    
        draw_plot(df, value, file_dir, hue_order, plot_type)    
        

if __name__ == "__main__":
    
    # parse the arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--file_name", type=str, required=True)
    parser.add_argument("--plot_params", type=str, required=False)
    parser.add_argument("--subplot_x_params", type=str, required=False)
    parser.add_argument("--subplot_y_params", type=str, required=False)
    parser.add_argument("--subplot_hue_params", type=str, required=False)
    parser.add_argument("--plot_x_params", type=str, required=False)
    parser.add_argument("--plot_y_param", type=str, required=False)
    parser.add_argument("--plot_type", type=str, required=False)
        
    args = parser.parse_args()
        
    file_dir = "/".join(args.file_name.split("/")[:-1]) 
    
    if args.plot_params is not None:
        plot_params = args.plot_params
    if args.subplot_x_params is not None:
        subplot_x_params = args.subplot_x_params
    if args.subplot_y_params is not None:
        subplot_y_params = args.subplot_y_params
    if args.subplot_hue_params is not None:
        subplot_hue_params = args.subplot_hue_params
    if args.plot_x_params is not None:
        plot_x_params = args.plot_x_params
    if args.plot_y_param is not None:
        plot_y_param = args.plot_y_param
    if args.plot_type is not None:
        plot_type = args.plot_type
        

    main(args.file_name, file_dir, plot_type)   

    # main(args.file_name, file_dir, "heatmap")
    # main(args.file_name, file_dir, "bar")
    # main(args.file_name, file_dir, "box") 
    # main(args.file_name, file_dir, "cdf") 
    # main(args.file_name, file_dir, "line")   
    # main(args.file_name, file_dir, "violin") 
