from utils.util import *
import pandas as pd 

path = "results/sweep/497-run/raw_results.csv"

# read the csv file
exp_results_df = pd.read_csv(path)

random_df = exp_results_df[exp_results_df["lb-scheme"] == "random"]
compared_lb_df = exp_results_df[exp_results_df["lb-scheme"] == "perfect"]


print(random_df)
print(compared_lb_df)