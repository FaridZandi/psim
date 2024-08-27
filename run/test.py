from utils.util import *
import pandas as pd 

COMPARED_LB = "perfect"
results_dir = "results/sweep/659-run/"
path = results_dir + "raw_results.csv"

# read the csv file
exp_results_df = pd.read_csv(path)

    