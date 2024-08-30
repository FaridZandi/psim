from utils.util import *
import pandas as pd 

COMPARED_LB = "perfect"
results_dir = "results/sweep/659-run/"
path = results_dir + "raw_results.csv"

# read the csv file
exp_results_df = pd.read_csv(path)


this_exp_results_keys = ['run_id', 'min_avg_ar_time', 'max_avg_ar_time', 'last_avg_ar_time', 'avg_avg_ar_time', 'all_avg_ar_time', 'min_avg_iter_time', 'max_avg_iter_time', 'last_avg_iter_time', 'avg_avg_iter_time', 'all_avg_iter_time']
run_context_keys =  ['perfect_lb', 'ideal_network', 'original_mult', 'original_core_count', 'placement-mode', 'ring_mode']
options_keys =  ['step-size', 'core-status-profiling-interval', 'rep-count', 'console-log-level', 'file-log-level', 'initial-rate', 'min-rate', 'drop-chance-multiplier', 'rate-increase', 'priority-allocator', 'network-type', 'link-bandwidth', 'ft-rack-per-pod', 'ft-agg-per-pod', 'ft-pod-count', 'ft-server-tor-link-capacity-mult', 'ft-tor-agg-link-capacity-mult', 'ft-agg-core-link-capacity-mult', 'shuffle-device-map', 'regret-mode', 'protocol-file-name', 'placement-seed', 'machine-count', 'ft-server-per-rack', 'general-param-1', 'general-param-3', 'general-param-4', 'general-param-5', 'general-param-6', 'general-param-7', 'ft-core-count', 'lb-scheme', 'timing-scheme', 'simulation-seed', 'load-metric', 'placement-file', 'worker-id']
