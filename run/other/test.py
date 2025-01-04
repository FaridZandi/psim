# import pandas as pd
# import matplotlib.pyplot as plt
# import seaborn as sns

# # Function to fetch data from World Bank
# def fetch_data():
#     # URLs for World Bank data
#     gdp_growth_url = 'http://api.worldbank.org/v2/en/indicator/NY.GDP.MKTP.KD.ZG?downloadformat=excel'
#     gini_url = 'http://api.worldbank.org/v2/en/indicator/SI.POV.GINI?downloadformat=excel'
    
#     # Fetching and reading data
#     gdp_growth_data = pd.read_excel(gdp_growth_url, sheet_name='Data', skiprows=3)
#     gini_data = pd.read_excel(gini_url, sheet_name='Data', skiprows=3)
    
#     # Melting the data to have one year per row
#     gdp_growth_data_melted = gdp_growth_data.melt(id_vars=['Country Name', 'Country Code'], var_name='Year', value_name='GDP Growth')
#     gini_data_melted = gini_data.melt(id_vars=['Country Name', 'Country Code'], var_name='Year', value_name='Gini Coefficient')
    
#     # Converting 'Year' from string to integer
#     gdp_growth_data_melted['Year'] = gdp_growth_data_melted['Year'].astype(int)
#     gini_data_melted['Year'] = gini_data_melted['Year'].astype(int)
    
#     # Merging datasets on common columns
#     merged_data = pd.merge(gdp_growth_data_melted, gini_data_melted, on=['Country Name', 'Country Code', 'Year'])
    
#     # Dropping rows with missing values
#     merged_data.dropna(inplace=True)
    
#     return merged_data

# # Fetch data
# data = fetch_data()

# # Plotting the correlation for all years
# plt.figure(figsize=(10, 6))
# sns.regplot(x='Gini Coefficient', y='GDP Growth', data=data, ci=None, scatter_kws={'s': 10})
# plt.title('Correlation between Income Inequality (Gini Coefficient) and GDP Growth (All Years)')
# plt.xlabel('Gini Coefficient')
# plt.ylabel('GDP Growth (%)')

# plt.savefig('correlation_plot.png') 


a = {
    "timing_file_path": "results/sweep/2461-nethint_LB+_TS+_R+_2_67/custom_files//timings/sim-random/2/cassini/first/time-no-coll//timing.txt",
    "routing_file_path": "results/sweep/2461-nethint_LB+_TS+_R+_2_67/custom_files//routings/sim-random/2/cassini/first/time-no-coll/routing.txt",
    "placement_seed": 2,
    "jobs": [
        {
            "job_id": 1,
            "machine_count": 5,
            "comm_size": 2000,
            "comp_size": 100,
            "layer_count": 1,
            "iter_count": 85,
            "machines": [16, 17, 15, 19, 18],
            "period": {"1.0": 232, "0.75": 248, "0.5": 264},
            "base_period": 232
        },
        {
            "job_id": 2,
            "machine_count": 5,
            "comm_size": 4000,
            "comp_size": 100,
            "layer_count": 2,
            "iter_count": 42,
            "machines": [2, 1, 0, 3, 4],
            "period": {"1.0": 464, "0.75": 488, "0.5": 528},
            "base_period": 464
        },
        {
            "job_id": 3,
            "machine_count": 7,
            "comm_size": 8000,
            "comp_size": 200,
            "layer_count": 1,
            "iter_count": 35,
            "machines": [31, 27, 29, 63, 28, 32, 30],
            "period": {"1.0": 544, "0.75": 592, "0.5": 676},
            "base_period": 544
        }
    ],
    "options": {
        "step-size": 1,
        "core-status-profiling-interval": 100000,
        "rep-count": 1,
        "console-log-level": 4,
        "file-log-level": 1,
        "initial-rate": 100,
        "min-rate": 100,
        "drop-chance-multiplier": 0,
        "rate-increase": 1,
        "priority-allocator": "maxmin",
        "network-type": "leafspine",
        "link-bandwidth": 100,
        "ft-rack-per-pod": 1,
        "ft-agg-per-pod": 1,
        "ft-pod-count": -1,
        "ft-server-tor-link-capacity-mult": 1,
        "ft-tor-agg-link-capacity-mult": 1,
        "ft-agg-core-link-capacity-mult": 1,
        "shuffle-device-map": False,
        "regret-mode": "none",
        "machine-count": 64,
        "ft-server-per-rack": 16,
        "simulation-seed": 67,
        "print-flow-progress-history": True,
        "protocol-file-name": "nethint-test",
        "lb-scheme": "leastloaded",
        "subflows": 1,
        "ft-core-count": 8,
        "workers-dir": "/home/faridzandi/git/psim/run/workers/",
        "load-metric": "flowsize",
        "placement-file": "results/sweep/2461-nethint_LB+_TS+_R+_2_67/custom_files//placements/sim-random/2//placement.txt"
    },
    "run_context": {
        "sim-length": 20000,
        "visualize-timing": False,
        "visualize-routing": False,
        "profiled-throttle-factors": [1.0, 0.75, 0.5],
        "random-rep-count": 1,
        "interesting-metrics": ["avg_ar_time", "avg_iter_time"],
        "all-placement-modes": ["random", "semirandom_4", "sim", "compact"],
        "experiment-seed": 67,
        "oversub": 2,
        "cassini-parameters": {
            "link-solution-candidate-count": 50,
            "link-solution-random-quantum": 10,
            "link-solution-top-candidates": 3,
            "overall-solution-candidate-count": 10,
            "save-profiles": True
        },
        "routing-parameters": {},
        "selected-setting": {
            "machine-count": 64,
            "ft-server-per-rack": 16,
            "jobs-machine-count-low": 12,
            "jobs-machine-count-high": 16,
            "placement-seed-range": 10,
            "comm-size": [8000, 4000, 2000],
            "comp-size": [200, 100, 400],
            "layer-count": [1, 2],
            "iter-count": [30]
        },
        "comparison-base": {
            "timing-scheme": "random",
            "ring-mode": "random",
            "lb-scheme": "random",
            "subflows": 1
        },
        "comparisons": [
            ["farid", {"timing-scheme": "farid"}],
            ["cassini", {"timing-scheme": "cassini"}],
            ["zero", {"timing-scheme": "zero"}],
            ["cassinLB", {"timing-scheme": "cassini", "lb-scheme": "leastloaded"}],
            ["sub8", {"subflows": 8}],
            ["ideal", {"lb-scheme": "ideal"}]
        ],
        "exp-uuid": 22,
        "worker-id-for-profiling": 17,
        "output-file": "results/sweep/2461-nethint_LB+_TS+_R+_2_67/exp_outputs/output-22.txt",
        "perfect_lb": False,
        "ideal_network": False,
        "farid_timing": False,
        "original_mult": 1,
        "original_core_count": 8,
        "original_lb_scheme": "leastloaded",
        "original_ring_mode": "random",
        "original_timing_scheme": "cassini",
        "routing-fit-strategy": "first",
        "compat-score-mode": "time-no-coll",
        "placement-mode": "sim",
        "ring-mode": "random",
        "placement-seed": 2,
        "timing-scheme": "cassini",
        "placements_dir": "results/sweep/2461-nethint_LB+_TS+_R+_2_67/custom_files//placements/sim-random/2/",
        "profiles-dir": "results/sweep/2461-nethint_LB+_TS+_R+_2_67/custom_files//profiles/sim-random/2/",
        "timings-dir": "results/sweep/2461-nethint_LB+_TS+_R+_2_67/custom_files//timings/sim-random/2/cassini/first/time-no-coll/",
        "routings-dir": "results/sweep/2461-nethint_LB+_TS+_R+_2_67/custom_files//routings/sim-random/2/cassini/first/time-no-coll"
    }
}

print(json.dumps(a, indent=4))