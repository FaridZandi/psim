import os
from pprint import pprint
import itertools
import subprocess
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import queue
import threading
import sys
import datetime
from utils.util import *
import resource
from processing.itertimes import get_convergence_info, get_first_iter_info

# pd.set_option('display.max_rows', 500)
# pd.set_option('display.max_columns', 500)


DEFAULT_WORKER_THREAD_COUNT = 40

class ConfigSweeper: 
    def __init__(self, base_options, sweep_config, 
                 run_command_options_modifier=None,
                 run_results_modifier=None,
                 global_results_modifier=None,
                 worker_thread_count=DEFAULT_WORKER_THREAD_COUNT):
        
        # arguments
        self.sweep_config = sweep_config
        self.base_options = base_options
        
        # global stuff 
        self.total_jobs = 0
        self.non_converged_jobs = 0 
        self.exp_results = []
        self.threads = []
        self.worker_id_counter = 0
        
        self.exp_q = queue.Queue()
        self.thread_lock = threading.Lock()    
        
        # constanst
        self.run_command_options_modifier = run_command_options_modifier
        self.run_results_modifier = run_results_modifier
        self.worker_thread_count = worker_thread_count
        self.global_results_modifier = global_results_modifier
        
        # paths
        self.run_id = str(get_incremented_number())
        self.base_dir = get_base_dir() 
        
        self.input_dir = self.base_dir + "/input/"
        self.build_path = self.base_dir + "/build"
        self.run_path = self.base_dir + "/run"
        self.base_executable = self.build_path + "/psim"
        self.run_executable = self.build_path + "/psim-" + self.run_id
        self.results_dir = "results/sweep/{}-run/".format(self.run_id)
        self.shuffle_dir = self.results_dir + "/shuffle/"
        self.csv_path = self.results_dir + "results.csv".format(self.run_id)
        self.raw_csv_path = self.results_dir + "raw_results.csv".format(self.run_id)
        self.merged_csv_path = self.results_dir + "merged_results.csv".format(self.run_id)
        self.workers_dir = self.run_path + "/workers/"

        os.system("mkdir -p {}".format(self.results_dir))
        os.system("mkdir -p {}".format(self.shuffle_dir))




    def sweep(self):
        with open(self.results_dir + "sweep-config.txt", "w") as f:
            pprint("------------------------------------------", stream=f)
            pprint("sweep_config", stream=f)
            pprint(self.sweep_config, stream=f)
            pprint("------------------------------------------", stream=f)
            pprint("base_options", stream=f)
            pprint(self.base_options, stream=f)
            pprint("------------------------------------------", stream=f)
            pprint("globals", stream=f)
            pprint(globals(), stream=f)
            pprint("------------------------------------------", stream=f)
            pprint("self", stream=f)
            pprint(self, stream=f)

        build_exec(self.run_executable, 
                   self.base_executable, 
                   self.build_path, 
                   self.run_path)
        set_memory_limit(10 * 1e9)
        
        self.run_all_experiments()

        print ("number of jobs that didn't converge:", self.non_converged_jobs)
        os.system("rm {}".format(self.run_executable))

        all_pd_frame = pd.DataFrame(self.exp_results)
        all_pd_frame.to_csv(self.raw_csv_path)
        
        if self.global_results_modifier is not None:
            final_df = self.global_results_modifier(all_pd_frame, self)
        final_df.to_csv(self.csv_path)
        
        return  self.results_dir, self.csv_path, self.exp_results
        # os.system("python plot.py {}".format(csv_path))
        
        
        
        
        
    def run_all_experiments(self):
        # get all the permutations of the sweep config
        keys, values = zip(*self.sweep_config.items())
        permutations_dicts = [dict(zip(keys, v)) for v in itertools.product(*values)]
        for exp in permutations_dicts:
            self.exp_q.put(exp)

        # set the total number of jobs
        self.total_jobs = self.exp_q.qsize()

        # start and join all the threads
        for i in range(self.worker_thread_count):
            t = threading.Thread(target=self.worker_function)
            self.threads.append(t)
            t.start()

        for t in self.threads:
            t.join()       
            
            
            
    def worker_function(self):
        with self.thread_lock:
            worker_id = self.worker_id_counter
            self.worker_id_counter += 1

        while True:
            try:
                exp = self.exp_q.get(block = 0)
            except queue.Empty:
                return
            self.run_experiment(exp, worker_id)   


            
            
    def run_experiment(self, exp, worker_id):
        start_time = datetime.datetime.now()

        options = {
            "worker-id": worker_id,
        }
        options.update(self.base_options)
        options.update(exp)

        # we don't to save every key in the options, so we keep track of the keys that we want to save.
        # TODO: why can't we just save all the keys? what's the limitation here? the csv file will be too big?
        saved_keys = set(list(self.sweep_config.keys())) 
        
        # a final chance for the user to modify the options before making the command. 
        # it should also return the keys that have been changed, so that we can save them in the results. 
        if self.run_command_options_modifier is not None:
            with self.thread_lock: 
                changed_keys = self.run_command_options_modifier(options, self)
                saved_keys.update(changed_keys)
                    
        # create the command
        cmd = make_cmd(self.run_executable, options, use_gdb=False, print_cmd=False)

        try:
            output = subprocess.check_output(cmd, shell=True)
            output = output.decode("utf-8")

            end_time = datetime.datetime.now()
            duration = end_time - start_time

            last_psim_time = 0
            min_psim_time = 1e12
            max_psim_time = 0
            all_times = []

            for line in output.splitlines():
                if "psim time" in line:
                    psim_time = float(line.strip().split(" ")[-1])
                    all_times.append(psim_time)
                    if psim_time > max_psim_time:
                        max_psim_time = psim_time
                    if psim_time < min_psim_time:
                        min_psim_time = psim_time
                    last_psim_time = psim_time
            
        except subprocess.CalledProcessError as e:
            min_psim_time = 0
            max_psim_time = 0
            last_psim_time = 0
            all_times = []
            duration = datetime.timedelta(0)

        this_exp_results = {
            "min_psim_time": min_psim_time,
            "max_psim_time": max_psim_time,
            "last_psim_time": last_psim_time,
            "avg_psim_time": np.mean(all_times),
            "all_times": all_times,
            "exp_duration": duration.microseconds,
            "run_id": self.run_id,
        }
        
        # TODO: maybe add them under a different key? 
        # m = {options: options = {}, results: results = {}}
        for key in saved_keys:
            this_exp_results[key] = options[key]

        with self.thread_lock:
            if self.run_results_modifier is not None:
                self.run_results_modifier(this_exp_results, output)
            self.exp_results.append(this_exp_results)
            
            pprint(this_exp_results)
            print("min time: {}, max time: {}, last time: {}".format(
                min_psim_time, max_psim_time, last_psim_time))
            print("jobs completed: {}/{}".format(len(self.exp_results), self.total_jobs))
            print("duration: {}".format(duration))
            print("worker id: {}".format(worker_id))
            print("--------------------------------------------")


    def plot_results(self, interesting_keys, plotted_key_min, plotted_key_max): 
        keys_arg = ",".join(interesting_keys)
        os.system("python plot.py {} {} {} {}".format(self.csv_path, keys_arg, plotted_key_min, plotted_key_max))