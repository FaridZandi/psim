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
import copy 

# pd.set_option('display.max_rows', 500)
# pd.set_option('display.max_columns', 500)


DEFAULT_WORKER_THREAD_COUNT = 40
MEMORY_LIMIT = 60

class ConfigSweeper: 
    def __init__(self, base_options, sweep_config, 
                 run_command_options_modifier=None,
                 run_results_modifier=None,
                 global_results_modifier=None,
                 result_extractor_function=None,
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
        self.result_extractor_function = result_extractor_function
        
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

        self.results_cache = {}
        self.cache_lock = threading.Lock() 
        self.cache_hits = 0 
        self.cache_mistakes = 0 
        
        self.last_df_save_time = datetime.datetime.now() 
        self.df_save_interval_seconds = 10 
        
        os.system("mkdir -p {}".format(self.results_dir))
        os.system("mkdir -p {}".format(self.shuffle_dir))
        
        # set up the watchdog. Run the "./ram_controller.sh 18 python3" in background,
        # keep pid and kill it when the program ends.
        self.watchdog_pid = subprocess.Popen(["./ram_controller.sh", str(MEMORY_LIMIT), "python3"]).pid
        

    def sweep(self):
        # some basic logging and setup
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

        # run the experiments
        build_exec(self.run_executable, self.base_executable, self.build_path, self.run_path)
        self.run_all_experiments()

        print("number of jobs that didn't converge:", self.non_converged_jobs)
        print("number of cache hits:", self.cache_hits)
        print("number of cache mistakes:", self.cache_mistakes) 


        # save the results to a csv file
        if self.global_results_modifier is not None:
            all_pd_frame = pd.DataFrame(self.exp_results)
            final_df = self.global_results_modifier(all_pd_frame, self)
            final_df.to_csv(self.csv_path)

        os.system("rm {}".format(self.run_executable))
        
        # kill the watchdog
        os.system("kill -9 {}".format(self.watchdog_pid))
        
        return  self.results_dir, self.csv_path, self.exp_results
        
        
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

        options = {}
        options.update(self.base_options)
        options.update(exp)

        # we don't to save every key in the options, so we keep track of the keys that we want to save.
        # TODO: why can't we just save all the keys? what's the limitation here? the csv file will be too big?
        saved_keys = set(list(self.sweep_config.keys())) 
        run_context = {} 
        
        # a final chance for the user to modify the options before making the command. 
        # it should also return the keys that have been changed, so that we can save them in the results. 
        if self.run_command_options_modifier is not None:
            with self.thread_lock: 
                changed_keys, run_context = self.run_command_options_modifier(options, self)
                saved_keys.update(changed_keys)
        
        cache_hit = False 
        cache_key = str(options)
        
        with self.cache_lock:
            if cache_key in self.results_cache:
                cache_hit = True
                self.cache_hits += 1
        
        if cache_hit:
            with self.cache_lock:
                this_exp_results, output = self.results_cache[cache_key]
                this_exp_results = copy.deepcopy(this_exp_results)
                duration = 0 
                
        else:                         
            options["worker-id"] = worker_id
            cmd = make_cmd(self.run_executable, options, use_gdb=False, print_cmd=False)
            
            try: 
                output = subprocess.check_output(cmd, shell=True)
                output = output.decode("utf-8").splitlines()
                
                end_time = datetime.datetime.now()
                duration = end_time - start_time
                
                # get the basic results. 
                this_exp_results = {
                    "run_id": self.run_id,
                }
                
                # get the rest of the results from the user defined function.            
                self.result_extractor_function(output, options, this_exp_results)
                
                with self.cache_lock:
                    
                    new_results_copy = copy.deepcopy(this_exp_results)
                     
                    # sanity check: 
                    if cache_key in self.results_cache:
                        # all keys should be the same. 
                        for key in new_results_copy:
                            if new_results_copy[key] != self.results_cache[cache_key][0][key]:
                                print("error in cache")
                                print("key: ", key)
                                print("cache: ", self.results_cache[cache_key][0][key])
                                print("cache_copy: ", new_results_copy[key])
                                
                                self.cache_mistakes += 1    
                                                        
                    self.results_cache[cache_key] = (new_results_copy, output)
                    
            except subprocess.CalledProcessError as e:
                print("error in running the command")
                print("I don't know what to do here")
                exit(0)
                
            
        for key in saved_keys:
            this_exp_results[key] = options[key]

        with self.thread_lock:
            if self.run_results_modifier is not None:
                self.run_results_modifier(this_exp_results, options, output, run_context)
            self.exp_results.append(this_exp_results)


            # save the results to a csv file every 10 seconds
            time_since_last_save = datetime.datetime.now() - self.last_df_save_time
            if time_since_last_save.total_seconds() > self.df_save_interval_seconds:

                self.last_df_save_time = datetime.datetime.now()
                
                df = pd.DataFrame(self.exp_results)
                df.to_csv(self.raw_csv_path)
                
                if self.global_results_modifier is not None:
                    try: 
                        final_df = self.global_results_modifier(df, self)
                        final_df.to_csv(self.csv_path)
                    except Exception as e:
                        pass
            
            pprint(this_exp_results)
            print("jobs completed: {}/{}".format(len(self.exp_results), self.total_jobs))
            print("duration: {}".format(duration))
            print("worker id: {}".format(worker_id))
            print("--------------------------------------------")


    def plot_results(self, interesting_keys, plotted_key_min, plotted_key_max, title): 
        keys_arg = ",".join(interesting_keys)
        
        plot_command = "python plot.py {} {} {} {}".format(self.csv_path, keys_arg, 
                                                           plotted_key_min, plotted_key_max)

        os.system(plot_command)
    
        print("To redraw the plot, use the following command: ")
        print(plot_command)
        
        
        
    def plot_cdfs(self, csv_path, separating_params, same_plot_param, cdf_params):
        separating_params_str = ",".join(separating_params)
        cdf_params_str = ",".join(cdf_params)
        
        plot_command = "python plot_cdf.py {} {} {} {}".format(csv_path, separating_params_str, 
                                                               same_plot_param, cdf_params_str)

        os.system(plot_command)
        
        print("To redraw the plot, use the following command: ")
        print(plot_command)
        