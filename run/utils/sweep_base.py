import os
from pprint import pprint
import itertools
import subprocess
import pandas as pd
import queue
import threading
import datetime
from utils.util import *
import copy 
import traceback

# pd.set_option('display.max_rows', 500)
# pd.set_option('display.max_columns', 500)

DEFAULT_WORKER_THREAD_COUNT = 40
MEMORY_LIMIT = 55

class ConfigSweeper: 
    def __init__(self, 
                 base_options, sweep_config, exp_context,
                 run_command_options_modifier=None,
                 run_results_modifier=None,
                 custom_save_results_func=None,
                 result_extractor_function=None,
                 exp_name="exp",
                 worker_thread_count=DEFAULT_WORKER_THREAD_COUNT):
        
        # arguments
        self.sweep_config = sweep_config
        self.base_options = base_options
        self.exp_context = exp_context  
        
        # global stuff 
        self.total_jobs = 0
        self.non_converged_jobs = 0 
        self.exp_results = []
        self.threads = []
        self.worker_id_counter = 0
        self.global_exp_id = 0

        self.exp_q = queue.Queue()
        self.thread_lock = threading.Lock()    
        
        # constanst
        self.run_command_options_modifier = run_command_options_modifier
        self.run_results_modifier = run_results_modifier
        self.worker_thread_count = worker_thread_count
        self.custom_save_results_func = custom_save_results_func
        self.result_extractor_function = result_extractor_function
        
        # paths
        self.run_id = str(get_incremented_number())
        self.base_dir = get_base_dir() 
        
        self.input_dir = self.base_dir + "/input/"
        self.build_path = self.base_dir + "/build"
        self.run_path = self.base_dir + "/run"
        self.base_executable = self.build_path + "/psim"
        self.run_executable = self.build_path + "/psim-" + self.run_id
        self.results_dir = "results/sweep/{}-{}/".format(self.run_id, exp_name)
        self.csv_dir = self.results_dir + "/csv/"
        self.raw_csv_path = self.csv_dir + "raw_results.csv"    
        self.plots_dir = self.results_dir + "/plots/" 
        # self.workers_dir = self.run_path + "/workers/"
        self.workers_dir = "/tmp2/workers/"
        self.plot_commands_script = self.results_dir + "plot_commands.sh"
        self.custom_files_dir = self.results_dir + "custom_files/"  
        self.exp_outputs_dir = self.results_dir + "exp_outputs/"
        self.commands_log_path = self.results_dir + "commands_log.txt"
        
        self.results_cache = {}
        self.cache_lock = threading.Lock() 
        self.cache_hits = 0 
        self.cache_mistakes = 0 
        self.cache_recalculations = 0 
        
        self.last_df_save_time = datetime.datetime.now() 
        self.df_save_interval_seconds = 300 
        
        self.do_store_outputs = False
        
        os.system("mkdir -p {}".format(self.results_dir))
        os.system("mkdir -p {}".format(self.csv_dir))
        os.system("mkdir -p {}".format(self.plots_dir))
        os.system("mkdir -p {}".format(self.workers_dir))
        os.system("mkdir -p {}".format(self.custom_files_dir))
        os.system("mkdir -p {}".format(self.exp_outputs_dir))   
        os.system("touch {}".format(self.plot_commands_script))
        os.system("chmod +x {}".format(self.plot_commands_script))
        
        # set up the watchdog. Run the "./ram_controller.sh 18 python3" in background,
        # keep pid and kill it when the program ends.
        self.watchdog_pid = subprocess.Popen([
                                "./ram_controller.sh", 
                                str(MEMORY_LIMIT), 
                                "python3"
                            ]).pid
        
    def __del__(self):
        print("running the destructor ... ")
        os.system("kill -9 {}".format(self.watchdog_pid))
    
    def log_config(self):
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
            pprint("------------------------------------------", stream=f)
            pprint("exp_context", stream=f)
            pprint(self.exp_context, stream=f)
            
            
    def sweep(self):
        self.log_config()

        # run the experiments
        build_exec(self.run_executable, self.base_executable, 
                   self.build_path, self.run_path)

        try: 
            self.run_all_experiments()

            print("number of jobs that didn't converge:", self.non_converged_jobs)
            print("number of cache hits:", self.cache_hits)
            print("number of cache mistakes:", self.cache_mistakes) 
            print("number of cache recalculations:", self.cache_recalculations) 
            
            if self.cache_mistakes > 0:
                print("cache mistakes happened. This means that the results are not deterministic.")
                print("The cache is only used to save time, not to change the results.")
                input("Press Enter to continue...")

            # save the raw results to a csv file. 
            all_pd_frame = pd.DataFrame(self.exp_results)
            all_pd_frame.to_csv(self.raw_csv_path)
            
            # call the custom func to do anything with the results.
            if self.custom_save_results_func is not None: 
                self.custom_save_results_func(all_pd_frame, self, self.exp_context, plot=True)
                
        except Exception as e:
            print("error in running the experiments")
            traceback.print_exc()
            print(e)
            
        finally:   
            print("cleaning up the run executable: ", self.run_executable)
            os.system("rm {}".format(self.run_executable))

            print("killing the watchdog with pid: ", self.watchdog_pid)
            os.system("kill -9 {}".format(self.watchdog_pid))        
        
        
    def run_all_experiments(self):
        # get all the permutations of the sweep config
        keys, values = zip(*self.sweep_config.items())
        permutations_dicts = [dict(zip(keys, v)) for v in itertools.product(*values)]
        
        # shuffle the permutations: 
        random.shuffle(permutations_dicts)
        
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
                self.run_experiment(exp, worker_id)   

            except queue.Empty:
                return
            
            except Exception as e:
                print("error in getting the experiment")
                traceback.print_exc()
                print(e)
                exit(0)
                
            
    def log_for_thread(self, run_context, message, data=None):
        with open(run_context["output-file"], "a+") as f:
            f.write(message + "\n")
            if data is not None:
                pprint(data, stream=f)
                f.write("\n")
        
    def run_experiment(self, exp, worker_id):
        with self.thread_lock:
            self.global_exp_id += 1 
            this_exp_uuid = self.global_exp_id
                
        start_time = datetime.datetime.now()

        # everything about the experiment is stored in the options and context. 
        # the options are the parameters that are passed to the executable.
        # the context is the rest of the information that is needed to save the results, 
        # but is not passed to the executable.
        run_context = {} 
        run_context.update(self.exp_context)
        run_context["exp-uuid"] = this_exp_uuid
        
        output_file_path = self.exp_outputs_dir + "output-{}.txt".format(run_context["exp-uuid"])
        with open(output_file_path, "w+") as f:
            f.write("output file for experiment {}\n\n\n".format(run_context["exp-uuid"]))

        run_context["output-file"] = output_file_path


        # options will have the base options, and the current combination of the 
        # sweep config parameters.
        options = {}
        options.update(self.base_options)
        options.update(exp)

        with open(output_file_path, "a+") as f:
            f.write("options: \n")
            pprint(options, stream=f)
            f.write("\n\n\n")
            f.write("run_context: \n")
            pprint(run_context, stream=f)
            f.write("\n\n\n")   
            
        # a final chance for the user to modify the options before making the command. 
        if self.run_command_options_modifier is not None:
            self.run_command_options_modifier(options, self, run_context)
        
        cache_hit = False 
        cache_key = str(options)
        
        self.log_for_thread(run_context, "Going to acquire the lock to check the cache")
        
        with self.thread_lock:
            self.log_for_thread(run_context, "Acquired the lock to check the cache")
            
            if cache_key in self.results_cache:
                cache_hit = True
                self.cache_hits += 1
        
        self.log_for_thread(run_context, "Done with the lock to check the cache")

        if cache_hit:
            with self.thread_lock:
                this_exp_results, output = self.results_cache[cache_key]
                this_exp_results = copy.deepcopy(this_exp_results)
                duration = 0 
                
        else:                         
            options["worker-id"] = worker_id
            options["workers-dir"] = self.workers_dir
            cmd = make_cmd(self.run_executable, options, use_gdb=False, print_cmd=False)
            
            try: 
                
                with open(self.commands_log_path, "a+") as f:
                    f.write(cmd + "\n")
                    f.write("-"*50 + "\n")
                    
                output = subprocess.check_output(cmd, shell=True)
                output = output.decode("utf-8").splitlines()
                
                if self.do_store_outputs:
                    # store the output in a file.
                    with open(run_context["output-file"], "a+") as f:
                        pprint(options, stream=f)
                        f.write("\n" + "-"*50 + "\n")
                        f.writelines("\n".join(output))
                
                # get the duration of the experiment.
                end_time = datetime.datetime.now()
                duration = end_time - start_time
                run_context["duration"] = duration.total_seconds()
                
                # get the basic results from the output with the custom function.
                this_exp_results = {
                    "run_id": self.run_id,
                }
                
                try: 
                    if self.result_extractor_function is not None:
                        self.result_extractor_function(output, options, this_exp_results, run_context)
                        
                except Exception as e:
                    print("error in result_extractor_function")
                    print("options: ", options) 
                    print("run_context: ", run_context)
                    print(e)
                    exit(0) 
                    
                # save the results to the cache to avoid recalculating them.
                self.log_for_thread(run_context, "Going to acquire the lock to save the results to the cache")
                with self.thread_lock:
                    self.log_for_thread(run_context, "Acquired the lock to save the results to the cache")
                    new_results_copy = copy.deepcopy(this_exp_results)
                    # sanity check: 
                    if cache_key in self.results_cache:
                        
                        self.cache_recalculations += 1
                        
                        # all keys should be the same. 
                        for key in new_results_copy:
                            if new_results_copy[key] != self.results_cache[cache_key][0][key]:
                                print("error in cache")
                                print("key: ", key)
                                print("cache: ", self.results_cache[cache_key][0][key])
                                print("cache_copy: ", new_results_copy[key])
                                
                                # a cache mistake happens when two experimenst have the same 
                                # options, but the results are different. This means that the 
                                # results are not deterministic, which is against our attempt 
                                # to make the results deterministic. So not a cache mistake, 
                                # per se, but a mistake in the simulation itself.
                                
                                self.cache_mistakes += 1    
                                                        
                    self.results_cache[cache_key] = (new_results_copy, output)
                
                self.log_for_thread(run_context, "Done with the lock to save the results to the cache")
                
            except subprocess.CalledProcessError as e:
                print("error in running the command")
                print("I don't know what to do here")
                print(e)
                exit(0)
        
        # everything will be combined in this dictionary. 
        # the results from the executable, the options, and the context. 
        this_exp_results_keys = list(this_exp_results.keys()) 
        run_context_keys = list(run_context.keys())
        options_keys = list(options.keys())
        
        # check for duplicate keys between the results and the options.
        duplicate_keys = set(this_exp_results_keys) & set(options_keys)
        if len(duplicate_keys) > 0:
            print("duplicate keys between the results and the options")
            print(duplicate_keys)
            exit(0)
        duplicate_keys = set(this_exp_results_keys) & set(run_context_keys)
        if len(duplicate_keys) > 0:
            print("duplicate keys between the results and the run_context")
            print(duplicate_keys)
            exit(0)
        duplicate_keys = set(run_context_keys) & set(options_keys)
        if len(duplicate_keys) > 0:
            print("duplicate keys between the run_context and the options")
            print(duplicate_keys)
            exit(0)
               
        results = {} 
        results.update(this_exp_results)
        results.update(run_context)
        results.update(options)

        self.log_for_thread(run_context, "Going to acquire the lock to save the results")
        
        with self.thread_lock:
            self.log_for_thread(run_context, "Acquired the lock to save the results")
        
            # a final chance for the user to modify the results before saving them.
            if self.run_results_modifier is not None:
                self.run_results_modifier(results)
        
            self.exp_results.append(results)

            # save the results to a csv file every 10 seconds
            time_since_last_save = datetime.datetime.now() - self.last_df_save_time
            if time_since_last_save.total_seconds() > self.df_save_interval_seconds:
                self.last_df_save_time = datetime.datetime.now()
                
                df = pd.DataFrame(self.exp_results)
                df.to_csv(self.raw_csv_path)
                
                if self.custom_save_results_func is not None:
                    try: 
                        self.custom_save_results_func(df, self, self.exp_context, plot=False)
                    except Exception as e:
                        print("error in custom_save_results_func")
                        print(e)
            
            pprint(results)
            print("jobs completed: {}/{}".format(len(self.exp_results), self.total_jobs))
            print("duration: {}".format(duration))
            print("worker id: {}".format(worker_id))
            print("--------------------------------------------")

        self.log_for_thread(run_context, "Done with the lock to save the results")
