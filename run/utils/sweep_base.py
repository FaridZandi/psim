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
import time 

# pd.set_option('display.max_rows', 500)
# pd.set_option('display.max_columns', 500)

DEFAULT_WORKER_THREAD_COUNT = 40
MEMORY_LIMIT = 80

class ConfigSweeper: 
    def __init__(self, 
                 base_options, sweep_config, global_context,
                 run_command_options_modifier=None,
                 run_results_modifier=None,
                 custom_save_results_func=None,
                 result_extractor_function=None,
                 exp_filter_function=None,
                 exp_name="exp",
                 worker_thread_count=DEFAULT_WORKER_THREAD_COUNT, 
                 plot_cdfs=False, 
                 store_outputs=False):  
        
        # arguments
        self.sweep_config = sweep_config
        self.base_options = base_options
        self.global_context = global_context  
        
        # global stuff 
        self.total_jobs = 0
        self.exp_results = []
        self.threads = []
        self.thread_states = {i: "idle" for i in range(worker_thread_count)}
        self.worker_id_counter = 0
        self.global_exp_id = 0

        self.exp_q = queue.Queue()
        self.thread_lock = threading.Lock()  
        self.exp_id_lock = threading.Lock() 
        self.placement_lock = threading.Lock()  
        self.timing_lock = threading.Lock() 
        self.plot_lock = threading.Lock()  
        
        # constants
        self.run_command_options_modifier = run_command_options_modifier
        self.run_results_modifier = run_results_modifier
        self.worker_thread_count = worker_thread_count
        self.custom_save_results_func = custom_save_results_func
        self.result_extractor_function = result_extractor_function
        self.exp_filter_function = exp_filter_function  
        
        # paths
        self.run_id = str(get_incremented_number())
        self.base_dir = get_base_dir() 
        
        hostname = os.uname()[1]    
        
        self.input_dir = self.base_dir + "/input/"
        self.build_path = self.base_dir + "/build"
        self.run_path = self.base_dir + "/run"
        self.base_executable = self.build_path + "/psim"
        self.results_dir = "results-{}/sweep/{}-{}/".format(hostname, self.run_id, exp_name)
        self.run_executable = self.results_dir + "/psim-" + self.run_id
        self.csv_dir = self.results_dir + "/csv/"
        self.raw_csv_path = self.csv_dir + "raw_results.csv"    
        self.plots_dir = self.results_dir + "/plots/" 
        self.workers_dir = self.run_path + "/workers-{}".format(hostname)
        # self.workers_dir = "/tmp2/workers/"
        self.plot_commands_script = self.results_dir + "plot_commands.sh"
        self.custom_files_dir = self.results_dir + "custom_files/"  
        self.exp_outputs_dir = self.results_dir + "exp_outputs/"
        self.commands_log_path = self.results_dir + "commands_log.txt"
        self.thread_output_dir = self.results_dir + "thread_outputs/"
        self.thread_state_path = self.results_dir + "thread_states.txt" 
        
        self.last_df_save_time = datetime.datetime.now() 
        self.df_save_interval_seconds = 300 
        
        self.do_store_outputs = store_outputs
        self.plot_cdfs = plot_cdfs  
        self.relevant_keys = [] 
        
        os.system("mkdir -p {}".format(self.results_dir))
        os.system("mkdir -p {}".format(self.csv_dir))
        os.system("mkdir -p {}".format(self.plots_dir))
        os.system("mkdir -p {}".format(self.workers_dir))
        os.system("mkdir -p {}".format(self.custom_files_dir))
        os.system("mkdir -p {}".format(self.exp_outputs_dir))   
        os.system("mkdir -p {}".format(self.thread_output_dir))
        os.system("touch {}".format(self.plot_commands_script))
        os.system("chmod +x {}".format(self.plot_commands_script))
        
        # make a symbolic link to the results directory in the run_path
        os.system("rm -f {}/last-sweep-results-link-*".format(self.run_path))  
        os.system("ln -s {} {}".format(self.results_dir, self.run_path + "/last-sweep-results-link-{}".format(self.run_id)))
        # set up the watchdog. Run the "./ram_controller.sh 18 python3" in background,
        # keep pid and kill it when the program ends.
        # kill all the ram_controller.sh processes that are running.     
        os.system("pkill -f ram_controller.sh")
        
        # run the ram_controller.sh in the background.
        self.watchdog_pid = subprocess.Popen([
                                "./ram_controller.sh", 
                                str(MEMORY_LIMIT), 
                                self.run_executable
                            ]).pid
        
    def __del__(self):
        print("running the destructor ... ")
        os.system("kill -9 {}".format(self.watchdog_pid))
    
    def get_results_dir(self):  
        return self.run_path + "/" + self.results_dir   
    
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
            pprint("global_context", stream=f)
            pprint(self.global_context, stream=f)
        
    def sweep(self):
        self.log_config()

        # run the experiments
        build_exec(self.run_executable, self.base_executable, 
                   self.build_path, self.run_path)

        try: 
            self.run_all_experiments()

            # save the raw results to a csv file. 
            all_pd_frame = pd.DataFrame(self.exp_results)
            all_pd_frame.to_csv(self.raw_csv_path)
            
            # call the custom func to do anything with the results.
            if self.custom_save_results_func is not None: 
                summary = self.custom_save_results_func(all_pd_frame, self, self.global_context, plot=True)
            else: 
                summary = {} 
                
        except Exception as e:
            print("error in running the experiments")
            traceback.print_exc()
            print(e)
            
        finally:   
            print("killing the watchdog with pid: ", self.watchdog_pid)
            os.system("kill -9 {}".format(self.watchdog_pid))        
        
        return summary
        
    def run_all_experiments(self):
        # each item in the sweep_config is a list of values for a parameter.
        # only keep the unique values.
        self.relevant_keys = list(self.sweep_config.keys())
        
        for key in self.sweep_config:
            self.sweep_config[key] = list(set(self.sweep_config[key]))
        
        # get all the permutations of the sweep config
        keys, values = zip(*self.sweep_config.items())
        permutations_dicts = [dict(zip(keys, v)) for v in itertools.product(*values)]
        
        # filter out the permutations that do not correspond to a comparison
        if self.exp_filter_function is not None:
            permutations_dicts, relevant_keys_filtered = self.exp_filter_function(permutations_dicts, self) 
            self.relevant_keys = relevant_keys_filtered
             
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
        
        t = threading.Thread(target=self.thread_state_function)
        self.threads.append(t)
        t.start()

        for t in self.threads:
            t.join()       
    
    def thread_state_function(self):
        while True:
            with open(self.thread_state_path, "w+") as f:
                for i in range(self.worker_thread_count):
                    thread_number = i 
                    # pad with zeros to make the thread number 3 digits long.
                    thread_number = str(thread_number).zfill(3)
                    
                    f.write("thread {} → {}\n".format(thread_number, self.thread_states[i]))
                f.write("\n")
                    
            if len(self.exp_results) == self.total_jobs:
                return  
            
            time.sleep(1)
            
            
    def worker_function(self):
        with self.exp_id_lock:
            worker_id = self.worker_id_counter
            self.worker_id_counter += 1

        thread_output_path = self.thread_output_dir + "output-{}.txt".format(worker_id) 
        with open(thread_output_path, "w+") as f:
            f.write("output file for thread {}\n\n\n".format(worker_id))
            
        while True:
            try:
                exp = self.exp_q.get(block = 0)
                with open(thread_output_path, "a+") as f:
                    f.write("got the experiment\n")
                    
                self.run_experiment(exp, worker_id)   
                
                with open(thread_output_path, "a+") as f:
                    f.write("done with the experiment\n")
                    f.write("\n")   
                    
            except queue.Empty:
                with open(thread_output_path, "a+") as f:
                    f.write("queue is empty\n")
                    f.write("\n")
                return
            
            except Exception as e:
                traceback.print_exc()   
                rage_quit("error in getting the experiment" + str(e))   
                
            
    def log_for_thread(self, run_context, message, data=None):
        with open(run_context["output-file"], "a+") as f:
            # f.write(message + "\n")
            f.write("[{}] {}\n".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), message))
            
            if data is not None:
                pprint(data, stream=f)
                f.write("\n")
    
    def only_run_command_with_options(self, run_context, options):
        cmd = make_cmd(self.run_executable, options, use_gdb=False, print_cmd=False)
        
        self.log_for_thread(run_context, "Going to run the command ..." + cmd)  
            
        try: 
            output = subprocess.check_output(cmd, shell=True)
            output = output.decode("utf-8").splitlines()
        except Exception as e:
            traceback.print_exc()   
            rage_quit("error in running the command" + str(e))              
        
        return output
             
    def combine_results(self, this_exp_results, run_context, options):
        # everything will be combined in this dictionary. 
        # the results from the executable, the options, and the context. 
        this_exp_results_keys = list(this_exp_results.keys()) 
        run_context_keys = list(run_context.keys())
        options_keys = list(options.keys())
        
        # check for duplicate keys between the results and the options.
        # if there are any, print them and exit. 
        duplicate_keys = set(this_exp_results_keys) & set(options_keys)
        if len(duplicate_keys) > 0:
            rage_quit("duplicate keys between the results and the options: " + str(duplicate_keys))    

        duplicate_keys = set(this_exp_results_keys) & set(run_context_keys)
        if len(duplicate_keys) > 0:
            rage_quit("duplicate keys between the results and the run_context: " + str(duplicate_keys))

        duplicate_keys = set(run_context_keys) & set(options_keys)
        if len(duplicate_keys) > 0:
            rage_quit("duplicate keys between the run_context and the options: " + str(duplicate_keys))                

        results = {} 
        results.update(this_exp_results)
        results.update(run_context)
        results.update(options)       
        
        return results
        
        
    def run_experiment(self, exp, worker_id, add_to_results=True):
        with self.exp_id_lock:
            self.global_exp_id += 1 
            this_exp_uuid = self.global_exp_id
            
            thread_output_path = self.thread_output_dir + "output-{}.txt".format(worker_id) 
            with open(thread_output_path, "a+") as f:
                f.write("this exp uuid is {}\n\n\n".format(this_exp_uuid))
                    
        start_time = datetime.datetime.now()

        # everything about the experiment is stored in the options and context. 
        # the options are the parameters that are passed to the executable.
        # the context is the rest of the information that is needed to save the results, 
        # but is not passed to the executable.
        run_context = {} 
        run_context.update(self.global_context)
        run_context["exp-uuid"] = this_exp_uuid
        run_context["worker-id-for-profiling"] = worker_id  
        
        self.thread_states[worker_id] = "running exp-{}".format(this_exp_uuid)  

        # options will have the base options, and the current combination of the 
        # sweep config parameters.
        options = {}
        options.update(self.base_options)
        options.update(exp)
        options["workers-dir"] = self.workers_dir

        # a final chance for the user to modify the options before making the command. 
        print("{}: before command modification".format(this_exp_uuid), flush=True)
        if self.run_command_options_modifier is not None:
            self.run_command_options_modifier(options, self, run_context)
        print("{}: after command modification".format(this_exp_uuid), flush=True)
        
        options["worker-id"] = worker_id
        self.thread_states[worker_id] = "exp-{}-running-{}".format(this_exp_uuid, run_context["runtime-dir"])     
                
        cmd = make_cmd(self.run_executable, options, use_gdb=False, print_cmd=False)
        if "runtime-dir" in run_context:
            with open(run_context["runtime-dir"] + "/command.txt", "w+") as f:
                f.write(cmd)
                
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
            
            printed_metrics = [] 
            
            if self.result_extractor_function is not None:
                try: 
                    printed_metrics = self.result_extractor_function(output, options, 
                                                                     this_exp_results, 
                                                                     run_context, self)        
                except Exception as e:
                    print("error in result_extractor_function")
                    print("options: ", options) 
                    print("run_context: ", run_context)
                    print(e)
                    traceback.print_exc()  
                    
                    rage_quit("error in result_extractor_function") 
                    
        except subprocess.CalledProcessError as e:
            print("error in running the command")
            print("I don't know what to do here")
            print(e)
            traceback.print_exc()   
            
            rage_quit("error in running the command")   
                
        self.thread_states[worker_id] = "exp-{}-saving results".format(this_exp_uuid)  
        results = self.combine_results(this_exp_results, run_context, options)

        self.log_for_thread(run_context, "Going to acquire the lock to save the results")
        
        # with self.thread_lock:
        self.log_for_thread(run_context, "Acquired the lock to save the results")
    
        # a final chance for the user to modify the results before saving them.
        if self.run_results_modifier is not None:
            self.run_results_modifier(results)
        
        if "runtime-dir" in run_context:    
            with open(run_context["runtime-dir"] + "/results.txt", "w+") as f:
                pprint(results, stream=f, indent=4, width=100) 
                                
        self.exp_results.append(results)

        relevent_results = {key: results[key] for key in self.relevant_keys}   
        relevent_metrics = {key: results[key] for key in printed_metrics}
        relevent_results.update(relevent_metrics)
        
        print(relevent_results, flush=True)
        sys.stdout.flush()
        
        if "runtime-dir" in run_context:    
            with open(run_context["runtime-dir"] + "/summarized_results.txt", "w+") as f:
                pprint(relevent_results, stream=f, indent=4, width=100) 
                
        print("jobs completed: {}/{}".format(len(self.exp_results), self.total_jobs), flush=True)
        print("duration: {}".format(duration), flush=True)
        print("worker id: {}".format(worker_id), flush=True)
        print("--------------------------------------------", flush=True)
        sys.stdout.flush()
            
        self.thread_states[worker_id] = "exp-{}-done with the experiment".format(this_exp_uuid)  
        self.log_for_thread(run_context, "Done with the lock to save the results")

        return results