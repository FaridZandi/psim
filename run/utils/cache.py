# handle the timing
# timings_dir = "{}/timings/{}-{}/{}/".format(config_sweeper.custom_files_dir, 
#                                             placement_mode, ring_mode, placement_seed)
# os.makedirs(timings_dir, exist_ok=True)

# timing_file_path = "{}/{}.txt".format(timings_dir, timing_scheme)

# # get the cache status. We don't have the lock at this point. 









# cache_status = None 
# config_sweeper.log_for_thread(run_context, "Going to acquire the lock to check the timing file ...")

# with config_sweeper.thread_lock:
    
#     config_sweeper.log_for_thread(run_context, "acquired the lock to check the timing file ...")
    
#     if timing_file_path in timing_files_states:
#         cache_content = timing_files_states[timing_file_path]
#         if cache_content == "in progress":
#             cache_status = "in progress"
#         else:
#             cache_status = "ready"
#     else:
#         cache_status = "you generate it"
#         timing_files_states[timing_file_path] = "in progress"   

# config_sweeper.log_for_thread(run_context, "Releasing the lock to check the timing file ...")
# config_sweeper.log_for_thread(run_context, "cache status: {}".format(cache_status))

# if cache_status == "ready":
#     job_timings = timing_files_states[timing_file_path]    

# elif cache_status == "you generate it":
#     timing_files_states[timing_file_path] = job_timings
    
# elif cache_status == "in progress":
#     with open(run_context["output-file"], "a+") as f:   
#         f.write("Waiting for the timing file to be ready.\n")
#         f.write(timing_file_path)
        
#     print("going to wait for the timing file to be ready ...")    
#     while timing_files_states[timing_file_path] == "in progress":
#         time.sleep(1)
#     print("timing file is ready ...")
    
#     job_timings = timing_files_states[timing_file_path]


# options["timing-file"] = timing_file_path


# we wan to have something that can be used like the code above. 


import time 


# this is a cache that is used to store stuff that takes a long time to compute. 
# the cache is non-blocking, meaning that if the value is not ready, it will not maintain the lock. 
# the lock will only be acquired to check the status of the cache. 

# if the value is not there, this thread will wait for the value to be ready
# if it's the first thread to ask for the value, it will compute the value and store it in the cache.


class NonBlockingCache:
    def __init__(self, name):
        self.cache = {}
        self.name = name
        self.hits = 0 
        self.misses = 0
        self.waiting = 0
        
    def get(self, key, lock, logger_func, run_context, calc_func, calc_func_args):
        cache_status = None 
        logger_func(run_context, "Going to acquire the lock to check the cache status ...")

        with lock:
            logger_func(run_context, "acquired the lock to check the cache status ...")
            
            if key in self.cache:
                cache_content = self.cache[key]
                
                if cache_content == "in progress":
                    cache_status = "in progress"
                else:
                    cache_status = "ready"
            else:
                cache_status = "you generate it"
                self.cache[key] = "in progress"   

        logger_func(run_context, "Releasing the lock ...")
        logger_func(run_context, "cache status is: {}".format(cache_status))

        if cache_status == "ready":
            self.hits += 1
            value = self.cache[key]    

        elif cache_status == "you generate it":
            self.misses += 1
            logger_func(run_context, "Generating the value ...")
            value = calc_func(*calc_func_args)
            self.cache[key] = value
            
        elif cache_status == "in progress":
            self.waiting += 1   
            with open(run_context["output-file"], "a+") as f:   
                f.write("Waiting for the result to be generated.\n")
                f.write(key)
                
            while self.cache[key] == "in progress":
                time.sleep(1)
            
            value = self.cache[key]

        print ("{} -> Hits: {}, Misses: {}, Waiting: {}".format(self.name, self.hits, self.misses, self.waiting))
        return value
     