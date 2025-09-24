import sys
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
        start_time = time.time()     
        
        logger_func(run_context, "Going to acquire the lock to check the {} cache status ...".format(self.name))

        with lock:
            logger_func(run_context, "acquired the lock to check the {} cache status ...".format(self.name))
            
            if key in self.cache:
                # if the key is in the cache, we can check the status of the cache.
                cache_content = self.cache[key]
                
                # if the content is "in progress", we need to wait for the value to be generated.      
                if cache_content == "in progress":
                    cache_status = "in progress"
                else:
                    # otherwise the value is ready.
                    cache_status = "ready"
            else:
                # the first thread to ask for the value will generate it.
                cache_status = "you generate it"
                
                # we need to mark this as in progress so that other threads don't try to generate it
                self.cache[key] = "in progress"   

        logger_func(run_context, "Releasing the lock ...")
        logger_func(run_context, "{} cache status is: {}".format(self.name, cache_status))
        
        # if the value is ready, we can return it.
        if cache_status == "ready":
            self.hits += 1
            value = self.cache[key]    

        # if the value is not ready, we need to generate it.
        elif cache_status == "you generate it":
            self.misses += 1
            logger_func(run_context, "Generating the value for {} on {} ...".format(key, self.name))
            value = calc_func(*calc_func_args)
            
            # in the end, we store the value in the cache. 
            self.cache[key] = value
        
        # if the value is in progress, we need to wait for it to be generated.
        # we check the status of the cache every second. 
        elif cache_status == "in progress":
            self.waiting += 1   
            logger_func(run_context, "{}: Waiting for the value to be generated: {}".format(self.name, key))
                
            while self.cache[key] == "in progress":
                time.sleep(1)
                logger_func(run_context, "{}: Waiting for the value to be generated: {}".format(self.name, key))
            
            value = self.cache[key]

        print("-----------------------------------------------------------------", flush=True)
        print("{} -> {} \n Hits: {}, Misses: {}, Waiting: {}, My state: {}, Time: {}".format(
               self.name, key, self.hits, self.misses, self.waiting, 
               cache_status, time.time() - start_time), flush=True)
        print("-----------------------------------------------------------------", flush=True)
        sys.stdout.flush()
        
        return value
     