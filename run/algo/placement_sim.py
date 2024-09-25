import random 
import os 
import sys 
import math 
from pprint import pprint



# this script path: 
this_path = os.path.dirname(os.path.abspath(__file__))
this_dir = os.path.dirname(this_path)    


class Simulation():    
    
    def __init__(self, strategy, 
                       ring_mode, 
                       rack_size, 
                       total_machine_count, 
                       sim_length, 
                       keep_history=True): 
        
        self.rack_size = rack_size  
        self.total_machine_count = total_machine_count   
        self.rack_count = total_machine_count // rack_size
        self.keep_history = keep_history
          
        self.sim_length = sim_length    
        
        self.strategy = strategy
        self.ring_mode = ring_mode 
        
        self.current_jobs = [] 
        self.waiting_jobs = []
        self.completed_jobs = []
        
        self.is_busy = [False] * self.total_machine_count    
        self.busy_machine_count = 0 
        
        self.current_time = 0
        self.earliest_job_end = sim_length * 2 
        self.job_id_counter = 0

        self.entropies = []
        self.base_entropies = [] 
        self.utilizations = []     
        self.service_rates = []     
        self.inter_arrival_times = [] 
    
    
    def get_job_machine_count(self):
        # return random.randint(2, 18) # mean 10

        # a pareto distribution with with mean 10 
        r = int(random.paretovariate(1.3)) + 4
        if r > (self.total_machine_count // 2):
            r = (self.total_machine_count // 2)
            
        return r 

    def get_job_duration(self):
        # return random.randint(1, 20000) # mean 10000

        r = int(random.paretovariate(1.16)) * 1500 + 2000
        return r

    def get_interarrival_time(self):
        # return random.randint(1, 240) # mean 200

        r = int(random.paretovariate(1.16)) * 60 + 1
        return r

    def mark_machine_as_busy(self, machine_id): 
        self.is_busy[machine_id] = True
        self.busy_machine_count += 1

    def mark_machine_as_free(self, machine_id): 
        self.is_busy[machine_id] = False
        self.busy_machine_count -= 1    

    def try_to_initiate_waiting_jobs(self):
        # are there any waiting jobs?
        waiting_jobs_copy = self.waiting_jobs.copy() 
        self.waiting_jobs.clear()
        waiting_jobs_count = len(waiting_jobs_copy) 
        
        started_jobs = 0        
        for job in waiting_jobs_copy :
            free_machines = self.total_machine_count - self.busy_machine_count 

            if job["machine_count"] > free_machines:
                could_start = False
                self.waiting_jobs.append(job)
            else:    
                could_start = self.attempt_job_initiation(job["id"], job["duration"], job["machine_count"]) 
            
            if could_start:
                started_jobs += 1
                
        if self.keep_history:
            print(f"Started {started_jobs} jobs out of {waiting_jobs_count} waiting jobs")
    
    def defragment(self):   
        initial_busy_machine_count = self.busy_machine_count    
        
        for job in self.current_jobs:
            for machine_id in job["machines"]:
                self.mark_machine_as_free(machine_id)
            job["machines"] = []
        
        # assert all machines are free
        assert self.busy_machine_count == 0
        
        
        #sort the jobs by machine count
        self.current_jobs.sort(key=lambda x: x["machine_count"], reverse=True)
        
        
        current_machine = 0 
        
        
        for job in self.current_jobs: 
            machine_range = list(range(current_machine, current_machine + job["machine_count"]))
            for machine_id in machine_range:
                self.mark_machine_as_busy(machine_id)
            job["machines"] = machine_range
            current_machine += job["machine_count"]
            
        current_busy_machine_count = self.busy_machine_count
        
        assert current_busy_machine_count == initial_busy_machine_count
        
        # now we have defragmented the machines. Make another attempt to initiate the waiting jobs.
        
        # try to initiate the waiting jobs: 
        self.try_to_initiate_waiting_jobs()
        
        
    def find_machines_for_job(self, job_machine_count):
        
        available_machine_count = self.total_machine_count - self.busy_machine_count 
        if available_machine_count < job_machine_count:
            return None
        
        machines = [] 
        
        # machine selection strategy
        if self.strategy == "random":
            machines_needed = job_machine_count 
            while machines_needed > 0:
                machine_id = random.randint(0, self.total_machine_count - 1)
                if not self.is_busy[machine_id]:
                    machines_needed -= 1
                    machines.append(machine_id) 


        if self.strategy == "firstfit":
            start_index = 0
    
            while start_index <= self.total_machine_count - job_machine_count:
                if not any(self.is_busy[start_index : start_index + job_machine_count]):
                    machines = list(range(start_index, start_index + job_machine_count))
                    break   
                
                start_index += 1
                
            if len(machines) == 0:
                machines_needed = job_machine_count
                
                for i in range(self.total_machine_count):
                    if not self.is_busy[i]:
                        machines.append(i)
                        machines_needed -= 1
                        if machines_needed == 0:
                            break                
                
        if self.strategy == "bestfit":
            # attempt to assign total racks to the job. 
            available_racks = [] 
            needed_rack_count = int(math.ceil(job_machine_count / self.rack_size))
            
            # i is the starting machine of each rack. 
            for i in range(0, self.total_machine_count, self.rack_size):
                rack_available = not any(self.is_busy[i: i + self.rack_size])

                if rack_available:
                    available_racks.append(i)
                    
                if len(available_racks) == needed_rack_count:
                    break
                
            if len(available_racks) == needed_rack_count:
                # we have enough racks to assign the job.
                for i in range(len(available_racks)):
                    start_machine = available_racks[i]  
                    end_machine = start_machine + self.rack_size 
                    
                    machines.extend(list(range(start_machine, end_machine)))
                    
                # some machines in the last rack might be extra.
                machines = machines[:job_machine_count]
                
            else:
                return None # this should technically keep the fragmentation at zero 

            
                # get all the machines that we can get from full racks. 
                for i in range(len(available_racks)):
                    start_machine = available_racks[i]  
                    end_machine = start_machine + rack_size 
                    
                    machines.extend(list(range(start_machine, end_machine)))
                    
                machines_needed = job_machine_count - len(machines)
                scattered_machines = [] 
                
                already_added_machines = set(machines) 
                
                for j in range(machine_count):
                    if j not in already_added_machines and not self.is_busy[j]:
                        scattered_machines.append(j)
                        
                        if len(scattered_machines) == machines_needed:
                            break
                        
                machines.extend(scattered_machines)                        
                
    
        #####################################################################
        # sanity check ######################################################        
        #####################################################################

        if len(machines) != job_machine_count:
            return None

        for machine in machines:
            if self.is_busy[machine]:
                return None 

        #####################################################################
        # sanity check ######################################################        
        #####################################################################
        
        # the ring stuff. 
        if self.ring_mode == "random":
            random.shuffle(machines)
        elif self.ring_mode == "optimal":
            machines.sort()
            
        return machines     
    
    def measure_entrorpy(self, deduct_base_entropy = True):
        
        # what is deduct_base_entropy?
        # even in the most ideal case, there is some entropy.
        # this entropy is the entropy of the base case.
        # specifically, the ring has to cross every rack_size machines.
        # so if there's a job with 10 machines and racks are 8 machines each, 
        # there will be 2 cross rack flows. So the base cross

        cross_rack_flows = 0
        total_flows = 0   
        
        for job in self.current_jobs:
            machines = job["machines"] 

            for i in range(len(machines)):
                src_machine = machines[i] 
                dest_machine = machines[(i + 1) % len(machines)]
                
                src_rack = src_machine // self.rack_size
                dest_rack = dest_machine // self.rack_size

                if src_rack != dest_rack:   
                    cross_rack_flows += 1
                    
                total_flows += 1
            
            if deduct_base_entropy:
                least_needed_racks = int(math.ceil(len(machines) / self.rack_size))
                
                if least_needed_racks == 1:
                    min_possible_cross_rack_flows = 0
                else:
                    min_possible_cross_rack_flows = least_needed_racks
                                                        
                cross_rack_flows -= min_possible_cross_rack_flows            
            
        return cross_rack_flows / (total_flows)
    
    
    def send_to_waiting(self, job_id, duration, job_machines_count):
        self.waiting_jobs.append({
            "duration": duration,   
            "machine_count": job_machines_count,
            "id": job_id, 
        })
        
    def attempt_job_initiation(self, job_id, duration, job_machines_count): 
        backlog_count = sum([job["machine_count"] for job in self.waiting_jobs]) 
        if backlog_count > self.total_machine_count:
            self.send_to_waiting(job_id, duration, job_machines_count)
            return False 
        
        job_machines = self.find_machines_for_job(job_machines_count) 
        if job_machines is not None:
            self.initiate_job(job_id, duration, job_machines)            
            return True 
        else: 
            self.send_to_waiting(job_id, duration, job_machines_count)
            return False
            
            
    def initiate_job(self, job_id, duration, job_machines):        
        job_end_time = self.current_time + duration
        
        self.current_jobs.append({
            "start_time": self.current_time, 
            "end_time": job_end_time,    
            "machines": job_machines, 
            "machine_count": len(job_machines),
            "id": job_id 
        })

        # mark the machines as busy
        for machine_id in job_machines:
            self.mark_machine_as_busy(machine_id)
            
        # update the earliest job end time   
        if job_end_time < self.earliest_job_end:
            self.earliest_job_end = job_end_time

        if self.keep_history:
            print(f"Job {job_id} started at {self.current_time} and will end at {job_end_time}")
    
    def terminate_job(self, job):
        self.current_jobs.remove(job)
        self.completed_jobs.append(job)
        
        for machine_id in job["machines"]:
            self.mark_machine_as_free(machine_id)
                
        if self.keep_history:
            print(f"Job {job['id']} ended at {self.current_time}")  
        
        self.update_earliest_job_end()
        
       
    def update_earliest_job_end(self):  
        self.earliest_job_end = self.sim_length * 2
         
        for job in self.current_jobs:
            if job["end_time"] < self.earliest_job_end:
                self.earliest_job_end = job["end_time"]
                
            
            
    def simulate(self): 
        next_job_arrival = 0 
        
        # Run the simulation
        while self.current_time < self.sim_length:
            something_changed = False 
            
            if self.current_time % 1000000 == 0:
                something_changed = True
                # self.defragment()

            if self.current_time == self.earliest_job_end:  # some jobs are ending now. maybe more than one.
                something_changed = True 
                # which jobs are ending now?     
                ending_jobs = [] 
                for job in self.current_jobs:
                    if job["end_time"] == self.earliest_job_end:
                        ending_jobs.append(job)

                # terminate those jobs
                for job in ending_jobs:
                    self.terminate_job(job)
                                    
                self.try_to_initiate_waiting_jobs()
                    
            if self.current_time == next_job_arrival:  # next job is here  
                something_changed = True 
                self.job_id_counter += 1
                
                # non negotioable, these two numbers should be decided here 
                job_machine_count = self.get_job_machine_count()
                job_duration = self.get_job_duration()
                job_id = self.job_id_counter             
                
                self.attempt_job_initiation(job_id, job_duration, job_machine_count)
                    
                # regardless, the next job will arrive in a while,
                next_interarrival_time = self.get_interarrival_time()             
                self.inter_arrival_times.append(next_interarrival_time)
                next_job_arrival = self.current_time + next_interarrival_time 
            

            if self.keep_history:
                if something_changed:
                    new_entropy = self.measure_entrorpy(deduct_base_entropy=True)
                    self.entropies.append(new_entropy)            

                    new_base_entropy = self.measure_entrorpy(deduct_base_entropy=False)
                    self.base_entropies.append(new_base_entropy)  

                else: 
                    last_entropy = self.entropies[-1] if len(self.entropies) > 0 else 0 
                    self.entropies.append(last_entropy)
                    
                    last_base_entropy = self.base_entropies[-1] if len(self.base_entropies) > 0 else 0
                    self.base_entropies.append(last_base_entropy)
                                    
                utilization = self.busy_machine_count / self.total_machine_count   
                self.utilizations.append(utilization)
                            
                backlog_machine_count = sum([job["machine_count"] for job in self.waiting_jobs])
                servicing_machine_count = self.busy_machine_count
                service_rate = servicing_machine_count / (servicing_machine_count + backlog_machine_count)
                self.service_rates.append(service_rate)
            
            self.current_time += 1 
            
        return self.entropies     
                   
def plot_cdf(data, title, xlabel, ylabel, filename):
    import matplotlib.pyplot as plt

    data.sort()
    yvals = [i / len(data) for i in range(len(data))]
    plt.plot(data, yvals)
    
    
    mean = sum(data) / len(data)
    plt.axvline(x=mean, color='r', linestyle='--', label="mean" + str(xlabel))
    # annotate the mean
    plt.text(mean * 1.1, 0.5, int(mean), rotation=90)  
                        
                        
    percentile_90 = data[int(len(data) * 0.9)]
    plt.axvline(x=percentile_90, color='g', linestyle='--', label="90th percentile" + str(xlabel))
    plt.text(percentile_90 * 1.1, 0.5, int(percentile_90), rotation=90)
    
    
    max = data[-1] 
    plt.axvline(x=max, color='b', linestyle='--', label="max" + str(xlabel))
    plt.text(max * 1.1, 0.5, int(max), rotation=90)
    
                        
    plt.ylabel(ylabel)
    plt.xlabel(xlabel)
    plt.title(title)
    plt.xscale("log")   
    
    # legend outside the plot, top right
    plt.legend(loc='upper left', bbox_to_anchor=(1.05, 1))
    plt.savefig(filename, dpi = 300, bbox_inches = "tight") 
    plt.clf()
    
def downsample(data, step=None):
    if step is None: 
        step = len(data) // 100
    
    def my_mean(arr):
        return sum(arr) / len(arr)  
    
    return [my_mean(data[i:i + step]) for i in range(0, len(data), step)]


def get_job_placement_info(strategy, ring_mode, rack_size, 
                           total_machine_count, sim_length):
    # the seed is supposed to have been set before calling this function.
    
    s = Simulation(strategy=strategy,
                   ring_mode=ring_mode,
                   rack_size=rack_size,
                   total_machine_count=total_machine_count,
                   sim_length=sim_length,
                   keep_history=False)
    s.simulate()    
    
    job_placement_info = [] 
    i = 0 
    for job in s.current_jobs:
        job_placement_info.append({
            "job_id": i,
            "machines": job["machines"],
            "machine_count": job["machine_count"], 
        })
        i += 1
        
    return job_placement_info                 

def main():
    import matplotlib.pyplot as plt
    
    base_seed = 58
    
    for strategy in ["firstfit"]:
        for ring_mode in ["optimal"]:

            random.seed(base_seed)
            
            sim_length = 1000000
            rack_size = 10 
            rack_count = 25
            total_machine_count = rack_size * rack_count     

            s = Simulation(strategy=strategy, 
                           ring_mode=ring_mode, 
                           rack_size=rack_size, 
                           total_machine_count=total_machine_count, 
                           sim_length=sim_length)
            
            s.simulate()    
            
            
            #####################################################################
            
            plt.plot(downsample(s.entropies), label="entropy")    
            plt.plot(downsample(s.utilizations), label="utilization")   
            plt.plot(downsample(s.service_rates), label="service rate")
            plt.plot(downsample(s.base_entropies), label="base entropy")
            
            plt.ylabel("entropy/utilization")
            plt.xlabel("time")
            plt.title("Strategy: {}, Ring Mode: {}".format(strategy, ring_mode))
            
            plt.ylim(-0.02, 1.02)
            
            plt.legend(loc='upper left', bbox_to_anchor=(1.05, 1))
            plt.savefig("{}/{}_{}.png".format(this_dir, strategy, ring_mode))
            plt.clf()
            
            #####################################################################
            
            job_machine_count_all = [job["machine_count"] for job in s.completed_jobs]  
            job_duration_all = [job["end_time"] - job["start_time"] for job in s.completed_jobs]
        
            plot_cdf(job_machine_count_all, "Job Machine Count CDF", "job machine count", "CDF", "{}/{}_{}_machine_cdf.png".format(this_dir, strategy, ring_mode))
            plot_cdf(job_duration_all, "Job Duration CDF", "job duration", "CDF", "{}/{}_{}_duration_cdf.png".format(this_dir, strategy, ring_mode))
            plot_cdf(s.inter_arrival_times, "Interarrival Time CDF", "interarrival time", "CDF", "{}/{}_{}_interarrival_cdf.png".format(this_dir, strategy, ring_mode))

if __name__ == "__main__":
    main()    