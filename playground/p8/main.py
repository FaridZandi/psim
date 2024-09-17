import random 
import matplotlib.pyplot as plt
import os 
import sys 
import math 

random.seed(45)

sim_length = 1000000  
rack_size = 10 
rack_count = 50 
machine_count = rack_size * rack_count   

# this script path: 
this_path = os.path.dirname(os.path.abspath(__file__))
this_dir = os.path.dirname(this_path)    

def get_job_machine_count():
    return random.randint(2, 18) # mean 10

def get_job_duration():
    return random.randint(1, 20000) # mean 10000

def get_interarrival_time():
    return random.randint(1, 400) # mean 200


class Simulation():    
    
    def __init__(self, strategy, ring_mode):
        self.strategy = strategy
        self.ring_mode = ring_mode 
        
        self.current_jobs = [] 
        self.waiting_jobs = []
        
        self.is_busy = [False] * machine_count    
        self.busy_machine_count = 0 
        
        self.current_time = 0
        self.earliest_job_end = sim_length * 2 
        self.job_id_counter = 0

        self.entropies = []
        self.utilizations = []     
        self.overload = []     
    
    def mark_machine_as_busy(self, machine_id): 
        self.is_busy[machine_id] = True
        self.busy_machine_count += 1

    def mark_machine_as_free(self, machine_id): 
        self.is_busy[machine_id] = False
        self.busy_machine_count -= 1    

    def find_machines_for_job(self, job_machine_count):
        
        available_machine_count = machine_count - self.busy_machine_count 
        if available_machine_count < job_machine_count:
            return None
        
        machines = [] 
        
        # machine selection strategy
        if self.strategy == "random":
            machines_needed = job_machine_count 
            while machines_needed > 0:
                machine_id = random.randint(0, machine_count - 1)
                if not self.is_busy[machine_id]:
                    machines_needed -= 1
                    machines.append(machine_id) 


        if self.strategy == "firstfit":
            start_index = 0
    
            while start_index <= machine_count - job_machine_count:
                if not any(self.is_busy[start_index : start_index + job_machine_count]):
                    machines = list(range(start_index, start_index + job_machine_count))
                    break   
                
                start_index += 1
                
            if len(machines) == 0:
                machines_needed = job_machine_count
                
                for i in range(machine_count):
                    if not self.is_busy[i]:
                        machines.append(i)
                        machines_needed -= 1
                        if machines_needed == 0:
                            break                
                
        if self.strategy == "bestfit":
            # attempt to assign total racks to the job. 
            available_racks = [] 
            needed_rack_count = int(math.ceil(job_machine_count / rack_size))
            
            # i is the starting machine of each rack. 
            for i in range(0, machine_count, rack_size):
                rack_available = not any(self.is_busy[i: i + rack_size])

                if rack_available:
                    available_racks.append(i)
                    
                if len(available_racks) == needed_rack_count:
                    break
                
            if len(available_racks) == needed_rack_count:
                # we have enough racks to assign the job.
                for i in range(len(available_racks)):
                    start_machine = available_racks[i]  
                    end_machine = start_machine + rack_size 
                    
                    machines.extend(list(range(start_machine, end_machine)))
                    
                # some machines in the last rack might be extra.
                machines = machines[:job_machine_count]
                
            else:
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
    
    def measure_entrorpy(self):

        cross_rack_flows = 0
        within_rack_flows = 0   
        
        for job in self.current_jobs:
            machines = job["machines"] 

            for i in range(len(machines)):
                src_machine = machines[i] 
                dest_machine = machines[(i + 1) % len(machines)]
                
                src_rack = src_machine // rack_size
                dest_rack = dest_machine // rack_size

                if src_rack != dest_rack:   
                    cross_rack_flows += 1
                else:
                    within_rack_flows += 1
            
        return cross_rack_flows / (cross_rack_flows + within_rack_flows)
    
    def attempt_job_initiation(self, job_id, duration, job_machines_count): 
        job_machines = self.find_machines_for_job(job_machines_count) 
            
        if job_machines is not None:
            self.initiate_job(job_id, duration, job_machines)            
            return True 
        else: 
            self.waiting_jobs.append({
                "duration": duration,   
                "machine_count": job_machines_count,
                "id": job_id, 
            })
            return False
            
            
    def initiate_job(self, job_id, duration, job_machines):        
        job_end_time = self.current_time + duration
        
        self.current_jobs.append({
            "start_time": self.current_time, 
            "end_time": job_end_time,    
            "machines": job_machines, 
            "id": job_id 
        })

        # mark the machines as busy
        for machine_id in job_machines:
            self.mark_machine_as_busy(machine_id)
            
        # update the earliest job end time   
        if job_end_time < self.earliest_job_end:
            self.earliest_job_end = job_end_time

        print(f"Job {job_id} started at {self.current_time} and will end at {job_end_time}")
    
    def terminate_job(self, job):
        self.current_jobs.remove(job)
        
        for machine_id in job["machines"]:
            self.mark_machine_as_free(machine_id)
                    
        print(f"Job {job['id']} ended at {self.current_time}")  
        
        self.update_earliest_job_end()
        
       
    def update_earliest_job_end(self):  
        self.earliest_job_end = sim_length * 2
         
        for job in self.current_jobs:
            if job["end_time"] < self.earliest_job_end:
                self.earliest_job_end = job["end_time"]
                
            
            
    def simulate(self): 
        next_job_arrival = 0 
        
        
        # Run the simulation
        while self.current_time < sim_length:
            something_changed = False 
            
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
                                    
                # are there any waiting jobs?
                waiting_jobs_copy = self.waiting_jobs.copy() 
                self.waiting_jobs.clear()
                waiting_jobs_count = len(waiting_jobs_copy) 

                started_jobs = 0        
                for job in waiting_jobs_copy :
                    could_start = self.attempt_job_initiation(job["id"], job["duration"], job["machine_count"]) 
                    if could_start:
                        started_jobs += 1
                        
                    
                print(f"Started {started_jobs} jobs out of {waiting_jobs_count} waiting jobs")
                    
            if self.current_time == next_job_arrival:  # next job is here  
                something_changed = True 
                self.job_id_counter += 1
                
                # non negotioable, these two numbers should be decided here 
                job_machine_count = get_job_machine_count()
                job_duration = get_job_duration()
                job_id = self.job_id_counter             
                
                self.attempt_job_initiation(job_id, job_duration, job_machine_count)
                    
                # regardless, the next job will arrive in a while,             
                next_job_arrival = self.current_time + get_interarrival_time() 
            
            if something_changed:
                new_entropy = self.measure_entrorpy() 
                self.entropies.append(new_entropy)            
            else: 
                last_entropy = self.entropies[-1] if len(self.entropies) > 0 else 0 
                self.entropies.append(last_entropy)
                                
            utilization = self.busy_machine_count / machine_count   
            self.utilizations.append(utilization)
                        
            overload = sum([job["machine_count"] for job in self.waiting_jobs]) / machine_count
            self.overload.append(overload)
            
            self.current_time += 1 
            
        return self.entropies     
                    
if __name__ == "__main__":
    
    for strategy in ["bestfit"]:
        for ring_mode in ["optimal"]:
            s = Simulation(strategy, ring_mode)
            s.simulate()    
            
            plt.plot(s.entropies, label="entropy")    
            plt.plot(s.utilizations, label="utilization")   
            plt.plot(s.overload, label="overload")
            
            plt.ylabel("entropy/utilization")
            plt.xlabel("time")
            plt.title("Strategy: {}, Ring Mode: {}".format(strategy, ring_mode))
            
            plt.ylim(0, 1)
            
            plt.legend()
            plt.savefig("{}/{}_{}.png".format(this_dir, strategy, ring_mode))
            plt.clf()