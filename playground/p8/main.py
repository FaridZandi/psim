import random 
import os 
import sys 
import math 
from pprint import pprint

base_seed = 44233
random.seed(base_seed)

# this script path: 
this_dir = os.path.dirname(os.path.abspath(__file__))



def get_incremented_number():
    filename = "{}/number.txt".format(this_dir)    
    
    # Step 1: Read the current number from a file
    if os.path.exists(filename):
        with open(filename, 'r') as file:
            current_number = int(file.read().strip())
    else:
        current_number = 0  # Default to 0 if file doesn't exist

    # Step 2: Increment the number
    incremented_number = current_number + 1

    # Step 3: Write the updated number back to the file
    with open(filename, 'w') as file:
        file.write(str(incremented_number))

    # Step 4: Return the updated number
    return incremented_number


class Simulation():    
    
    def __init__(self, strategy, ring_mode, sim_length, 
                 rack_size, rack_count, setting_dir, 
                 verbose = False, alpha = 0):
        
        self.rack_size = rack_size  
        self.rack_count = rack_count            
        self.total_machine_count = rack_size * rack_count   
        self.setting_dir = setting_dir  
        self.alpha = alpha  
        self.verbose = verbose  
        
        
        self.sim_length = sim_length    
        
        self.strategy = strategy
        self.ring_mode = ring_mode 
        
        self.current_jobs = [] 
        self.waiting_jobs = []
        self.completed_jobs = []
        self.current_jobs_map = {}
        
        self.is_busy = [False] * self.total_machine_count    
        self.machine_job_assignment = [None] * self.total_machine_count
        self.busy_machine_count = 0 
        
        self.current_time = 0
        self.earliest_job_end = sim_length * 2 
        self.job_id_counter = 0

        self.entropies = []
        self.base_entropies = [] 
        self.utilizations = []     
        self.service_rates = []     
        self.inter_arrival_times = [] 
        self.max_jobs_in_same_rack = []
    
    def get_job_machine_count(self):
        # return random.randint(2, 18) # mean 10

        # a pareto distribution with with mean 10 
        r = int(random.paretovariate(0.5)) + 2
        if r > (self.total_machine_count // 4):
            r = (self.total_machine_count // 4)
            
        return r 

    def get_job_duration(self):
        # return random.randint(1, 20000) # mean 10000

        r = int(random.paretovariate(1.16)) * 1500 + 2000
        return r

    def get_interarrival_time(self):
        # return random.randint(1, 240) # mean 200

        r = int(random.paretovariate(1.16)) * 120 + 1
        
        if r > 10000:
            r = 10000
            
        return r

    def mark_machine_as_busy(self, machine_id, job_id): 
        self.is_busy[machine_id] = True
        self.machine_job_assignment[machine_id] = job_id
        self.busy_machine_count += 1

    def mark_machine_as_free(self, machine_id): 
        self.is_busy[machine_id] = False
        self.machine_job_assignment[machine_id] = None
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
                could_start = self.attempt_job_initiation(job["id"], job["duration"], job["machine_count"], job["arrival_time"])
            
            if could_start:
                started_jobs += 1
                
        if self.verbose:
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
                self.mark_machine_as_busy(machine_id, job["id"])
            job["machines"] = machine_range
            current_machine += job["machine_count"]
            
        current_busy_machine_count = self.busy_machine_count
        
        assert current_busy_machine_count == initial_busy_machine_count
        
        # now we have defragmented the machines. Make another attempt to initiate the waiting jobs.
        
        # try to initiate the waiting jobs: 
        self.try_to_initiate_waiting_jobs()
        
    def find_free_extent(self, job_machine_count):
        # Initialize variables
        start_index = 0
        current_free_count = 0

        # Create a sliding window to count free machines in the initial window
        for i in range(job_machine_count):
            if not self.is_busy[i]:
                current_free_count += 1

        # Check if the initial window is free
        if current_free_count == job_machine_count:
            return list(range(start_index, start_index + job_machine_count))

        # Slide the window across the entire list
        for start_index in range(1, self.total_machine_count - job_machine_count + 1):
            # Slide the window to the right
            if not self.is_busy[start_index - 1]:
                current_free_count -= 1
            if not self.is_busy[start_index + job_machine_count - 1]:
                current_free_count += 1

            # Check if the new window is free
            if current_free_count == job_machine_count:
                return list(range(start_index, start_index + job_machine_count))

        # Return an empty list if no free extent is found
        return []
    
    
    
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


        if self.strategy == "firstfit" or self.strategy == "firstfit_strict":
            machines = self.find_free_extent(job_machine_count) 
            
            if len(machines) == 0:
                if self.strategy == "firstfit_strict":
                    return None
                
                if self.strategy == "firstfit":
                    # get the random number between 0 and 1 
                    r = random.random()
                    if r < self.alpha:
                        return None
                
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
         
         
        if total_flows == 0:
            return 0
        else: 
            return cross_rack_flows / (total_flows)
    
    def draw_rect(self, ax, bottom_left, width, height, color, job_id, text_color):
        from matplotlib import patches  
        
        if job_id is None:
            label = None
            hatch = "x" 
        else:
            label = str(job_id)
            hatch = None
            
        rect = patches.Rectangle(bottom_left, width, height, linewidth=1, edgecolor='black', facecolor=color, hatch=hatch)
        ax.add_patch(rect)
        
        ax.text(bottom_left[0] + width/2, 
                bottom_left[1] + height/2, 
                label, 
                horizontalalignment='center', 
                verticalalignment='center', 
                fontsize=15, 
                color=text_color, 
                # make it bold
                fontweight='bold'
                )
        


    def visualize_assignments(self):
        import matplotlib.pyplot as plt
        
        fig, ax = plt.subplots(figsize=(self.rack_size, self.rack_count))
        
        color_list = ["red", "blue", "green", "yellow", "purple", "orange", "cyan", "magenta", "brown", "pink", "white"] 
        color_list_index = 0    
        
        for rack_idx in range(self.rack_count):
            for machine_idx in range(self.rack_size):
                machine_id = rack_idx * self.rack_size + machine_idx
                
                if machine_id >= len(self.machine_job_assignment):
                    break

                job_id = self.machine_job_assignment[machine_id]
                
                # generate a random color for the job
                if job_id is None:
                    color = "gray"
                else: 
                    job_info = self.current_jobs_map[job_id]
                    if "color" in job_info:
                        color = job_info["color"]
                    else:
                        color = color_list[color_list_index]
                        color_list_index += 1 
                        color_list_index %= len(color_list)
                        job_info["color"] = color
                                        
                x_position = machine_idx
                y_position = rack_idx
                
                
                # translate the color from english name to hex
                color = plt.cm.colors.to_hex(color)
                
                # if the color is dark, write the job id in white
                if sum([int(color.lstrip("#")[i:i+2], 16) for i in (0, 2, 4)]) < 382:
                    text_color = "white"
                else:
                    text_color = "black"
                    
                self.draw_rect(ax, (x_position, y_position), 1, 1, color, job_id, text_color)          
                
        ax.set_xlim(0, self.rack_size)
        ax.set_ylim(0, self.rack_count)
        
        # ax.set_xticks(np.arange(self.rack_size) + 0.5)
        # ax.set_yticks(np.arange(self.rack_count) + 0.5)
        # do the same without numpy
        ax.set_xticks([i + 0.5 for i in range(self.rack_size)])
        ax.set_yticks([i + 0.5 for i in range(self.rack_count)])
        
        
        ax.set_xticklabels([f'M {i+1}' for i in range(self.rack_size)])
        ax.set_yticklabels([f'R {i+1}' for i in range(self.rack_count)])
        
        plt.title("Machine Job Assignments")
        plt.xlabel("Machines")
        plt.ylabel("Racks")
        plt.savefig("{}/machine_job_assignments.png".format(self.setting_dir), 
                    dpi = 300, bbox_inches = "tight")
        
    def calculated_max_jobs_in_same_rack(self):
        max_jobs_in_same_rack = 0    
        
        for i in range(self.rack_count): 
            rack_start_machine = i * self.rack_size 
            rack_end_machine = rack_start_machine + self.rack_size
            
            rack_job_ids = [self.machine_job_assignment[j] for j in range(rack_start_machine, rack_end_machine)]
            jobs_in_rack = {}

            for job_id in rack_job_ids:
                if job_id is not None:
                    if job_id in jobs_in_rack:
                        jobs_in_rack[job_id] += 1
                    else:
                        jobs_in_rack[job_id] = 1

            rack_jobs_unique = list(jobs_in_rack.keys())
            
            inter_rack_jobs = [] 
            for job_id in rack_jobs_unique:
                total_job_machine_count = self.current_jobs_map[job_id]["machine_count"]
                this_rack_job_machine_count = jobs_in_rack[job_id]
                
                if total_job_machine_count != this_rack_job_machine_count:
                    inter_rack_jobs.append(job_id)
            
            rack_jobs_unique_count = len(inter_rack_jobs)
            
            
            if self.current_time == 75000 and self.verbose: 
                print(f"Rack {i}")  
                pprint(rack_job_ids)
                pprint(rack_jobs_unique)    
                pprint(inter_rack_jobs)    
                pprint(rack_jobs_unique_count)
                
            if rack_jobs_unique_count > max_jobs_in_same_rack:
                max_jobs_in_same_rack = rack_jobs_unique_count
        
        if self.current_time == 75000 and self.verbose: 
            pprint(max_jobs_in_same_rack)  
            self.visualize_assignments()
            input("Press Enter to continue...")  
                
        return max_jobs_in_same_rack / self.rack_size       
    
    def send_to_waiting(self, job_id, duration, job_machines_count, arrival_time):
        self.waiting_jobs.append({
            "arrival_time": arrival_time,   
            "duration": duration,   
            "machine_count": job_machines_count,
            "id": job_id, 
        })
        
    def attempt_job_initiation(self, job_id, duration, job_machines_count, arrival_time): 
        job_machines = self.find_machines_for_job(job_machines_count) 
        if job_machines is not None:
            self.initiate_job(job_id, duration, job_machines, arrival_time)            
            return True 
        else: 
            self.send_to_waiting(job_id, duration, job_machines_count, arrival_time)
            return False
            
            
    def initiate_job(self, job_id, duration, job_machines, arrival_time):        
        job_end_time = self.current_time + duration
        
        new_job = {
            "start_time": self.current_time, 
            "end_time": job_end_time,    
            "machines": job_machines, 
            "machine_count": len(job_machines),
            "arrival_time": arrival_time,   
            "id": job_id 
        }
        
        self.current_jobs.append(new_job)
        self.current_jobs_map[job_id] = new_job

        # mark the machines as busy
        for machine_id in job_machines:
            self.mark_machine_as_busy(machine_id, job_id)
            
        # update the earliest job end time   
        if job_end_time < self.earliest_job_end:
            self.earliest_job_end = job_end_time

        if self.verbose:
            print(f"Job {job_id} started at {self.current_time} and will end at {job_end_time}")
    
    def terminate_job(self, job):
        self.current_jobs.remove(job)
        self.completed_jobs.append(job)
        
        for machine_id in job["machines"]:
            self.mark_machine_as_free(machine_id)
        
        if self.verbose:
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
                
                job_spec_magic = 392384
                inter_arrival_magic = 234234
                
                random.seed(base_seed + self.job_id_counter + job_spec_magic)
                job_machine_count = self.get_job_machine_count()
                job_duration = self.get_job_duration()
                job_id = self.job_id_counter             
                
                self.attempt_job_initiation(job_id, job_duration, job_machine_count, self.current_time)
                    
                # regardless, the next job will arrive in a while,
                
                random.seed(base_seed + self.job_id_counter + inter_arrival_magic)
                next_interarrival_time = self.get_interarrival_time()             
                self.inter_arrival_times.append(next_interarrival_time)
                next_job_arrival = self.current_time + next_interarrival_time 
            
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
            
            if (servicing_machine_count + backlog_machine_count) == 0: 
                service_rate = 0    
            else:
                service_rate = servicing_machine_count / (servicing_machine_count + backlog_machine_count)
            
            self.service_rates.append(service_rate)
            
            self.max_jobs_in_same_rack.append(self.calculated_max_jobs_in_same_rack())
            
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
    plt.xscale("symlog")   
    
    # legend outside the plot, top right
    plt.legend(loc='upper left', bbox_to_anchor=(1.05, 1))
    plt.savefig(filename, dpi = 300, bbox_inches = "tight") 
    plt.clf()
    
def downsample(data, step=None):
    if step is None: 
        step = len(data) // 500
    
    def my_mean(arr):
        return sum(arr) / len(arr)  
    
    return [my_mean(data[i:i + step]) for i in range(0, len(data), step)]


def get_job_placement_info(strategy, job_machine_count, rack_size, rack_count, simulation_len):
    
    s = Simulation(strategy=strategy,
                   job_machine_count=job_machine_count,
                   rack_size=rack_size,
                   rack_count=rack_count,
                   sim_length=simulation_len, 
                   verbose=False)
    
    job_placement_info = [] 
    
    i = 0 
    for job in s.current_jobs:
        job_placement_info.append({
            "job_id": i,
            "machines": job["machines"],
        })
        i += 1
        
    pprint(job_placement_info)     
    
    return job_placement_info                 

def main():
    import matplotlib.pyplot as plt

    exp_number = get_incremented_number()
    exp_dir = "{}/out/{}/".format(this_dir, exp_number)  
    os.makedirs(this_dir, exist_ok=True)

    for alpha in [0.9, 0.91, 0.92, 0.93, 0.94, 0.95, 0.96, 0.97, 0.98, 0.99, 1.0]:
        for strategy in ["firstfit"]:
            for ring_mode in ["optimal"]:
                
                sim_length = 1000000
                rack_size = 16
                rack_count = 15
                
                setting_dir = "{}/{}_{}_{}/".format(exp_dir, strategy, ring_mode, alpha)
                
                os.makedirs(setting_dir)
                
                s = Simulation(strategy=strategy, 
                            ring_mode=ring_mode, 
                            sim_length=sim_length,
                            rack_size=rack_size, 
                            rack_count=rack_count, 
                            setting_dir=setting_dir,
                            verbose=True,  
                            alpha=alpha)
                
                s.simulate()    
                
                #####################################################################
                
                
                # history = { 
                #     "entropies": downsample(s.entropies),
                #     "base_entropies": downsample(s.base_entropies),
                #     "utilizations": downsample(s.utilizations),
                #     "service_rates": downsample(s.service_rates),
                #     "max_jobs_in_same_rack": downsample(s.max_jobs_in_same_rack)
                # } 
                
                # fig, ax = plt.subplots(2, 1, figsize=(6, 3), sharex=True)    
                    
                # ax[0].plot(downsample(s.entropies), label="entropy")    
                # ax[0].plot(downsample(s.base_entropies), label="base entropy")
                # ax[0].plot(downsample(s.max_jobs_in_same_rack), label="max jobs in same rack")
                # ax[0].set_ylabel("entropy")
                # ax[0].legend(loc='upper left', bbox_to_anchor=(1.05, 1))

                # ax[1].plot(downsample(s.service_rates), label="service rate")
                # ax[1].plot(downsample(s.utilizations), label="utilization") 
    
                # ax[1].set_ylabel("utilization")
                # ax[1].legend(loc='upper left', bbox_to_anchor=(1.05, 1))
                
                # # plt.ylim(-0.02, 1.02)
                # plt.title("Strategy: {}, Ring Mode: {}".format(strategy, ring_mode))
                # plt.xlabel("time")
                
                # plt.savefig("{}/output.png".format(setting_dir), dpi = 300, bbox_inches = "tight")
                # plt.clf()
                
                # #####################################################################
                
                # job_machine_count_all = [job["machine_count"] for job in s.completed_jobs]  
                # job_duration_all = [job["end_time"] - job["start_time"] for job in s.completed_jobs]
                # wait_times = [job["start_time"] - job["arrival_time"] for job in s.completed_jobs]
            
                # plot_cdf(job_machine_count_all, "Job Machine Count CDF", "job machine count", "CDF", "{}/machine_cdf.png".format(setting_dir))
                # plot_cdf(job_duration_all, "Job Duration CDF", "job duration", "CDF", "{}/duration_cdf.png".format(setting_dir))
                # plot_cdf(s.inter_arrival_times, "Interarrival Time CDF", "interarrival time", "CDF", "{}/interarrival_cdf.png".format(setting_dir))
                # plot_cdf(wait_times, "Wait Time CDF", "wait time", "CDF", "{}/wait_cdf.png".format(setting_dir))
                
                # #####################################################################
                
                # with open("{}/job_info.csv".format(setting_dir), "w+") as f:
                #     # sort completed jobs by start time
                #     s.completed_jobs.sort(key=lambda x: x["id"])
                    
                #     for job in s.completed_jobs:
                #         f.write(f"{job['id']},{job['start_time']},{job['arrival_time']},{job['end_time']},{job['machine_count']}\n")
                        
                        
                avg_util = sum(s.utilizations) / len(s.utilizations)
                avg_max_job_in_same_rack = sum(s.max_jobs_in_same_rack) / len(s.max_jobs_in_same_rack)  

                print("Alpha: {}, avg_util: {}, avg_max_job_in_same_rack: {}".format(alpha,
                                                                                   avg_util,
                                                                                   avg_max_job_in_same_rack))
                                                                                       
                
if __name__ == "__main__":
    main()    