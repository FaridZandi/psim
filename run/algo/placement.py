import random 
import json 
import time 
from algo.placement_sim import get_job_placement_info
from pprint import pprint
import os 
import copy
from processing.flowprogress import get_job_profiles
from processing.sim_finish import get_simulation_finish_time
import pickle as pkl    
import math 
from utils.util import rage_quit


def perturb_placement(jobs): 
    job1 = random.choice(jobs)
    job2 = random.choice(jobs)
    
    job1_machine_index = random.randint(0, len(job1["machines"]) - 1) 
    job2_machine_index = random.randint(0, len(job2["machines"]) - 1)
    
    # swap the machines.
    job1_machine = job1["machines"][job1_machine_index]
    job2_machine = job2["machines"][job2_machine_index]
    
    job1["machines"][job1_machine_index] = job2_machine
    job2["machines"][job2_machine_index] = job1_machine
    
def measure_entrorpy(jobs, rack_size):
    cross_rack_flows = 0
    total_flows = 0   
    
    for job in jobs:
        machines = job["machines"] 

        for i in range(len(machines)):
            src_machine = machines[i] 
            dest_machine = machines[(i + 1) % len(machines)]
            
            src_rack = src_machine // rack_size
            dest_rack = dest_machine // rack_size

            if src_rack != dest_rack:   
                cross_rack_flows += 1
                
            total_flows += 1
            
    return cross_rack_flows / (total_flows)

def generate_job_basics(options, run_context, job_machine_counts=None): 
    attempts = 0 

    cmmcmp_range = run_context["selected-setting"]["cmmcmp-range"]        

    closest_distance = 1e9
    closest_jobs = None 
    closest_assigned_machines = 0
    closest_ratio = 0
    
    while attempts < 1000:
        jobs, assigned_machines = generate_job_basics_(options, run_context, job_machine_counts) 

        ratios = [] 
        
        for job in jobs:
            comp_time = job["comp_size"] 
            comm_time = 2 * job["comm_size"] / options["link-bandwidth"]
            ratio = comm_time / comp_time 
            ratios.append(round(ratio, 2))
            
        average_ratio = sum(ratios) / len(ratios) 
        
        
        if average_ratio >= cmmcmp_range[0] and average_ratio <= cmmcmp_range[1]:
            closest_jobs = jobs
            closest_assigned_machines = assigned_machines
            
            break
        else:
            distance = min(abs(average_ratio - cmmcmp_range[0]), abs(average_ratio - cmmcmp_range[1]))

            if distance < closest_distance: 
                closest_distance = distance
                closest_jobs = jobs
                closest_assigned_machines = assigned_machines
                closest_ratio = average_ratio
        
        attempts += 1 

    print("ratios: ", ratios, " average_ratio: ", average_ratio, " acceptable range: ", cmmcmp_range, " attempts: ", attempts)  

    if attempts == 1000:    
        print("Warning: Could not find a job with the desired communication/computation ratio.")

    return closest_jobs, closest_assigned_machines, closest_ratio

def generate_job_basics_(options, run_context, job_machine_counts=None):
    machine_count = options["machine-count"]
    
    jobs_machine_count_high = run_context["selected-setting"]["jobs-machine-count-high"]
    jobs_machine_count_low = run_context["selected-setting"]["jobs-machine-count-low"]
    
    jobs = [] 
    
    machines_left = machine_count 
    current_job_id = 1 
    assigned_machines = 0 
    # random_mode = "either_or"
    random_mode = "range"

    # assigning 1 machine to a job would be bad, beceasse there would be no communication. 
    # so if just one machine is left, then we skip it.    
    while True:
        if job_machine_counts is not None: # if we have a list of job machine counts, then we use that.
            if len(job_machine_counts) == 0: # if we run out of job machine counts, then we break.
                break 
        else: 
            if machines_left < 2: # if we have just one machine left, then we break. 
                break                   
                 
        if job_machine_counts is not None:
            this_job_machine_count = job_machine_counts.pop(0)
        else:
            if random_mode == "either_or":
                this_job_machine_count = random.choice([jobs_machine_count_low, jobs_machine_count_high])  
            elif random_mode == "range":
                this_job_machine_count = random.randint(jobs_machine_count_low, jobs_machine_count_high)
            
        if this_job_machine_count > machines_left:
            this_job_machine_count = machines_left    
        
        job_communication_size = random.choice(run_context["selected-setting"]["comm-size"])   
        job_computation_size = random.choice(run_context["selected-setting"]["comp-size"])    
        job_layer_count = random.choice(run_context["selected-setting"]["layer-count"])  
        
        # TODO: wish I could be changing the iter count based on the job size. But to get 
        # a good estimate of the job size, I will need to do the profiling. To be fair, 
        # it's not a crazy idea to do the profiling here. becuase it's supposed to be 
        # independent of the timing. 
        job_iter_count = random.choice(run_context["selected-setting"]["iter-count"])
        
        jobs.append({
            "job_id": current_job_id,   
            "machine_count": this_job_machine_count, 
            "comm_size": job_communication_size,
            "comp_size": job_computation_size,
            "layer_count": job_layer_count,
            "iter_count": job_iter_count,
            "machines": []
        })
        
        machines_left -= this_job_machine_count 
        assigned_machines += this_job_machine_count     
        current_job_id += 1
        
        
        if "job-count" in run_context["selected-setting"]:
            if run_context["selected-setting"]["job-count"]:
                if current_job_id >= run_context["selected-setting"]["job-count"]:
                    break
                
    return jobs, assigned_machines
        


def generate_random_placement_file(options, run_context):
    machine_count = options["machine-count"]
    all_machines = list(range(0, machine_count))
    
    jobs, _, cmmcmp_ratio = generate_job_basics(options, run_context)  
    
    for job in jobs:
        job["machines"] = random.sample(all_machines, job["machine_count"])
        for machine in job["machines"]:
            all_machines.remove(machine)

    return jobs, cmmcmp_ratio


def generate_compact_placement_file(options, run_context):
    machine_count = options["machine-count"]
    all_machines = list(range(0, machine_count))

    jobs, _, cmmcmp_ratio = generate_job_basics(options, run_context)

    for job in jobs:
        job["machines"] = all_machines[:job["machine_count"]]
        all_machines = all_machines[job["machine_count"]:]

    return jobs, cmmcmp_ratio

def generate_entropy_placement_file(options, run_context):  
    desired_entropy = run_context["placement-parameters"]["desired-entropy"]
    jobs, cmmcmp_ratio = generate_compact_placement_file(options, run_context)

    current_entropy = measure_entrorpy(jobs, options["ft-server-per-rack"])
    
    perturbation_count = 0 
    max_perturbation_count = 1000   
    
    while current_entropy < desired_entropy and perturbation_count < max_perturbation_count:    
        perturb_placement(jobs)
        perturbation_count += 1
        current_entropy = measure_entrorpy(jobs, options["ft-server-per-rack"])

    return jobs, cmmcmp_ratio


def generate_semirandom_placement_file(options, run_context, fragmentation_factor):
    machine_count = options["machine-count"]
    allocated_machines = 0     
    available_machines = [True] * machine_count
    
    jobs, assigned_machines, cmmcmp_ratio = generate_job_basics(options, run_context)  
    
    while allocated_machines < assigned_machines: 
        # take a random job that is under allocated.    
        under_allocated_jobs = [job for job in jobs if len(job["machines"]) < job["machine_count"]] 
        
        if len(under_allocated_jobs) == 0:
            print("Error: No under allocated jobs left, but the number of allocated machines is less than the number of assigned machines.")
            rage_quit("Error: No under allocated jobs left, but the number of allocated machines is less than the number of assigned machines.")
            
        job = random.choice(under_allocated_jobs)
        
        # how many machines are still needed for this job?  
        job_machines_still_needed = job["machine_count"] - len(job["machines"])
        if job_machines_still_needed == 0:
            continue    
        
        # let's allocate some more machine to this job.
        allocation_chunk_min = max(1, job_machines_still_needed // fragmentation_factor)
        allocation_chunk_max = job_machines_still_needed
        allocation_chunk = random.randint(allocation_chunk_min, allocation_chunk_max)
        
        # find the first availabel chunk of machines
        found_chunk = False   
        range_end = machine_count - allocation_chunk + 1
        range_start = random.randint(0, range_end - 1)   
        
        
        for i in range(range_start, range_end): 
            if all(available_machines[i:i + allocation_chunk]):
                found_chunk = True
                job["machines"].extend(range(i, i + allocation_chunk))
                allocated_machines += allocation_chunk  
                for j in range(i, i + allocation_chunk):
                    available_machines[j] = False
                break
            
        if not found_chunk:
            # allocate one machine for this job. 
            for i in range(machine_count):   
                if available_machines[i]:
                    job["machines"].append(i)
                    allocated_machines += 1 
                    available_machines[i] = False
                    break

    return jobs, cmmcmp_ratio


def generate_simulated_placement_file(options, run_context, placement_strategy):
    
    print("simulating to get the placement info ...")
    
    placement_info = get_job_placement_info(
            strategy=placement_strategy,
            ring_mode=run_context["ring-mode"], 
            rack_size=options["ft-server-per-rack"],
            total_machine_count=options["machine-count"],
            sim_length=10000)  
    
    job_machine_counts = [job["machine_count"] for job in placement_info]
    
    jobs, _, cmmcmp_ratio = generate_job_basics(options, run_context, job_machine_counts)                        
    
    # placement_info is a list of dictionaries, each containing the placement information for a job.
    for job, job_placement_info in zip(jobs, placement_info):
        job["machines"] = job_placement_info["machines"]    
                
    return jobs, cmmcmp_ratio


def generate_manual_1_placement_file(options, run_context):   
    assert options["machine-count"] == 16, "Error: machine count does not match."        
    
    jobs = [
        {
            "job_id": 1,
            "machine_count": 4,
            "comm_size": 10000,
            "comp_size": 175,
            "layer_count": 1,
            "machines": [14, 15, 0, 1],
            "iter_count": 1 
        },
        {
            "job_id": 2,
            "machine_count": 4,
            "comm_size": 10000,
            "comp_size": 275,
            "layer_count": 1,
            "machines": [2, 3, 4, 5],
            "iter_count": 1 
        },
        {
            "job_id": 3,
            "machine_count": 4,
            "comm_size": 10000,
            "comp_size": 75,
            "layer_count": 1,
            "machines": [6, 7, 8, 9],
            "iter_count": 1 
        },
        {
            "job_id": 4,
            "machine_count": 4,
            "comm_size": 10000,
            "comp_size": 375,
            "layer_count": 1,
            "machines": [10, 11, 12, 13],
            "iter_count": 1 
        }
    ]
    
    return jobs, 1.0


def generate_manual_2_placement_file(options, run_context):   
    assert options["machine-count"] == 32, "Error: machine count does not match."        
    # 
    jobs = [
        {
            "job_id": 1,
            "machine_count": 8,
            "comm_size": 10000,
            "comp_size": 275,
            "layer_count": 1,
            "machines": [4, 11, 5, 10, 6, 9, 7, 8],
            "iter_count": 1 
        },
        {
            "job_id": 2,
            "machine_count": 8,
            "comm_size": 10000,
            "comp_size": 375,
            "layer_count": 1,
            "machines": [12, 19, 13, 18, 14, 17, 15, 16],
            "iter_count": 1 
        },
        {
            "job_id": 3,
            "machine_count": 8,
            "comm_size": 10000,
            "comp_size": 275,
            "layer_count": 1,
            "machines": [20, 21, 27, 26, 22, 23, 24, 25],
            "iter_count": 1 
        },
        {
            "job_id": 4,
            "machine_count": 4,
            "comm_size": 10000,
            "comp_size": 475,
            "layer_count": 1,
            "machines": [0, 1, 31, 30, 2, 3, 28, 29],
            "iter_count": 1 
        }
    ]
    
    return jobs, 1.0



def generate_manual_3_placement_file(options, run_context):   
    assert options["machine-count"] == 8, "Error: machine count does not match."        
    # 
    job1_machines = [1, 5, 3, 7] 
    job2_machines = [2, 6, 4, 0]
    
    # select a random number between 0 and 4
    r = random.randint(0, 2) 
    
    if r == 0:
        job1_machines = [1, 5, 3, 7] # 2  
        job2_machines = [2, 6, 0, 4] # 2   
        
    elif r == 1:
        job1_machines = [1, 5, 3, 7] # 2
        job2_machines = [2, 6, 4, 0] # 1
        
    elif r == 2:
        job1_machines = [1, 5, 7, 3] # 1
        job2_machines = [2, 4, 6, 0] # 1
    
    jobs = [
        {
            "job_id": 1,
            "machine_count": 4,
            "comm_size": 10000,
            "comp_size": 75,
            "layer_count": 1,
            "machines": job1_machines,
            "iter_count": 1 
        },
        {
            "job_id": 2,
            "machine_count": 4,
            "comm_size": 10000,
            "comp_size": 125,
            "layer_count": 1,
            "machines": job2_machines,     
            "iter_count": 1 
        }
        # maybe a third job as well later on? 
    ]
    
    return jobs, 1.0



def generate_manual_4_placement_file(options, run_context):   
    assert options["machine-count"] == 12, "Error: machine count does not match."        
    
    r = random.randint(0, 2)
    
    if r == 0: 
        job2_machines = [0, 6, 1, 7, 2, 8]   # load: 3
        job1_machines = [3, 4, 5, 9, 10, 11] # load: 1
    if r == 1:
        job2_machines = [0, 6, 1, 7, 2, 8]   # load: 3
        job1_machines = [3, 9, 5, 4, 10, 11] # load: 2
    if r == 2:
        job2_machines = [0, 6, 1, 7, 2, 8]   # load: 3
        job1_machines = [3, 9, 4, 10, 5, 11] # load: 3 
    
    jobs = [
        {
            "job_id": 1,
            "machine_count": 6,
            "comm_size": random.randint(10, 30) * 1000,
            "comp_size": random.randint(10, 15) * 25,
            # "comm_size": 30000,
            # "comp_size": 225,
            "layer_count": 1,
            "machines": job1_machines,
            "iter_count": 1 
        },
        {
            "job_id": 2,
            "machine_count": 6,
            "comm_size": random.randint(10, 30) * 1000,
            "comp_size": random.randint(10, 15) * 25,
            # "comm_size": 10000,
            # "comp_size": 175,
            "layer_count": 1,
            "machines": job2_machines,     
            "iter_count": 1 
        }
    ]
    
    return jobs, 1.0

def profile_all_jobs(jobs, options, run_context, config_sweeper, placement_path, stretch_factor=1):
    for job in jobs:
        if "profiled-throttle-factors" not in run_context:  
            rage_quit("Error: profiled-throttle-factors not in run_context.")
        
        job["period"] = {}

        for throttle_factor in run_context["profiled-throttle-factors"]:
            profiling_job_options = copy.deepcopy(options)  
            profiling_job_options["isolate-job-id"] = job["job_id"]
            profiling_job_options["print-flow-progress-history"] = True
            profiling_job_options["timing-file"] = None  
            profiling_job_options["ft-core-count"] = 1  
            profiling_job_options["ft-agg-core-link-capacity-mult"] = run_context["profiling-core-count"]
            profiling_job_options["lb-scheme"] = "random"   
            profiling_job_options["worker-id"] = run_context["worker-id-for-profiling"]
            profiling_job_options["stretch-factor"] = stretch_factor 
            profiling_job_options["placement-file"] = placement_path
            profiling_job_options["throttle_factor"] = throttle_factor
            profiling_job_options["subflows"] = 1 
            
            output = config_sweeper.only_run_command_with_options(run_context, profiling_job_options)
            
            run_path = "{}/worker-{}/run-1".format(config_sweeper.workers_dir,
                                                   run_context["worker-id-for-profiling"])
            
            flow_info_path = "{}/flow-info.txt".format(run_path)    
            results_path = "{}/results.txt".format(run_path)    
            
            job_prof, _, _ = get_job_profiles(flow_info_path)
            psim_finish_time = get_simulation_finish_time(results_path)
            
            # job_prof might be empty.
            job_id = job["job_id"]
            this_job_prof = None 
            
            if job_id in job_prof:
                assert job_prof[job_id]["period"] == psim_finish_time, "periods do not match" 
                this_job_prof = job_prof[job_id] 
            else: 
                this_job_prof = {
                    "period": psim_finish_time,
                    "flows": []
                }
                
            profile_file_path = f"{run_context['profiles-dir']}/{job_id}_{throttle_factor}.pkl" 
            with open(profile_file_path, "wb") as f:
                pkl.dump(this_job_prof, f)    
            
            # profile_file_path_json = f"{run_context['profiles-dir']}/{job_id}_{throttle_factor}.json"   
            # with open(profile_file_path_json, "w") as f:    
            #     json.dump(this_job_prof, f, indent=4) 
                
            job["period"][str(throttle_factor)] = psim_finish_time
            if throttle_factor == 1.0:  
                job["base_period"] = psim_finish_time
                
            print("profiled job: ", job_id, " with throttle factor: ", throttle_factor, " period: ", psim_finish_time)
            
            
def handle_rings(jobs, placement_mode, ring_mode): 
    if placement_mode.startswith("manual"):
       return 
    
    if ring_mode == "random":
        for job in jobs:
            random.shuffle(job["machines"]) 

    elif ring_mode == "sorted": 
        for job in jobs:
            job["machines"] = sorted(job["machines"])  

    elif ring_mode == "letitbe":
        pass    

    else: 
        raise Exception("Error: unknown ring mode: " + ring_mode)      
    
def set_iter_counts(jobs, run_context):
    # at this point, all the profiles are ready and each job has a period associated with it.   
    for job in jobs: 
        period = job["base_period"] 
        sim_length = run_context["sim-length"]  
        iter_count = math.floor(sim_length / period)

        if iter_count < 1:
            iter_count = 1   

        job["iter_count"] = iter_count
        
                                         
def generate_placement_file(placement_path, placement_seed,   
                            options, run_context, config_sweeper):  
    
    placement_magic = 45 
    ring_magic = 67
    
    # the seed will be set at this point everything from here on will be deterministic.
    # for the same experiment seed and placement seed. 
    if "placement-parameters" in run_context:
        placement_parameters = run_context["placement-parameters"]
        if "placement-seed-limit" in placement_parameters:
            seed_limit = placement_parameters["placement-seed-limit"]
            placement_seed = placement_seed % seed_limit
            
    random.seed(run_context["experiment-seed"] + placement_seed + placement_magic)
    
    placement_mode = run_context["placement-mode"] 
    
    if placement_mode == "compact": 
        jobs, cmmcmp_ratio = generate_compact_placement_file(options, run_context)

    elif placement_mode == "random":
        jobs, cmmcmp_ratio = generate_random_placement_file(options, run_context)
    
    elif placement_mode == "entropy":
        jobs, cmmcmp_ratio = generate_entropy_placement_file(options, run_context)

    elif placement_mode.startswith("semirandom"):   
        if "_" in placement_mode:
            placement_mode, fragmentation_factor = placement_mode.split("_")
            fragmentation_factor = int(fragmentation_factor)
        else:
            fragmentation_factor = 1   
        jobs, cmmcmp_ratio = generate_semirandom_placement_file(options, run_context, fragmentation_factor) 
        
    elif placement_mode.startswith("sim"):
        if "_" in placement_mode:
            placement_strategy = placement_mode.split("_")[1]
        else:
            placement_strategy = "firstfit"
              
        jobs, cmmcmp_ratio = generate_simulated_placement_file(options, run_context, placement_strategy)
    
    elif placement_mode == "manual_1":
        jobs, cmmcmp_ratio = generate_manual_1_placement_file(options, run_context) 
    elif placement_mode == "manual_2":
        jobs, cmmcmp_ratio = generate_manual_2_placement_file(options, run_context)
    elif placement_mode == "manual_3":
        jobs, cmmcmp_ratio = generate_manual_3_placement_file(options, run_context)
    elif placement_mode == "manual_4":
        jobs, cmmcmp_ratio = generate_manual_4_placement_file(options, run_context)
    else: 
        rage_quit("Error: unknown placement mode: " + placement_mode)
        
    random.seed(run_context["experiment-seed"] + placement_seed + ring_magic)
        
    handle_rings(jobs, placement_mode, run_context["ring-mode"] )
    
    with open(placement_path, "w") as f:
        json.dump(jobs, f, indent=4)
        f.flush()

    # we want to do the profiling here. 
    profile_all_jobs(jobs, options, run_context, config_sweeper, placement_path) 
    set_iter_counts(jobs, run_context)
    
    # now we save the jobs with the iter count.       
    with open(placement_path, "w") as f:
        json.dump(jobs, f, indent=4)
        f.flush()
    
    add_to_context = {  
        "cmmcmp-ratio": cmmcmp_ratio,
    }
    return jobs, add_to_context



if __name__ == "__main__":
    options = {
        'machine-count': 96,
        'ft-server-per-rack': 8,
    }        
    
    run_context = {
        'experiment-seed': 76,
        'placement-mode': 'compact',
        'placement-parameters': {'placement-seed-limit': 100},
        'placement-seed': 1,
        'ring-mode': 'random',
        'selected-setting': {'comm-size': [1600,1800,2000,2200,2400,2600,2800,3000,3200,3400,
                                           3600,3800,4000,4200,4400,4600,4800,5000,5200,5400,5600,
                                            5800, 6000,6200,6400,6600,6800,7000,7200,7400,7600,7800,
                                            8000,8200,8400,8600,8800,9000,9200,9400,9600, 9800],
                            'comp-size': [50,60,70,80,90,100,110,120,130,140,150,160,170,180,190,
                                          200,210,220,230,240,250,260,270,280,290,300,310,320,
                                            330,340,350,360,370,380,390,400,410,420,430,440],
                            'ft-server-per-rack': 8,
                            'iter-count': [30],
                            'jobs-machine-count-high': 12,
                            'jobs-machine-count-low': 12,
                            'layer-count': [1],
        },
        'sim-length': 4000,}
    
    
    
    for desired_entropy in [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]:
        jobs = generate_compact_placement_file(options, run_context)
        
        entropies = [] 
        
        for i in range(1000): 
            entropy = measure_entrorpy(jobs, options["ft-server-per-rack"])
            perturb_placement(jobs)    
            entropies.append(entropy)
            
            if entropy > desired_entropy:
                break
            
        print("desired_entropy: ", desired_entropy, " rounds: ", i)
            
    # plot the entropies.
    # import matplotlib.pyplot as plt
    
    # plt.plot(entropies)
    # plt.savefig("entropies.png")    
        
