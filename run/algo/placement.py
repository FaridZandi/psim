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

def generate_job_basics(options, run_context, job_machine_counts=None):
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
        
    return jobs, assigned_machines
        


def generate_random_placement_file(options, run_context):
    machine_count = options["machine-count"]
    all_machines = list(range(0, machine_count))
    
    jobs, _ = generate_job_basics(options, run_context)  
    
    for job in jobs:
        job["machines"] = random.sample(all_machines, job["machine_count"])
        for machine in job["machines"]:
            all_machines.remove(machine)
                               
    return jobs               


def generate_compact_placement_file(options, run_context):
    machine_count = options["machine-count"]
    all_machines = list(range(0, machine_count))
    
    jobs, _ = generate_job_basics(options, run_context)  
    
    for job in jobs:
        job["machines"] = all_machines[:job["machine_count"]]
        all_machines = all_machines[job["machine_count"]:]
                               
    return jobs
            
            

def generate_semirandom_placement_file(options, run_context, fragmentation_factor):
    machine_count = options["machine-count"]
    allocated_machines = 0     
    available_machines = [True] * machine_count
    
    jobs, assigned_machines = generate_job_basics(options, run_context)  
    
    while allocated_machines < assigned_machines: 
        # take a random job that is under allocated.    
        under_allocated_jobs = [job for job in jobs if len(job["machines"]) < job["machine_count"]] 
        
        if len(under_allocated_jobs) == 0:
            print("Error: No under allocated jobs left, but the number of allocated machines is less than the number of assigned machines.")
            exit(0)
            
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
        for i in range(machine_count - allocation_chunk + 1):
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

                               
    return jobs    
                                  
                
def generate_simulated_placement_file(options, run_context, placement_strategy):
    
    print("simulating to get the placement info ...")
    
    sim_length = run_context["sim-length"]
    
    placement_info = get_job_placement_info(
            strategy=placement_strategy,
            ring_mode=run_context["ring-mode"], 
            rack_size=options["ft-server-per-rack"],
            total_machine_count=options["machine-count"],
            sim_length=sim_length)  
    
    job_machine_counts = [job["machine_count"] for job in placement_info]
    
    jobs, _ = generate_job_basics(options, run_context, job_machine_counts)                        
    
    
    # placement_info is a list of dictionaries, each containing the placement information for a job.
    
    for job, job_placement_info in zip(jobs, placement_info):
        job["machines"] = job_placement_info["machines"]    
                
    return jobs



def profile_all_jobs(jobs, options, run_context, config_sweeper, placement_path, stretch_factor=1):
    for job in jobs:
        profiling_job_options = copy.deepcopy(options)  
        profiling_job_options["isolate-job-id"] = job["job_id"]
        profiling_job_options["print-flow-progress-history"] = True
        profiling_job_options["timing-file"] = None  
        profiling_job_options["ft-core-count"] = 1  
        profiling_job_options["ft-agg-core-link-capacity-mult"] = 100
        profiling_job_options["lb-scheme"] = "random"   
        profiling_job_options["worker-id"] = run_context["worker-id-for-profiling"]
        profiling_job_options["stretch-factor"] = stretch_factor 
        profiling_job_options["placement-file"] = placement_path
        
        output = config_sweeper.only_run_command_with_options(run_context, profiling_job_options)
        
        run_path = "{}/worker-{}/run-1".format(config_sweeper.workers_dir,
                                               run_context["worker-id-for-profiling"])
        
        flow_info_path = "{}/flow-info.txt".format(run_path)    
        results_path = "{}/results.txt".format(run_path)    
        
        job_prof, _, _ = get_job_profiles(flow_info_path)
        psim_finish_time = get_simulation_finish_time(results_path)
        
        # job_prof might be empty.
        job_id = job["job_id"]
        if job_id in job_prof:
            
            assert job_prof[job_id]["period"] == psim_finish_time, "periods do not match" 
            print("job_prof period: ", job_prof[job_id]["period"])
            
            # job_profiles[job_id] = job_prof[job_id]
            profile_file_path = f"{run_context['profiles-dir']}/{job_id}.pkl" 
            with open(profile_file_path, "wb") as f:
                pkl.dump(job_prof[job_id], f)    
            
            job["period"] = job_prof[job_id]["period"]
        else:
            print("no profiling but psim_finish_time: ", psim_finish_time)   
            job["period"] = psim_finish_time    
                                              
def generate_placement_file(placement_path, placement_seed,   
                            options, run_context, config_sweeper):  
    
    placement_magic = 45 
    ring_magic = 67
    
    # the seed will be set at this point everything from here on will be deterministic.
    # for the same experiment seed and placement seed. 
    random.seed(run_context["experiment-seed"] + placement_seed + placement_magic)
    
    placement_mode = run_context["placement-mode"] 
    
    if placement_mode == "compact": 
        jobs = generate_compact_placement_file(options, run_context)

    elif placement_mode == "random":
        jobs = generate_random_placement_file(options, run_context)

    elif placement_mode.startswith("semirandom"):   
        if "_" in placement_mode:
            placement_mode, fragmentation_factor = placement_mode.split("_")
            fragmentation_factor = int(fragmentation_factor)
        else:
            fragmentation_factor = 1   
        jobs = generate_semirandom_placement_file(options, run_context, fragmentation_factor) 
        
    elif placement_mode.startswith("sim"):
        if "_" in placement_mode:
            placement_strategy = placement_mode.split("_")[1]
        else:
            placement_strategy = "firstfit"
              
        jobs = generate_simulated_placement_file(options, run_context, placement_strategy)
        
    random.seed(run_context["experiment-seed"] + placement_seed + ring_magic)
        
    ring_mode = run_context["ring-mode"]    
    if ring_mode == "random":
        for job in jobs:
            random.shuffle(job["machines"]) 
    else:
        for job in jobs:
            job["machines"] = sorted(job["machines"])  
    
    with open(placement_path, "w") as f:
        json.dump(jobs, f, indent=4)
        f.flush()

    # we want to do the profiling here. 
    profile_all_jobs(jobs, options, run_context, config_sweeper, placement_path) 
    
    # at this point, all the profiles are ready and each job has a period associated with it.   
    for job in jobs: 
        period = job["period"] 
        sim_length = run_context["sim-length"]  
        iter_count = math.floor(sim_length / period) - 1
        if iter_count < 1:
            iter_count = 1   
        job["iter_count"] = iter_count  
        print(f"job_id: {job['job_id']}, period: {period}, iter_count: {iter_count}")   
       
    # now we save the jobs with the iter count.       
    with open(placement_path, "w") as f:
        json.dump(jobs, f, indent=4)
        f.flush()
    
    return jobs
    
