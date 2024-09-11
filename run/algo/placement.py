import random 
import json 
import time 



def generate_compact_or_random_placement_file(options, run_context, selected_setting):

    machine_count = options["machine-count"]
    placement_mode = options["placement-mode"]
    ring_mode = options["ring-mode"]    
    
    jobs_machine_count_high = run_context["placement-parameters"]["jobs-machine-count-high"]
    jobs_machine_count_low = run_context["placement-parameters"]["jobs-machine-count-low"]
    
    jobs = [] 
    
    machines_left = machine_count 
    current_job_id = 1 
    
    
    # random_mode = "either_or"
    random_mode = "range"

    # assigning 1 machine to a job would be bad, beceasse there would be no communication. 
    # so if just one machine is left, then we skip it.    
    while machines_left > 1:
        if random_mode == "either_or":
            this_job_machine_count = random.choice([jobs_machine_count_low, jobs_machine_count_high])  
        elif random_mode == "range":
            this_job_machine_count = random.randint(jobs_machine_count_low, jobs_machine_count_high)
            
        if this_job_machine_count > machines_left:
            this_job_machine_count = machines_left    
            
        job_communication_size = random.choice(selected_setting["comm-size"])   
        job_computation_size = random.choice(selected_setting["comp-size"])    
        job_layer_count = random.choice(selected_setting["layer-count"])  
        job_iter_count = random.choice(selected_setting["iter-count"])
        
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
        current_job_id += 1
        
    all_machines = list(range(0, machine_count))
    
    if placement_mode == "random":
        for job in jobs:
            job["machines"] = random.sample(all_machines, job["machine_count"])
            for machine in job["machines"]:
                all_machines.remove(machine)
    elif placement_mode == "compact":   
        for job in jobs:
            job["machines"] = all_machines[:job["machine_count"]]
            all_machines = all_machines[job["machine_count"]:]
                
    if ring_mode == "random":
        for job in jobs:
            random.shuffle(job["machines"]) 
    else:
        for job in jobs:
            job["machines"] = sorted(job["machines"])  
                               
                               
    return jobs               
            
            
            

def generate_semirandom_placement_file(options, run_context, selected_setting):

    machine_count = options["machine-count"]
    placement_mode = options["placement-mode"]
    ring_mode = options["ring-mode"]    
    
    jobs_machine_count_high = run_context["placement-parameters"]["jobs-machine-count-high"]
    jobs_machine_count_low = run_context["placement-parameters"]["jobs-machine-count-low"]
    
    jobs = [] 
    
    machines_left = machine_count 
    current_job_id = 1 
    
    # random_mode = "either_or"
    random_mode = "range"

    # assigning 1 machine to a job would be bad, beceasse there would be no communication. 
    # so if just one machine is left, then we skip it.    
    while machines_left > 1:
        if random_mode == "either_or":
            this_job_machine_count = random.choice([jobs_machine_count_low, jobs_machine_count_high])  
        elif random_mode == "range":
            this_job_machine_count = random.randint(jobs_machine_count_low, jobs_machine_count_high)
            
        if this_job_machine_count > machines_left:
            this_job_machine_count = machines_left        
            
        job_communication_size = random.choice(selected_setting["comm-size"])   
        job_computation_size = random.choice(selected_setting["comp-size"])    
        job_layer_count = random.choice(selected_setting["layer-count"])  
        job_iter_count = random.choice(selected_setting["iter-count"])
        
        jobs.append({
            "job_id": current_job_id,   
            "comm_size": job_communication_size,
            "comp_size": job_computation_size,
            "layer_count": job_layer_count,
            "iter_count": job_iter_count,

            "machine_count": this_job_machine_count, 
            "machines": []
        })
        
        machines_left -= this_job_machine_count     
        current_job_id += 1
        
    assigned_machines = machine_count - machines_left
    allocated_machines = 0     
    available_machines = [True] * machine_count
    
    
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
        allocation_chunk = random.randint(max(1, job_machines_still_needed // 2), job_machines_still_needed)
        
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

    if ring_mode == "random":
        for job in jobs:
            random.shuffle(job["machines"]) 
    else:
        for job in jobs:
            job["machines"] = sorted(job["machines"])  
                               
                               
    return jobs    
                                  
                                              
                                              
def generate_placement_file(placement_path, placement_seed,   
                            options, run_context, selected_setting):
    
    # the seed will be set at this point everything from here on will be deterministic.
    # for the same experiment seed and placement seed. 
    random.seed(run_context["experiment-seed"] + placement_seed)
    
    placement_mode = options["placement-mode"] 
    
    if placement_mode == "compact" or placement_mode == "random":   
        jobs = generate_compact_or_random_placement_file(options, run_context, selected_setting)
    elif placement_mode == "semirandom": 
        jobs = generate_semirandom_placement_file(options, run_context, selected_setting) 
    
    
    with open(placement_path, "w") as f:
        json.dump(jobs, f, indent=4)
        f.flush()

    return jobs
    
