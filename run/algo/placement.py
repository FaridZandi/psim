import random 
import json 
import time 

def generate_placement_file(placement_path, placement_seed,   
                            options, run_context, selected_setting):
    
    # the seed will be set at this point everything from here on will be deterministic.
    # for the same experiment seed and placement seed. 
    random.seed(run_context["experiment-seed"] + placement_seed)
    
    machine_count = options["machine-count"]

    placement_mode = options["placement-mode"]
    ring_mode = options["ring-mode"]    
    
    jobs_machine_count_high = options["general-param-3"] 
    jobs_machine_count_low = options["general-param-1"]
    
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
    
    with open(placement_path, "w") as f:
        json.dump(jobs, f, indent=4)
        f.flush()

    return jobs
    
