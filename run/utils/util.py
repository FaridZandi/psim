import random 
import os 
import resource 
import sys 
import numpy as np

def make_shuffle(count, path):
    dir = os.path.dirname(path)
    if not os.path.exists(dir):
        os.makedirs(dir)
        
    
    numbers = list(range(count))
    random.shuffle(numbers)
    with open(path, "w") as f:
        for n in numbers:
            f.write("{}\n".format(n))


def get_incremented_number(filename='number.txt'):
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


def parse_arguments(argv):
    params = {}
    use_gdb = False
    
    for i, arg in enumerate(argv):
        if i == 0:
            continue
        p = arg.split("=")
        key = p[0][2:]
        if len(p) == 1:
            val = True
        else:
            val = p[1]
            if val == "true":
                val = True
            if val == "false":
                val = False
        params[key] = val
        
    if "gdb" in params:
        use_gdb = True
        del params["gdb"]
        
    return params, use_gdb

def set_memory_limit(limit):
    memory_limit_kb = int(limit)
    resource.setrlimit(resource.RLIMIT_AS, (memory_limit_kb, memory_limit_kb))
    
    
def make_cmd(executable, options, use_gdb=False, print_cmd=False):
    cmd = executable
    
    for option in options.items():
        if option[1] is False:
            continue
        elif option[1] is True:
            cmd += " --" + option[0]
        else: 
            cmd += " --" + option[0] + "=" + str(option[1])

    if use_gdb:
        cmd = "gdb -ex run --args " + cmd

    if print_cmd:
        print(cmd)
        
    return cmd


def build_exec(executable, base_executable, build_path, run_path):
    # build the executable, exit if build fails
    os.chdir(build_path)
    # run the make -j command, get the exit code
    exit_code = os.system("make -j")
    
    if exit_code != 0:
        print("make failed, exiting")
        sys.exit(1)
        
    os.chdir(run_path)
    os.system("cp {} {}".format(base_executable, executable))
    
    
def get_base_dir():
    base_dir = os.environ.get("PSIM_BASE_DIR")

    if base_dir is None:
        print("export PSIM_BASE_DIR to the base directory of the project, exiting")
        sys.exit(1)

    print("base_dir:", base_dir)
    return base_dir


   
def get_psim_time(job_output, print_output=False): 
    psim_times = []
    output_lines = [] 
    for line in iter(job_output.readline, b''):
        output = line.decode("utf-8")
        if "psim time:" in output:
            psim_time = float(output.split("psim time:")[1])
            psim_times.append(psim_time)
            
        if print_output:
            print(output, end="")
            sys.stdout.flush()
            sys.stderr.flush()
        else :
            output_lines.append(output)
    
    if len(psim_times) == 0:
        print("no psim times found, probably a crash happened")
        print("no point in continuing, exiting")
        for line in output_lines:
            print(line, end="")
        sys.exit(1)
        
    else:     
        result = {
            "all": psim_times,
            "avg": np.mean(psim_times), 
            "max": np.max(psim_times),
            "min": np.min(psim_times),
            "median": np.median(psim_times), 
            "last": psim_times[-1]
        } 
    
    return result