import random 
import os 
import resource 
import sys 
import numpy as np
import traceback 


def flatten_dict(d, parent_key='', levels=None, level_names=None):
    """
    Flattens a nested dictionary with detailed level information and custom level names.

    :param d: Dictionary to flatten
    :param parent_key: Key prefix for nested dictionaries (used in recursion)
    :param levels: List to track the hierarchy of levels (used in recursion)
    :param level_names: List of custom names for levels
    :return: A list of dictionaries with level details and values
    """
    if levels is None:
        levels = []

    flat_list = []

    for key, value in d.items():
        current_levels = levels + [key]
        if isinstance(value, dict):
            flat_list.extend(flatten_dict(value, key, current_levels, level_names))
        else:
            entry = {}
            for i, lvl in enumerate(current_levels):
                if level_names and i < len(level_names):
                    entry[level_names[i]] = lvl
                else:
                    entry[f"level{i+1}"] = lvl
            entry["value"] = value
            flat_list.append(entry)

    return flat_list

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


def build_exec(executable, base_executable, build_path, run_dir):
    # build the executable, exit if build fails
    os.chdir(build_path)
    
    # run the make -j command, get the exit code
    exit_code = os.system("make -j")
    
    if exit_code != 0:
        print("make failed, exiting")
        rage_quit("make failed, exiting")   
        
    os.chdir(run_dir)
    os.system("cp {} {}".format(base_executable, executable))
    
    
def get_base_dir():
    base_dir = os.environ.get("PSIM_BASE_DIR")

    if base_dir is None:
        rage_quit("export PSIM_BASE_DIR to the base directory of the project, exiting")

    print("base_dir:", base_dir)
    return base_dir


# [14:14:13.445] [critical] run number: 1
# [14:14:13.445] [critical] psim time: 8020
# [14:14:13.445] [critical] Total congested time: 1.375

def get_psim_time(output): 
    for line in output:
        if "psim time:" in line:
            psim_time = float(line.split("psim time:")[1])
            return psim_time        
        
    rage_quit("no psim times found, simulation probably failed")

def get_psim_total_congested_time(output):  
    for line in output:
        if "Total congested time:" in line:
            congested_time = float(line.split("Total congested time:")[1])
            return congested_time

    rage_quit("no congested times found, simulation probably failed")

def get_random_string(length):
    letters = "abcdefghijklmnopqrstuvwxyz"
    return ''.join(random.choice(letters) for i in range(length))

default_load_metric_map = {
    "futureload": "utilization",
    "leastloaded": "flowsize",
    "powerof2": "flowsize",
    "powerof3": "flowsize",
    "powerof4": "flowsize",
    "random": "utilization",
    "robinhood": "utilization",
    "roundrobin": "flowsize",
    "sita-e": "utilization",
    "ecmp": "flowsize",
    "zero": "flowsize", 
    "readprotocol": "flowsize", 
}

rounding_precision = 3

def rage_quit(msg, error_code=1):   
    print("-----------CRITICAL----------")
    print("CRITICAL ERROR: ", end="")
    print(msg)
    sys.stdout.flush()
    traceback.print_stack()
    os._exit(error_code)  