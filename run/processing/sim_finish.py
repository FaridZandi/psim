import re 

def parse_line(line):
    # [14:54:34.607] [critical] psim time: xxxx 
    
    pattern = re.compile(r'\[.*\] \[.*\] psim time: (\d+)')
    match = pattern.match(line)
    
    if match is not None:   
        psim_time = int(match.group(1)) 
        return psim_time
    
    return None 

def get_simulation_finish_time(file_path):
    
    with open(file_path, 'r') as file:
        for line in file:
            
            sim_finish = parse_line(line)
            
            if sim_finish is not None:
                return sim_finish   

    return -1