import random 
from pprint import pprint
from queue import PriorityQueue

random_seed = 435
random.seed(random_seed)


processor_count = random.randint(1, 10)
print("processor_count", processor_count)   


workloads = [] 

workload_count = random.randint(2, 2)
print("workload_count", workload_count) 

for i in range(workload_count):
    w = {
        "p": random.randint(1, 10),
        "d": random.randint(1, 10),
        "s": [], 
        "f": [],
        "c": [] 
    }
    
    w["util"] = w["p"] / (w["d"] + w["p"])
    
    workloads.append(w)
    
pprint(workloads)


class Event: 
    
    def __init__(self, ready_time, data):   
        self.data = data 
        self.ready_time = ready_time
        
    def __lt__(self, other):
        return self.ready_time < other.ready_time

def simulate_workloads(): 
    current_time = 0 
    processors_state = [False] * processor_count
    
    event_queue = PriorityQueue()
    
    # in the beginning, all workloads are ready to start.
    for i, w in enumerate(workloads):
        
        event = Event(ready_time=0, 
                      data={"type": "start", "workload": i, "instance": 0})
        
        event_queue.put(event)  
        
    waiting_workloads = [] 
    
    while True: 
        
        # get the next event
        event = event_queue.get()    
        current_time = event.ready_time
            
            
        if event.data["type"] == "start":
            w_i = event.data["workload"]
            
            print("time: ", current_time, "start workloald:", w_i, "instance:", event.data["instance"])
            
            finish_time = current_time + workloads[w_i]["p"]  
            
            # find a processor to run the workload 
            processor = -1  
            for i, p in enumerate(processors_state):
                if p == False:
                    processor = i
                    break
                
            if processor == -1:
                waiting_workloads.append((w_i, event.data["instance"] + 1))
                
            else:   
                processors_state[processor] = True
                                                  
                event = Event(ready_time=finish_time, 
                              data={"type": "finish", "workload": w_i, "processor": processor, "instance": event.data["instance"]})    
            
                event_queue.put(event)
        
                
        elif event.data["type"] == "finish":
            w_i = event.data["workload"]
            print("time: ", current_time, "finish workload:", w_i, "instance:", event.data["instance"])  
                        
            # release the processor
            processor = event.data["processor"]
            processors_state[processor] = False
            
            # check if there are any waiting workloads
            if len(waiting_workloads) > 0:
                waiting_w_i, instance = waiting_workloads.pop(0) 
                
                finish_time = current_time + workloads[waiting_w_i]["p"]
                
                processors_state[processor] = True
                
                event = Event(ready_time=finish_time,
                              data={"type": "finish", "workload": waiting_w_i, "processor": processor, "instance": instance})

                event_queue.put(event)
                
            
            # add the next start event to the event queue
            event = Event(ready_time=current_time + workloads[w_i]["d"], 
                          data={"type": "start", "workload": w_i, "instance": event.data["instance"] + 1})    
            
            event_queue.put(event)                                
        
        if current_time > 100:
            break
        
        
if __name__ == "__main__":
    simulate_workloads()