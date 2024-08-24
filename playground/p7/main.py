import random 
from pprint import pprint
from queue import PriorityQueue

random_seed = 435
sim_finish_time = 100

# FAIRNESS_MODEL = "fairness"
FAIRNESS_MODEL = "queue"

# QUEUE_METHOD = "global-queue"
QUEUE_METHOD = "per-processor-queue"

INIT_STATE = 0
WAITING_STATE = 1
RUNNING_STATE = 2
PAUSE_STATE = 3

class Event: 
    def __init__(self, ready_time, data):   
        self.data = data 
        self.ready_time = ready_time
        
    def __lt__(self, other):
        return self.ready_time < other.ready_time


class Simulator: 
    def __init__(self, processor_count, workloads):
        self.processor_count = processor_count
        self.workloads = workloads
        
        self.timer = 0 # current time of the simulation
        self.evq = PriorityQueue() # the main event queue
        
        self.waiting_workloads = [] 
        self.processors_state = [0] * processor_count
        
        self.processor_history = [[] for _ in range(processor_count)] # the times when the processors were busy
        self.workload_history = [[] for _ in range(len(workloads))] # the times when the workloads were running
        
        
    def initiate_stuff(self):
        for i, w in enumerate(self.workloads):
            event = Event(ready_time=0, 
                          data={"type": "arrive", 
                                "workload": i, 
                                "instance": 0})
            w["current_instance"] = 0
            w["current_state"] = INIT_STATE
            self.evq.put(event)
            
    
    def find_empty_processor(self):
        for i, p in enumerate(self.processors_state):
            if p == False:
                return i
        return -1
    
    def start_a_waiting_workload(self, processor): 
        waiting_w_i, instance = self.waiting_workloads.pop(0) 
        
        finish_time = self.timer + self.workloads[waiting_w_i]["duration"]
        self.workloads[waiting_w_i]["current_state"] = RUNNING_STATE
        
        self.processors_state[processor] = True
        self.processor_history[processor].append(event.data) 
         
        event = Event(ready_time=finish_time,
                      data={"type": "finish", 
                            "workload": waiting_w_i, 
                            "processor": processor, 
                            "instance": instance, 
                            "started_at": self.timer})

        self.evq.put(event)
        
        
    def simulate(self, sim_finish_time): 
        while True: 
            # get the next event. everything always starts with getting an event from the event queue
            event = self.evq.get()    
            self.timer = event.ready_time
            
            if event.data["type"] == "arrive":
                w_i = event.data["workload"]
                print("time: ", self.timer, "arrived workloald:", w_i, "instance:", event.data["instance"])

                processor = self.find_empty_processor()
                                    
                if processor == -1:
                    # there are no empty processors, so we need to wait for one to be available
                    self.workloads[w_i]["current_state"] = WAITING_STATE
                    self.waiting_workloads.append((w_i, event.data["instance"] + 1))
                    
                else:   
                    # start the workload
                    self.processors_state[processor] = True
                    
                    self.workloads[w_i]["current_state"] = RUNNING_STATE
                    
                    finish_time = self.timer + self.workloads[w_i]["duration"] 
                    
                    event = Event(ready_time=finish_time, 
                                  data={"type": "finish", 
                                        "workload": w_i, 
                                        "processor": processor, 
                                        "instance": event.data["instance"], 
                                        "started_at": self.timer})    
                
                    self.evq.put(event)
            
                    
            elif event.data["type"] == "finish":
                w_i = event.data["workload"]
                processor = event.data["processor"]
                
                print("time: ", self.timer, "finish workload:", w_i, "instance:", event.data["instance"])  

                # add the next start event to the event queue
                next_arrival_event = Event(ready_time=self.timer + self.workloads[w_i]["pause"], 
                             data={"type": "arrive", 
                                   "workload": w_i, 
                                   "instance": event.data["instance"] + 1})    
                
                self.workloads[w_i]["current_state"] = PAUSE_STATE  
                self.evq.put(next_arrival_event)                                


                # release the processor
                self.processors_state[processor] = False
                self.processor_history[processor].append(event.data)
                
                # check if there are any waiting workloads
                if len(self.waiting_workloads) > 0:
                    self.start_a_waiting_workload(processor)
                    
            
            if self.timer > sim_finish_time:
                break
    
    def print_stats(self):
        for i, p in enumerate(self.processor_history):
            print("processor", i)
            pprint(p)
        

def main():
    random.seed(random_seed)
    
    processor_count = random.randint(1, 10)
    workload_count = random.randint(2, 2)

    print("processor_count", processor_count)   
    print("workload_count", workload_count) 

    workloads = [] 

    for i in range(workload_count):
        w = {
            "duration": random.randint(1, 10),
            "pause": random.randint(1, 10),
            "current_state": 0, 
            "current_instance": 0, 
            "history": []
        }
        
        w["util"] = w["duration"] / (w["duration"] + w["pause"])
        
        workloads.append(w)
        
    pprint(workloads)

    simulator = Simulator(processor_count, workloads)
    simulator.initiate_stuff()
    simulator.simulate(sim_finish_time)
    
    simulator.print_stats()    


if __name__ == "__main__":
    main()
    
    