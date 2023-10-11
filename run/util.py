import random 
import os 

def make_shuffle(count, path):
    dir = os.path.dirname(path)
    if not os.path.exists(dir):
        os.makedirs(dir)
        
    
    numbers = list(range(count))
    random.shuffle(numbers)
    with open(path, "w") as f:
        for n in numbers:
            f.write("{}\n".format(n))
             
    