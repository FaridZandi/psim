from pprint import pprint
import matplotlib.pyplot as plt

home_path = '/home/faridzandi/git/psim/'

log_dir = home_path + "run/workers/worker-0/" 

logs = []

for i in range(3, 11):
    log_path = log_dir + "run-{}/errors.txt".format(i)
    logs.append(log_path)
    

for i, log in enumerate(logs):
    print("Processing {}".format(log))    
    errors1 = []
    errors2 = []
    
    with open(log, "r") as f:
        lines = f.readlines()
        for line in lines:
            error1 = float(line.split(" ")[-3])
            error2 = float(line.split(" ")[-1])
            
            if error1 > 2: 
                error1 = 2
            if error1 < 0:
                error1 = 0
                
            if error2 > 2:
                error2 = 2
            if error2 < 0:
                error2 = 0
            
            errors1.append(error1)
            errors2.append(error2)
            
    plt.hist(errors1, bins = 100)
    plt.title("Run {}".format(i + 2))
    plt.xlabel("Error")
    plt.ylabel("Count")    
    plt.savefig("run-{}-error1.png".format(i + 2))
    plt.clf()
    
    
    plt.hist(errors2, bins = 100)
    plt.title("Run {}".format(i + 2))
    plt.xlabel("Error")
    plt.ylabel("Count")    
    plt.savefig("run-{}-error2.png".format(i + 2))
    plt.clf()
        
