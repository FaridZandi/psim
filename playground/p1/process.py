import os
import matplotlib.pyplot as plt

x = [] 
y = [] 

print('loading data...')
with open('vgg16.txt', 'r') as f:
    for line in f:
        s = line.strip().split('|')
        comm = float(s[3].strip().split()[2])
        mem = float(s[4].strip().split()[2])
        
        x.append(comm)
        y.append(mem)
        
print('processing data...')
x, y = zip(*sorted(zip(x, y)))
print("number of all points: ", len(x))

# keep unique pairs 

unique_x = []
unique_y = []

for i in range(len(x)):
    if (i == 0 or x[i] != x[i-1] or y[i] != y[i-1]):
        unique_x.append(x[i])
        unique_y.append(y[i])
        
x = unique_x
y = unique_y

print("number of unique points: ", len(x))
print('finding pareto front...')

pareto_x = []
pareto_y = []
flags = [True] * len(x)

for i in range(len(x)):
    if (flags[i]):
        for j in range(len(x)):
            if (x[i] < x[j] and y[i] < y[j]):
                flags[j] = False
                
for i in range(len(x)):
    if (flags[i]):
        pareto_x.append(x[i])
        pareto_y.append(y[i])
        
        
# sort the pareto front
pareto_x, pareto_y = zip(*sorted(zip(pareto_x, pareto_y)))
    
    
    
print('plotting...')
plt.plot(x, y, 'ro', markersize=0.1)
plt.plot(pareto_x, pareto_y, linestyle='-', marker='o', markersize=0.1)
plt.xlabel('Communication (GB)')
plt.ylabel('Memory (GB)')
plt.savefig('b4.png', dpi=300, bbox_inches='tight')
