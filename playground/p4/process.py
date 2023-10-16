# plot histogram. Read all the numbers from the file and plot the histogram 

import matplotlib.pyplot as plt
import numpy as np
import sys
# read the file

file_path = sys.argv[1]

f = open(file_path, "r")
numbers = []
for line in f:
    
    number = float(line)
    if number < 1000:
        numbers.append(number)
f.close()

# plot the histogram
plt.hist(numbers, bins=100)
plt.savefig("histogram.png")

