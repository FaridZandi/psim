import matplotlib.pyplot as plt
import os
from pprint import pprint

base_path = "/home/faridzandi/git/psim/input/128search/"
all_files = [f for f in os.listdir(base_path) if f.endswith(".txt")]
print("all files:")
pprint(all_files)


modes = ["through-core", "through-rack", "through-machine"]
for mode in modes: 
    output_dir = "results-" + mode
    mkdir_cmd = "mkdir -p " + output_dir
    os.system(mkdir_cmd)
        
    for file_name in all_files:
        input_path = base_path + file_name
        exp_name = file_name.split(".")[0]
        output_path = output_dir + "/" + exp_name + ".png"
        
        sizes = [] 
        with open(input_path, "r") as f:
            for line in f.readlines():
                        
                if not line.startswith("Comm"):
                    continue
                
                size = float(line.split()[5]) / 8 
                src = int(line.split()[7])
                dst = int(line.split()[9])
                
                same_machine = (src // 8) == (dst // 8)
                same_rack = (src // 32) == (dst // 32)

                if mode == "through-machine":
                    if same_machine:
                        sizes.append(size)
                elif mode == "through-rack":
                    if same_rack and not same_machine:
                        sizes.append(size)
                elif mode == "through-core":
                    if not same_rack:
                        sizes.append(size)
                

        print("file:", file_name, 
            ", size:", len(sizes), 
            ", mean:", sum(sizes)/len(sizes), 
            ", max:", max(sizes), 
            ", min:", min(sizes), 
            ", median:", sorted(sizes)[len(sizes)//2])

        # print a histogram of the sizes with matplotlib
        plt.hist(sizes, bins=100)
        plt.yscale('log')
        plt.xlabel('size (MB)')
        plt.title('Histogram of message sizes ' + exp_name + ", " + mode)
        plt.savefig(output_path) 
        plt.clf()