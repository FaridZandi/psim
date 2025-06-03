# Protcol Simulator

Define protocols and networks, run the protocols and see the results!


### get the code 
```
git clone --recursive git@github.com:FaridZandi/psim.git
```


### dependencies 
```
# install gcc-9.4, python3.8

sudo apt-get install cmake libboost-all-dev -y
 
python3 -m pip install matplotlib numpy pandas networkx seaborn scipy
```

### Build 

build is done with cmake. 

```
mkdir build 
cd build 
cmake ..
make -j
```


### IFIP Neworking 2025 Submission

This simulator was used to evaluate the paper "**Foresight: Joint Time and Space Scheduling for Efficient Distributed ML Training**" published in the proceedings of the IFIP Networking 2025 Conference. 

To Run the experiments, cd into the ``run`` directory, and run any of the following: 

```
# figure 5 
python sweep-components-jobsizes.py 
python sweep-components-oversub.py

# Figure 6 
python sweep-placement.py

# Figure 7 
python sweep-intensity.py
python sweep-topology.py
```

The results will be generated under the ``results/exps/``. 