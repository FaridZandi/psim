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

### Run 

```
cd ../run
python3 run.py
```