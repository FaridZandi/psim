#!/usr/bin/env python3

from utils.util import *
from utils.run_base import run_exp

options = {
    "protocol-file-name": "nethint-test",

    "step-size": 1,
    "core-status-profiling-interval": 100000,
    "rep-count": 1, 
    "console-log-level": 4,
    "file-log-level": 3,
    
    "initial-rate": 100,
    "min-rate": 100,
    "drop-chance-multiplier": 0, 
    "rate-increase": 2, 
    
    # "priority-allocator": "fairshare",
    "priority-allocator": "maxmin",
    # "priority-allocator": "priorityqueue", 

    "network-type": "leafspine",    
    "link-bandwidth": 100,
    "machine-count": 80,
    "ft-server-per-rack": 8,
    "ft-rack-per-pod": 1,
    "ft-agg-per-pod": 1,
    "ft-core-count": 4,
    "ft-pod-count": -1,
    "gpu-per-machine": 1,
    "gpu-gpu-link-capacity-mult": 2,
    "ft-server-tor-link-capacity-mult": 1,
    "ft-tor-agg-link-capacity-mult": 1,
    "ft-agg-core-link-capacity-mult": 1,
    
    # "lb-scheme": "random",
    "lb-scheme": "ecmp",
    # "lb-scheme": "roundrobin",
    
    # "lb-scheme": "leastloaded",
    "load-metric": "utilization",
    
    "shuffle-device-map": True,
    "regret-mode": "none",
    
    "simulation-seed": 46, 
    "ecmp-entropy-options": 4, 
    
    "placement-file": "/home/faridzandi/git/psim/run/sampleinput/placement.txt",
    "timing-file": "/home/faridzandi/git/psim/run/sampleinput/timing.txt",
}

run_exp(options, sys.argv)