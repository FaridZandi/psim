# PSIM

PSIM is a C++ simulator for evaluating distributed machine learning execution protocols under different network topologies and load-balancing strategies.

The simulator models protocol task graphs that combine compute tasks and communication flows, places them on simulated machines, routes flows through a configurable network, and reports completion time, flow-level metrics, utilization, and load-balancing decisions.

This repository also includes the experiment scripts used for the IFIP Networking 2025 paper:

> Foresight: Joint Time and Space Scheduling for Efficient Distributed ML Training

## What PSIM Simulates

PSIM represents a distributed ML workload as a dependency graph of tasks:

- **Compute tasks** execute on a machine or accelerator.
- **Flow tasks** transfer data between source and destination devices.
- **Empty tasks** act as graph markers, synchronizers, or logging points.

During simulation, PSIM advances active compute tasks and flows in discrete time steps. Network flows register their requested bandwidth on each bottleneck in their path, bottlenecks allocate bandwidth according to the configured allocator, and completed tasks trigger their dependent tasks.

## Main Features

- Protocol graph simulation with compute and communication dependencies.
- Fat-tree, leaf-spine, and big-switch network models.
- Multiple core/path selection policies, including random, ECMP, round-robin, least-loaded, power-of-k, and replay-from-file modes.
- Several bandwidth allocation policies, including fair-share, max-min fair-share, fixed-level priority, and priority queue allocation.
- Multi-run experiments with per-run logs, flow information, load-balancing decisions, and regret measurements.
- Python experiment orchestration and plotting scripts for reproducing paper figures.

## Repository Layout

```text
.
├── CMakeLists.txt              # C++ build definition
├── Readme.md                   # Original project notes
├── Readme2.md                  # Professional README draft
├── TODO                        # Development notes and open design tasks
├── include/                    # Public C++ headers
├── src/                        # Simulator implementation
├── deps/                       # Git submodules: spdlog and nlohmann/json
├── input/                      # Protocol inputs and placement/routing inputs
├── run/                        # Experiment runners, plotting, and utilities
├── playground/                 # Scratch analysis and development scripts
└── setup-pycc.sh               # Python/C++ environment helper
```

Important implementation entry points:

- `src/main.cc` sets up configuration, logging, repetitions, and output directories.
- `src/psim.cc` owns the main simulation loop and result logging.
- `src/network.cc` implements shared network, machine, and bottleneck behavior.
- `src/core_network.cc` implements fat-tree and leaf-spine network behavior.
- `src/loadbalancer.cc` implements path/core selection policies.
- `src/protocol_builder.cc` loads or generates protocol graphs.
- `include/gconfig.h` defines global runtime configuration.

## Dependencies

PSIM currently expects:

- A C++17 compiler.
- CMake.
- Boost Program Options.
- Python development headers/libraries.
- Python packages used by plotting and experiment scripts:
  - `matplotlib`
  - `numpy`
  - `pandas`
  - `networkx`
  - `seaborn`
  - `scipy`
- Git submodules:
  - `deps/spdlog`
  - `deps/json`

On Ubuntu-like systems, the base system dependencies are typically:

```bash
sudo apt-get update
sudo apt-get install -y cmake g++ libboost-all-dev python3-dev
python3 -m pip install matplotlib numpy pandas networkx seaborn scipy
```

## Cloning

Clone with submodules:

```bash
git clone --recursive git@github.com:FaridZandi/psim.git
cd psim
```

If the repository was already cloned without submodules:

```bash
git submodule update --init --recursive
```

This is required because the CMake build imports `deps/spdlog` and `deps/json`.

## Building

```bash
mkdir -p build
cd build
cmake ..
make -j
```

The build creates the `psim` executable under `build/`.

## Quick Start

From the build directory, run the simulator with a protocol input:

```bash
./psim \
  --protocol-file-dir ../input/128search \
  --protocol-file-name vgg128-simtime.txt \
  --network-type leafspine \
  --lb-scheme roundrobin \
  --rep-count 1
```

Output is written under the configured workers directory. By default, PSIM writes to:

```text
workers/worker-<worker-id>/run-<rep>/
```

Typical generated files include:

- `runtime.txt`
- `results.txt`
- `lb-decisions.txt`
- `regrets.txt`
- `flow-info.txt`

## Configuration

PSIM is configured through command-line flags that populate the global configuration object in `include/gconfig.h`.

Common options:

| Option | Description |
| --- | --- |
| `--protocol-file-dir` | Directory containing protocol input files. |
| `--protocol-file-name` | Protocol file name, or comma-separated protocol file names. |
| `--network-type` | Network model: `fattree`, `leafspine`, or `bigswitch`. |
| `--lb-scheme` | Load-balancing scheme, such as `random`, `roundrobin`, `ecmp`, `leastloaded`, or `powerof2`. |
| `--machine-count` | Number of machines/devices in the simulated cluster. |
| `--link-bandwidth` | Base link bandwidth. |
| `--rep-count` | Number of repeated simulation runs. |
| `--step-size` | Fixed simulation time step. |
| `--workers-dir` | Directory where per-run output is written. |
| `--simulation-seed` | Base seed used for repeated runs. |

For the full option list:

```bash
./build/psim --help
```

## Protocol Inputs

Protocol files describe task graphs. The loader currently recognizes lines for:

- `Comm` communication tasks.
- `Forw` and `Back` compute tasks.
- `AllR` empty/synchronization tasks.

The default protocol directory is configured as `../input`, and the default protocol file name is `vgg.txt`. Most experiment scripts pass explicit protocol paths and generated placement, timing, or routing files.

## Running Paper Experiments

The `run/` directory contains Python scripts for reproducing or extending the experiments.

From `run/`:

```bash
# Figure 5
python sweep-components-jobsizes.py
python sweep-components-oversub.py

# Figure 6
python sweep-placement.py

# Figure 7
python sweep-intensity.py
python sweep-topology.py
```

Experiment results are written under:

```text
run/results/exps/
```

The experiment scripts expect a built simulator at `build/psim` and may copy that binary into per-run result directories.

## Development Notes

This repository is research-oriented and contains several areas that are good candidates for cleanup:

- Modernize CMake target definitions and project metadata.
- Replace shell-based filesystem operations with `std::filesystem`.
- Move global configuration out of the singleton-style `GConf` object.
- Replace fixed-size job progress arrays with dynamically sized containers.
- Clarify the boundary between reusable simulator code and experiment-specific scripts.
- Document the protocol file format with a complete example.
- Add a small smoke-test input and a deterministic quick-start command.

## Current Status

PSIM is actively useful as a research simulator, but the repository still reflects its research-prototype history. The core simulator is implemented in C++, while experiment generation, execution, and plotting are handled by Python scripts under `run/`.

For new contributors, the best starting points are:

1. Build the simulator.
2. Run a single small protocol input.
3. Inspect the generated `results.txt` and `flow-info.txt`.
4. Follow one sweep script under `run/` to understand how large experiment batches are configured.

