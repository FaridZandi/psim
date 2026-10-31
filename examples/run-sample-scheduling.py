#!/usr/bin/env python3
import argparse
import copy
import json
import os
import subprocess
import sys
import time
from collections.abc import Mapping
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
RUN_DIR = REPO_ROOT / "run"
PSIM_BIN = REPO_ROOT / "build" / "psim"

sys.path.insert(0, str(RUN_DIR))

from algo import timing  # noqa: E402
from algo.placement import profile_all_jobs  # noqa: E402


def option_args(options):
    args = []
    for key, value in options.items():
        if value is False or value is None:
            continue
        if value is True:
            args.append(f"--{key}")
        else:
            args.append(f"--{key}={value}")
    return args


def coerce_value(value):
    if not isinstance(value, str):
        return value

    try:
        return int(value)
    except ValueError:
        pass

    try:
        return float(value)
    except ValueError:
        return value


def parse_psim_args(args):
    options = {}
    index = 0

    while index < len(args):
        arg = args[index]
        if not arg.startswith("--"):
            raise ValueError(f"Expected option starting with --, got: {arg}")

        item = arg[2:]
        if "=" in item:
            key, value = item.split("=", 1)
            options[key] = coerce_value(value)
            index += 1
        elif index + 1 < len(args) and not args[index + 1].startswith("--"):
            options[item] = coerce_value(args[index + 1])
            index += 2
        else:
            options[item] = True
            index += 1

    return options


def load_shell_config(config_file):
    script = r'''
set -euo pipefail
SCRIPT_DIR="$1"
REPO_ROOT="$2"
CONFIG_FILE="$3"
source "${CONFIG_FILE}"
printf '%s\0' "${WORKERS_DIR}" "${PLACEMENT_FILE}" "${TRACE_FILE}" "${PORT}" "${PSIM_ARGS[@]}"
'''
    completed = subprocess.run(
        ["bash", "-c", script, "bash", str(SCRIPT_DIR), str(REPO_ROOT), str(config_file)],
        check=True,
        stdout=subprocess.PIPE,
    )
    parts = [part.decode("utf-8") for part in completed.stdout.split(b"\0") if part]
    if len(parts) < 4:
        raise ValueError(f"Could not load config from {config_file}")

    return {
        "workers_dir": parts[0],
        "placement_file": parts[1],
        "trace_file": parts[2],
        "port": parts[3],
        "psim_args": parts[4:],
        "psim_options": parse_psim_args(parts[4:]),
    }


def build_psim():
    if PSIM_BIN.exists() and os.access(PSIM_BIN, os.X_OK):
        return

    build_dir = PSIM_BIN.parent
    subprocess.run(["cmake", "-S", str(REPO_ROOT), "-B", str(build_dir)], check=True)
    subprocess.run(["cmake", "--build", str(build_dir), "-j"], check=True)


class DirectPsimRunner:
    def __init__(self, workers_dir):
        self.workers_dir = str(workers_dir)
        self.run_executable = str(PSIM_BIN)

    def only_run_command_with_options(self, run_context, options):
        cmd = [self.run_executable, *option_args(options)]
        with open(run_context["output-file"], "a") as output_file:
            output_file.write("Running:\n")
            output_file.write(" ".join(cmd))
            output_file.write("\n\n")

        completed = subprocess.run(
            cmd,
            check=True,
            cwd=REPO_ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

        lines = completed.stdout.splitlines()
        with open(run_context["output-file"], "a") as output_file:
            output_file.write("\n".join(lines))
            output_file.write("\n\n")
        return lines


def load_jobs(placement_file):
    with open(placement_file) as f:
        jobs = json.load(f)

    required = {"job_id", "machine_count", "comm_size", "comp_size", "layer_count", "iter_count", "machines"}
    for job in jobs:
        missing = required - set(job)
        if missing:
            raise ValueError(f"job {job.get('job_id', '<unknown>')} is missing fields: {sorted(missing)}")
    return jobs


def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=4)
        f.write("\n")


class ProgressWriter:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text("")
        self.started_at = time.time()
        self.seq = 0

    def emit(self, event):
        def json_safe(value):
            if isinstance(value, Mapping):
                return {str(key): json_safe(item) for key, item in value.items()}
            if isinstance(value, (list, tuple, set)):
                return [json_safe(item) for item in value]
            if hasattr(value, "item"):
                return value.item()
            return value

        self.seq += 1
        data = {
            "seq": self.seq,
            "time": time.time(),
            "elapsed": round(time.time() - self.started_at, 3),
            **json_safe(event),
        }
        with open(self.path, "a") as f:
            json.dump(data, f, sort_keys=True)
            f.write("\n")


def main():
    parser = argparse.ArgumentParser(
        description="Run one scheduled PSIM sample workload using the sample placement file."
    )
    parser.add_argument(
        "--config-file",
        default=str(SCRIPT_DIR / "simple-nethint.config.sh"),
        help="Shared shell config containing PSIM_ARGS.",
    )
    parser.add_argument(
        "--placement-file",
        default=None,
        help="Sample placement JSON to schedule.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(REPO_ROOT / "workers" / "sample-scheduling"),
        help="Directory for generated profiles, timing, routing, and simulator output.",
    )
    parser.add_argument("--timing-scheme", default="faridv6")
    parser.add_argument("--routing-fit-strategy", default="graph-coloring-v7")
    parser.add_argument("--farid-rounds", type=int, default=10)
    parser.add_argument("--subflows", type=int, default=4)
    parser.add_argument("--ft-core-count", type=int, default=None)
    parser.add_argument("--ft-server-per-rack", type=int, default=None)
    parser.add_argument("--initial-rate", type=float, default=None)
    parser.add_argument("--rate-increase", type=float, default=None)
    parser.add_argument("--min-rate", type=int, default=None)
    parser.add_argument("--skip-simulation", action="store_true")
    args = parser.parse_args()

    config = load_shell_config(Path(args.config_file).resolve())
    base_options = config["psim_options"]
    option_overrides = {
        "ft-core-count": args.ft_core_count,
        "ft-server-per-rack": args.ft_server_per_rack,
        "initial-rate": args.initial_rate,
        "rate-increase": args.rate_increase,
        "min-rate": args.min_rate,
    }
    base_options.update({
        key: value for key, value in option_overrides.items() if value is not None
    })

    placement_file = Path(args.placement_file or config["placement_file"]).resolve()
    output_dir = Path(args.output_dir).resolve()
    profiles_dir = output_dir / "profiles"
    timings_dir = output_dir / "timings"
    routings_dir = output_dir / "routings"
    trace_file = output_dir / "worker-0" / "run-1" / "trace.jsonl"
    output_file = output_dir / "scheduling-output.txt"
    progress_file = output_dir / "scheduler-progress.jsonl"
    timing_file = timings_dir / "timing.json"
    routing_file = routings_dir / "routing.json"
    enriched_placement_file = output_dir / "sample-placement-with-profiles.json"

    for path in [output_dir, profiles_dir, timings_dir, routings_dir]:
        path.mkdir(parents=True, exist_ok=True)
    output_file.write_text("")
    progress = ProgressWriter(progress_file)
    progress.emit({
        "phase": "setup",
        "status": "started",
        "timing_scheme": args.timing_scheme,
        "routing_fit_strategy": args.routing_fit_strategy,
        "farid_rounds": args.farid_rounds,
        "subflows": args.subflows,
    })

    build_psim()
    progress.emit({"phase": "setup", "status": "build_ready"})
    jobs = load_jobs(placement_file)
    progress.emit({"phase": "setup", "status": "loaded_placement", "job_count": len(jobs)})

    base_options.update({
        "placement-file": str(placement_file),
        "workers-dir": str(output_dir),
        "worker-id": 0,
        "lb-scheme": "readprotocol",
        "subflows": args.subflows,
    })

    run_context = {
        "output-file": str(output_file),
        "profiles-dir": str(profiles_dir),
        "routings-dir": str(routings_dir),
        "timings-dir": str(timings_dir),
        "experiment-seed": 777,
        "placement-seed": 1,
        "worker-id-for-profiling": 0,
        "sim-length": 20000,
        "timing-scheme": args.timing_scheme,
        "routing-fit-strategy": args.routing_fit_strategy,
        "compat-score-mode": "time-no-coll",
        "farid-rounds": args.farid_rounds,
        "fallback-threshold": 0.5,
        "throttle-search": args.subflows > 1,
        "profiled-throttle-factors": [1.0, 0.75, 0.5, 0.25] if args.subflows > 1 else [1.0],
        "profiling-core-count": int(base_options["ft-core-count"]),
        "cassini-parameters": {
            "link-solution-candidate-count": 100,
            "link-solution-random-quantum": 10,
            "link-solution-top-candidates": 3,
            "overall-solution-candidate-count": 10,
            "save-profiles": True,
        },
        "routing-parameters": {},
        "plot-initial-timing": False,
        "plot-intermediate-timing": False,
        "plot-final-timing": False,
        "plot-routing-assignment": False,
        "plot-merged-ranges": False,
        "plot-runtime-timing": False,
        "progress-callback": progress.emit,
        "plot-link-empty-times": False,
        "use_inflation": True,
    }

    runner = DirectPsimRunner(output_dir)

    print(f"Using placement: {placement_file}")
    print(f"Writing artifacts under: {output_dir}")
    print("Profiling sample jobs...")
    progress.emit({
        "phase": "profiling",
        "status": "started",
        "job_count": len(jobs),
        "throttle_count": len(run_context["profiled-throttle-factors"]),
    })
    profile_options = copy.deepcopy(base_options)
    for key in ["routing-file", "timing-file", "trace-file", "trace-snapshot-interval", "trace-snapshots"]:
        profile_options.pop(key, None)
    profile_all_jobs(jobs, profile_options, run_context, runner, str(placement_file), progress_callback=progress.emit)
    write_json(enriched_placement_file, jobs)
    progress.emit({
        "phase": "profiling",
        "status": "finished",
        "enriched_placement_file": str(enriched_placement_file),
    })

    print("Generating timing and routing schedule...")
    progress.emit({
        "phase": "scheduling",
        "status": "started",
        "timing_file": str(timing_file),
        "routing_file": str(routing_file),
    })
    schedule_options = copy.deepcopy(base_options)
    for key in ["trace-file", "trace-snapshot-interval", "trace-snapshots"]:
        schedule_options.pop(key, None)
    schedule_options["timing-file"] = str(timing_file)
    schedule_options["routing-file"] = str(routing_file)
    job_timings, lb_decisions, add_to_context = timing.generate_timing_file(
        str(timing_file),
        str(routing_file),
        run_context["placement-seed"],
        jobs,
        schedule_options,
        run_context,
    )

    write_json(output_dir / "scheduling-summary.json", {
        "placement_file": str(placement_file),
        "enriched_placement_file": str(enriched_placement_file),
        "timing_file": str(timing_file),
        "routing_file": str(routing_file),
        "job_count": len(jobs),
        "timed_jobs": len(job_timings),
        "routing_decisions": len(lb_decisions or []),
        "scheduler_context": add_to_context,
    })
    progress.emit({
        "phase": "scheduling",
        "status": "finished",
        "timed_jobs": len(job_timings),
        "routing_decisions": len(lb_decisions or []),
        "scheduler_context": add_to_context,
    })

    if args.skip_simulation:
        progress.emit({"phase": "simulation", "status": "skipped"})
        progress.emit({"phase": "done", "status": "finished"})
        print("Skipping final simulation.")
        return

    print("Running scheduled simulation...")
    progress.emit({"phase": "simulation", "status": "started", "trace_file": str(trace_file)})
    final_options = copy.deepcopy(base_options)
    final_options.update({
        "timing-file": str(timing_file),
        "routing-file": str(routing_file),
        "trace-file": str(trace_file),
    })
    runner.only_run_command_with_options(run_context, final_options)
    progress.emit({"phase": "simulation", "status": "finished", "trace_file": str(trace_file)})
    progress.emit({"phase": "done", "status": "finished"})

    print()
    print(f"Timing file: {timing_file}")
    print(f"Routing file: {routing_file}")
    print(f"Trace file: {trace_file}")
    print(f"Simulator output: {output_dir / 'worker-0' / 'run-1'}")


if __name__ == "__main__":
    main()
