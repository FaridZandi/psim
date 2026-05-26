#!/usr/bin/env python3
import argparse
import json
import os
import re
import shlex
import shutil
import subprocess
import threading
import time
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
PSIM_BIN = REPO_ROOT / "build" / "psim"
CONFIG_FILE = SCRIPT_DIR / "simple-nethint.config.sh"
SCHEDULER = SCRIPT_DIR / "run-sample-scheduling.py"
RUNS_ROOT = REPO_ROOT / "workers" / "web-runs"

RUNS = {}
RUNS_LOCK = threading.Lock()


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


def load_shell_config(config_file=CONFIG_FILE):
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
    subprocess.run(["cmake", "-S", str(REPO_ROOT), "-B", str(PSIM_BIN.parent)], check=True)
    subprocess.run(["cmake", "--build", str(PSIM_BIN.parent), "-j"], check=True)


def make_run(schedule, settings):
    run_id = time.strftime("%Y%m%d-%H%M%S")
    with RUNS_LOCK:
        suffix = 1
        base_id = run_id
        while run_id in RUNS:
            suffix += 1
            run_id = f"{base_id}-{suffix}"

        output_dir = RUNS_ROOT / run_id
        run = {
            "id": run_id,
            "schedule": schedule,
            "settings": settings,
            "status": "queued",
            "created_at": time.time(),
            "started_at": None,
            "finished_at": None,
            "returncode": None,
            "output_dir": str(output_dir),
            "run_dir": str(output_dir / "worker-0" / "run-1"),
            "trace_file": str(output_dir / "worker-0" / "run-1" / "trace.jsonl"),
            "log_file": str(output_dir / "runner.log"),
            "command": [],
            "metrics": {},
            "error": None,
        }
        RUNS[run_id] = run
        write_run_manifest(run)
    return run


def update_run(run_id, **updates):
    with RUNS_LOCK:
        RUNS[run_id].update(updates)
        run = dict(RUNS[run_id])
    write_run_manifest(run)


def delete_run(run_id):
    with RUNS_LOCK:
        run = RUNS.get(run_id)
        if not run:
            return None, "not_found"
        if run["status"] in {"queued", "running"}:
            return run, "active"
        RUNS.pop(run_id)

    output_dir = Path(run["output_dir"]).resolve()
    runs_root = RUNS_ROOT.resolve()
    if output_dir == runs_root or runs_root not in output_dir.parents:
        return run, "unsafe_path"

    shutil.rmtree(output_dir, ignore_errors=True)
    return run, "deleted"


def run_manifest_path(run):
    return Path(run["output_dir"]) / "run.json"


def write_run_manifest(run):
    path = run_manifest_path(run)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {key: value for key, value in run.items() if key != "log_tail"}
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
        f.write("\n")


def read_log_tail(path, limit=12000):
    if not Path(path).exists():
        return ""
    text = Path(path).read_text(errors="replace")
    return text[-limit:]


def parse_results(run_dir):
    results_path = Path(run_dir) / "results.txt"
    if not results_path.exists():
        return {}

    patterns = {
        "psim_time": r"psim time:\s*([0-9.]+)",
        "total_congested_time": r"Total congested time:\s*([0-9.]+)",
        "average_fct": r"average_fct:\s*([0-9.]+)",
        "average_flow_bw": r"average_flow_bw:\s*([0-9.]+)",
        "average_flow_path_length": r"average_flow_path_length:\s*([0-9.]+)",
        "machine_utilization": r"total machine utilization rate:\s*([0-9.]+)",
    }
    text = results_path.read_text(errors="replace")
    metrics = {}
    for key, pattern in patterns.items():
        matches = re.findall(pattern, text)
        if matches:
            metrics[key] = float(matches[-1])
    return metrics


def parse_time_from_run_id(run_id, fallback):
    try:
        return time.mktime(time.strptime(run_id[:15], "%Y%m%d-%H%M%S"))
    except ValueError:
        return fallback


def read_command(log_file):
    path = Path(log_file)
    if not path.exists():
        return []

    lines = path.read_text(errors="replace").splitlines()
    for index, line in enumerate(lines):
        if line.strip() == "Running:" and index + 1 < len(lines):
            try:
                return shlex.split(lines[index + 1])
            except ValueError:
                return [lines[index + 1]]
    return []


def setting_from_command(command, name, default):
    prefix = f"--{name}="
    for index, item in enumerate(command):
        if item.startswith(prefix):
            return item[len(prefix):]
        if item == f"--{name}" and index + 1 < len(command):
            return command[index + 1]
    return default


def discover_run(run_dir):
    run_id = run_dir.name
    manifest_path = run_dir / "run.json"
    if manifest_path.exists():
        try:
            with open(manifest_path) as f:
                run = json.load(f)
        except json.JSONDecodeError:
            run = {}
    else:
        run = {}

    output_dir = str(run_dir)
    worker_run_dir = str(run_dir / "worker-0" / "run-1")
    trace_file = str(run_dir / "worker-0" / "run-1" / "trace.jsonl")
    log_file = str(run_dir / "runner.log")
    command = run.get("command") or read_command(log_file)
    schedule = run.get("schedule")
    if schedule is None:
        schedule = (run_dir / "scheduling-summary.json").exists() or any("run-sample-scheduling.py" in item for item in command)

    settings = run.get("settings") or {}
    if schedule:
        settings = {
            "timing_scheme": setting_from_command(command, "timing-scheme", settings.get("timing_scheme", "faridv6")),
            "routing_fit_strategy": setting_from_command(command, "routing-fit-strategy", settings.get("routing_fit_strategy", "graph-coloring-v7")),
            "farid_rounds": int(setting_from_command(command, "farid-rounds", settings.get("farid_rounds", 10))),
            "subflows": int(setting_from_command(command, "subflows", settings.get("subflows", 4))),
        }

    results_path = Path(worker_run_dir) / "results.txt"
    trace_path = Path(trace_file)
    metrics = parse_results(worker_run_dir)
    log_tail = read_log_tail(log_file)

    status = run.get("status", "failed")
    returncode = run.get("returncode")
    error = run.get("error")
    if results_path.exists():
        status = "finished"
        returncode = 0 if returncode is None else returncode
        error = None
    elif "ERROR:" in log_tail or "Traceback" in log_tail:
        status = "failed"
        error = error or "run did not finish"
    elif command:
        status = "failed"
        error = error or "server restarted before this run finished"

    created_at = run.get("created_at") or parse_time_from_run_id(run_id, run_dir.stat().st_mtime)
    started_at = run.get("started_at") or created_at
    finished_at = run.get("finished_at")
    if finished_at is None and (results_path.exists() or status == "failed"):
        existing_files = [path for path in [results_path, trace_path, Path(log_file)] if path.exists()]
        finished_at = max((path.stat().st_mtime for path in existing_files), default=run_dir.stat().st_mtime)

    return {
        "id": run_id,
        "schedule": bool(schedule),
        "settings": settings,
        "status": status,
        "created_at": created_at,
        "started_at": started_at,
        "finished_at": finished_at,
        "returncode": returncode,
        "output_dir": output_dir,
        "run_dir": worker_run_dir,
        "trace_file": trace_file,
        "log_file": log_file,
        "command": command,
        "metrics": metrics,
        "error": error,
    }


def discover_runs():
    discovered = {}
    if RUNS_ROOT.exists():
        for run_dir in sorted(RUNS_ROOT.iterdir()):
            if not run_dir.is_dir():
                continue
            run = discover_run(run_dir)
            discovered[run["id"]] = run
            write_run_manifest(run)

    with RUNS_LOCK:
        RUNS.clear()
        RUNS.update(discovered)
    print(f"Discovered {len(discovered)} previous run(s).")


def run_process(run, cmd):
    output_dir = Path(run["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)
    update_run(run["id"], status="running", started_at=time.time(), command=cmd)

    with open(run["log_file"], "w") as log:
        log.write("Running:\n")
        log.write(" ".join(shlex.quote(str(part)) for part in cmd))
        log.write("\n\n")
        log.flush()

        proc = subprocess.Popen(
            cmd,
            cwd=REPO_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        for line in proc.stdout:
            log.write(line)
            log.flush()
        returncode = proc.wait()

    status = "finished" if returncode == 0 else "failed"
    metrics = parse_results(run["run_dir"])
    update_run(
        run["id"],
        status=status,
        finished_at=time.time(),
        returncode=returncode,
        metrics=metrics,
        error=None if returncode == 0 else f"process exited with {returncode}",
    )


def run_unscheduled(run):
    config = load_shell_config()
    build_psim()
    options = config["psim_options"]
    options.update({
        "workers-dir": run["output_dir"],
        "trace-file": run["trace_file"],
        "lb-scheme": "random",
    })
    options.pop("timing-file", None)
    options.pop("routing-file", None)
    cmd = [str(PSIM_BIN), *option_args(options)]
    run_process(run, cmd)


def run_scheduled(run):
    settings = run["settings"]
    cmd = [
        str(SCHEDULER),
        "--config-file", str(CONFIG_FILE),
        "--output-dir", run["output_dir"],
        "--timing-scheme", settings.get("timing_scheme", "faridv6"),
        "--routing-fit-strategy", settings.get("routing_fit_strategy", "graph-coloring-v7"),
        "--farid-rounds", str(settings.get("farid_rounds", 10)),
        "--subflows", str(settings.get("subflows", 4)),
    ]
    run_process(run, cmd)


def run_background(run):
    try:
        if run["schedule"]:
            run_scheduled(run)
        else:
            run_unscheduled(run)
    except Exception as exc:
        output_dir = Path(run["output_dir"])
        output_dir.mkdir(parents=True, exist_ok=True)
        with open(run["log_file"], "a") as log:
            log.write(f"\nERROR: {exc}\n")
        update_run(
            run["id"],
            status="failed",
            finished_at=time.time(),
            error=str(exc),
        )


def public_run(run):
    trace_exists = Path(run["trace_file"]).exists()
    data = dict(run)
    data["duration"] = None
    if run["started_at"]:
        end = run["finished_at"] or time.time()
        data["duration"] = round(end - run["started_at"], 2)
    data["trace_exists"] = trace_exists
    data["trace_url"] = f"/api/runs/{run['id']}/trace"
    data["viewer_url"] = f"/web/index.html?trace=/api/runs/{run['id']}/trace"
    data["log_tail"] = read_log_tail(run["log_file"])
    return data


class RunnerHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(REPO_ROOT), **kwargs)

    def send_json(self, data, status=HTTPStatus.OK):
        body = json.dumps(data, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/":
            self.path = "/web/run.html"
            return super().do_GET()

        if path == "/api/runs":
            with RUNS_LOCK:
                runs = [public_run(run) for run in sorted(RUNS.values(), key=lambda item: item["created_at"], reverse=True)]
            return self.send_json({"runs": runs})

        match = re.fullmatch(r"/api/runs/([^/]+)", path)
        if match:
            run_id = match.group(1)
            with RUNS_LOCK:
                run = RUNS.get(run_id)
            if not run:
                return self.send_json({"error": "run not found"}, HTTPStatus.NOT_FOUND)
            return self.send_json(public_run(run))

        match = re.fullmatch(r"/api/runs/([^/]+)/trace", path)
        if match:
            run_id = match.group(1)
            with RUNS_LOCK:
                run = RUNS.get(run_id)
            if not run:
                return self.send_json({"error": "run not found"}, HTTPStatus.NOT_FOUND)
            trace_path = Path(run["trace_file"])
            if not trace_path.exists():
                return self.send_json({"error": "trace not ready"}, HTTPStatus.NOT_FOUND)
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/x-ndjson")
            self.send_header("Content-Length", str(trace_path.stat().st_size))
            self.end_headers()
            with open(trace_path, "rb") as f:
                self.wfile.write(f.read())
            return

        return super().do_GET()

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path != "/api/runs":
            return self.send_json({"error": "not found"}, HTTPStatus.NOT_FOUND)

        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length).decode("utf-8") if length else "{}"
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            return self.send_json({"error": "invalid JSON"}, HTTPStatus.BAD_REQUEST)

        schedule = bool(payload.get("schedule", False))
        settings = {
            "timing_scheme": str(payload.get("timing_scheme", "faridv6")),
            "routing_fit_strategy": str(payload.get("routing_fit_strategy", "graph-coloring-v7")),
            "farid_rounds": int(payload.get("farid_rounds", 10)),
            "subflows": int(payload.get("subflows", 4)),
        }
        run = make_run(schedule, settings)
        thread = threading.Thread(target=run_background, args=(run,), daemon=True)
        thread.start()
        return self.send_json(public_run(run), HTTPStatus.CREATED)

    def do_DELETE(self):
        parsed = urlparse(self.path)
        match = re.fullmatch(r"/api/runs/([^/]+)", parsed.path)
        if not match:
            return self.send_json({"error": "not found"}, HTTPStatus.NOT_FOUND)

        run_id = match.group(1)
        run, result = delete_run(run_id)
        if result == "not_found":
            return self.send_json({"error": "run not found"}, HTTPStatus.NOT_FOUND)
        if result == "active":
            return self.send_json({"error": "cannot delete a queued or running run"}, HTTPStatus.CONFLICT)
        if result == "unsafe_path":
            return self.send_json({"error": "refusing to delete unsafe path"}, HTTPStatus.BAD_REQUEST)
        return self.send_json({"deleted": run_id})


def main():
    parser = argparse.ArgumentParser(description="Serve the PSIM run launcher web UI.")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()

    RUNS_ROOT.mkdir(parents=True, exist_ok=True)
    discover_runs()
    server = ThreadingHTTPServer((args.host, args.port), RunnerHandler)
    print(f"PSIM runner: http://{args.host}:{args.port}/")
    print(f"Trace viewer: http://{args.host}:{args.port}/web/")
    server.serve_forever()


if __name__ == "__main__":
    main()
