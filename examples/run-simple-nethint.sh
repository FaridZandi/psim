#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BUILD_DIR="${REPO_ROOT}/build"
PSIM_BIN="${BUILD_DIR}/psim"
PLACEMENT_FILE="$(mktemp "${TMPDIR:-/tmp}/psim-simple-nethint-placement.XXXXXX.json")"
WORKERS_DIR="${REPO_ROOT}/workers/simple-nethint"
TRACE_FILE="${WORKERS_DIR}/worker-0/run-1/trace.jsonl"
PORT="${PORT:-8080}"

trap 'rm -f "${PLACEMENT_FILE}"' EXIT

if [[ ! -x "${PSIM_BIN}" ]]; then
  cmake -S "${REPO_ROOT}" -B "${BUILD_DIR}"
  cmake --build "${BUILD_DIR}" -j
fi

cat > "${PLACEMENT_FILE}" <<'JSON'
[
  {
    "job_id": 1,
    "machine_count": 4,
    "comm_size": 9000,
    "comp_size": 190,
    "layer_count": 1,
    "iter_count": 7,
    "machines": [0, 1, 4, 5]
  },
  {
    "job_id": 2,
    "machine_count": 4,
    "comm_size": 12000,
    "comp_size": 330,
    "layer_count": 1,
    "iter_count": 5,
    "machines": [2, 3, 6, 7]
  }
]
JSON

"${PSIM_BIN}" \
  --protocol-file-name nethint-test \
  --placement-file "${PLACEMENT_FILE}" \
  --network-type leafspine \
  --lb-scheme random \
  --machine-count 8 \
  --ft-server-per-rack 4 \
  --ft-rack-per-pod 1 \
  --ft-core-count 1 \
  --link-bandwidth 10 \
  --priority-allocator maxmin \
  --initial-rate 5 \
  --min-rate 5 \
  --step-size 0.1 \
  --trace-snapshots \
  --trace-snapshot-interval 1000 \
  --trace-file "${TRACE_FILE}" \
  --rep-count 1 \
  --workers-dir "${WORKERS_DIR}" \
  --console-log-level 5

echo
echo "Output written under: ${WORKERS_DIR}/worker-0/run-1"
echo "Trace written to: ${TRACE_FILE}"
echo
echo "Serving PSIM viewer at: http://0.0.0.0:${PORT}/web/"
echo "From your local computer, open the server-forwarded port 8080."
echo "Press Ctrl-C here to stop the web server."
cd "${REPO_ROOT}"
python3 -m http.server "${PORT}" --bind 0.0.0.0
