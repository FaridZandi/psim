#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BUILD_DIR="${REPO_ROOT}/build"
PSIM_BIN="${BUILD_DIR}/psim"
CONFIG_FILE="${CONFIG_FILE:-${SCRIPT_DIR}/simple-nethint.config.sh}"
SCHEDULER="${SCRIPT_DIR}/run-sample-scheduling.py"
SCHEDULE=0

usage() {
  cat <<EOF
Usage: $0 [--schedule]

Runs the sample nethint workload and starts the web viewer.

Options:
  --schedule   Generate timing/routing artifacts first, then run with readprotocol.
  -h, --help   Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --schedule)
      SCHEDULE=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ ! -f "${CONFIG_FILE}" ]]; then
  echo "Config file not found: ${CONFIG_FILE}" >&2
  exit 1
fi

# shellcheck source=/dev/null
source "${CONFIG_FILE}"

if [[ ! -f "${PLACEMENT_FILE}" ]]; then
  echo "Placement file not found: ${PLACEMENT_FILE}" >&2
  exit 1
fi

if [[ "${SCHEDULE}" -eq 1 ]]; then
  if [[ ! -x "${SCHEDULER}" ]]; then
    echo "Scheduled runner not found or not executable: ${SCHEDULER}" >&2
    exit 1
  fi

  SCHEDULED_WORKERS_DIR="${SCHEDULED_WORKERS_DIR:-${REPO_ROOT}/workers/sample-scheduling}"
  SCHEDULED_TRACE_FILE="${SCHEDULED_TRACE_FILE:-${SCHEDULED_WORKERS_DIR}/worker-0/run-1/trace.jsonl}"
  TIMING_SCHEME="${TIMING_SCHEME:-faridv6}"
  ROUTING_FIT_STRATEGY="${ROUTING_FIT_STRATEGY:-graph-coloring-v7}"
  FARID_ROUNDS="${FARID_ROUNDS:-10}"
  SUBFLOWS="${SUBFLOWS:-4}"

  "${SCHEDULER}" \
    --config-file "${CONFIG_FILE}" \
    --placement-file "${PLACEMENT_FILE}" \
    --output-dir "${SCHEDULED_WORKERS_DIR}" \
    --timing-scheme "${TIMING_SCHEME}" \
    --routing-fit-strategy "${ROUTING_FIT_STRATEGY}" \
    --farid-rounds "${FARID_ROUNDS}" \
    --subflows "${SUBFLOWS}"

  OUTPUT_DIR="${SCHEDULED_WORKERS_DIR}/worker-0/run-1"
  OUTPUT_TRACE="${SCHEDULED_TRACE_FILE}"
else
  if [[ ! -x "${PSIM_BIN}" ]]; then
    cmake -S "${REPO_ROOT}" -B "${BUILD_DIR}"
    cmake --build "${BUILD_DIR}" -j
  fi

  "${PSIM_BIN}" "${PSIM_ARGS[@]}"

  OUTPUT_DIR="${WORKERS_DIR}/worker-0/run-1"
  OUTPUT_TRACE="${TRACE_FILE}"
fi

echo
echo "Output written under: ${OUTPUT_DIR}"
echo "Trace written to: ${OUTPUT_TRACE}"
echo
echo "Serving PSIM viewer at: http://0.0.0.0:${PORT}/web/"
echo "From your local computer, open the server-forwarded port ${PORT}."
echo "Press Ctrl-C here to stop the web server."
cd "${REPO_ROOT}"
python3 -m http.server "${PORT}" --bind 0.0.0.0
