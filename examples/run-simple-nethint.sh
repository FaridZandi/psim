#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BUILD_DIR="${REPO_ROOT}/build"
PSIM_BIN="${BUILD_DIR}/psim"
CONFIG_FILE="${CONFIG_FILE:-${SCRIPT_DIR}/simple-nethint.config.sh}"

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

if [[ ! -x "${PSIM_BIN}" ]]; then
  cmake -S "${REPO_ROOT}" -B "${BUILD_DIR}"
  cmake --build "${BUILD_DIR}" -j
fi

"${PSIM_BIN}" "${PSIM_ARGS[@]}"

echo
echo "Output written under: ${WORKERS_DIR}/worker-0/run-1"
echo "Trace written to: ${TRACE_FILE}"
echo
echo "Serving PSIM viewer at: http://0.0.0.0:${PORT}/web/"
echo "From your local computer, open the server-forwarded port 8080."
echo "Press Ctrl-C here to stop the web server."
cd "${REPO_ROOT}"
python3 -m http.server "${PORT}" --bind 0.0.0.0
