# Configuration for examples/run-simple-nethint.sh.
#
# The runner sources this file after defining SCRIPT_DIR and REPO_ROOT.

WORKERS_DIR="${WORKERS_DIR:-${REPO_ROOT}/workers/simple-nethint}"
PLACEMENT_FILE="${PLACEMENT_FILE:-${SCRIPT_DIR}/simple-nethint-placement.json}"
TRACE_FILE="${TRACE_FILE:-${WORKERS_DIR}/worker-0/run-1/trace.jsonl}"
PORT="${PORT:-8080}"

PSIM_ARGS=(
  --protocol-file-name nethint-test
  --placement-file "${PLACEMENT_FILE}"
  --network-type leafspine
  --lb-scheme random
  --load-metric utilization
  --machine-count 32
  --ft-server-per-rack 8
  --ft-rack-per-pod 1
  --ft-core-count 4
  --link-bandwidth 10
  --priority-allocator maxmin
  --initial-rate 3
  --min-rate 3
  --rate-increase 1.4
  --punish-oversubscribed 1
  --punish-oversubscribed-min 0.8
  --step-size 0.1
  --trace-snapshots
  --ft-agg-core-link-capacity-mult 1
  --ft-tor-agg-link-capacity-mult 1
  --trace-snapshot-interval 500
  --trace-file "${TRACE_FILE}"
  --rep-count 1
  --workers-dir "${WORKERS_DIR}"
  --console-log-level 5
)
