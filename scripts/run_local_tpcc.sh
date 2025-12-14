#!/usr/bin/env bash

# Simple wrapper to launch a local Paxos-backed TPC-C benchmark.
# Starts two follower replicas plus the leader/client and collects logs.

set -euo pipefail

THREADS=6
WAREHOUSES=6
NSHARDS=1
SHARD_INDEX=0
LOG_DIR="logs/local_tpcc"
SHARD_CONFIG=""
PAXOS_CFG=""
CC_CFG="config/occ_paxos.yml"
PORT_BASE=31000
EXTRA_FLAGS=()
include_learner=1

usage() {
    cat <<EOF
Usage: $0 [options] [-- additional dbtest flags]

Options:
  -t, --threads N     Worker threads per process (default: ${THREADS})
  -s, --scale  N      Warehouses per shard (default: ${WAREHOUSES})
      --warehouses N  Same as --scale
      --shards N      Number of logical shards (default: ${NSHARDS})
      --shard-index N Shard index to run (default: ${SHARD_INDEX})
      --shard-config PATH  Explicit shard config (overrides auto selection)
      --paxos-config PATH  Use an existing Paxos config (skip auto-generation)
      --port-base N   Base TCP port for leader (default: ${PORT_BASE})
  -l, --log-dir DIR   Directory to store logs (default: ${LOG_DIR})
      --no-learner    Skip launching the learner replica (default: launch)
  -h, --help          Show this message

Environment:
  DBTEST_BIN   Override path to dbtest binary (default: ./build/dbtest)

Any arguments following '--' are passed straight to dbtest.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -t|--threads)
            THREADS="$2"
            shift 2
            ;;
        -s|--scale|--warehouses)
            WAREHOUSES="$2"
            shift 2
            ;;
        --shards)
            NSHARDS="$2"
            shift 2
            ;;
        --shard-index)
            SHARD_INDEX="$2"
            shift 2
            ;;
        --shard-config)
            SHARD_CONFIG="$2"
            shift 2
            ;;
        --paxos-config)
            PAXOS_CFG="$2"
            shift 2
            ;;
        --port-base)
            PORT_BASE="$2"
            shift 2
            ;;
        -l|--log-dir)
            LOG_DIR="$2"
            shift 2
            ;;
        --no-learner)
            include_learner=0
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        --)
            shift
            EXTRA_FLAGS=("$@")
            break
            ;;
        *)
            echo "Unknown option: $1" >&2
            usage
            exit 1
            ;;
    esac
done

DBTEST_BIN="${DBTEST_BIN:-./build/dbtest}"

if [[ ! -x "${DBTEST_BIN}" ]]; then
    echo "Error: ${DBTEST_BIN} not found or not executable. Build the project first (e.g. make -j)." >&2
    exit 1
fi

if [[ -z "${SHARD_CONFIG}" ]]; then
    SHARD_CONFIG="src/mako/config/local-shards${NSHARDS}-warehouses${WAREHOUSES}.yml"
fi

if [[ ! -f "${SHARD_CONFIG}" ]]; then
    echo "Error: shard config ${SHARD_CONFIG} not found. Run ./src/mako/update_config.sh or choose an existing file." >&2
    exit 1
fi

generate_paxos_config() {
    local outfile="${LOG_DIR}/paxos_autogen_shard${SHARD_INDEX}_threads${THREADS}.yml"
    local leader_base=$((PORT_BASE))
    local p1_base=$((PORT_BASE + 100))
    local p2_base=$((PORT_BASE + 200))
    local learner_base=$((PORT_BASE + 300))

    {
        echo "site:"
        echo "  server:"
        for ((i=0; i<THREADS; i++)); do
            local leader_name=$((101 + i))
            local p1_name=$((201 + i))
            local p2_name=$((301 + i))
            local learner_name=$((401 + i))
            local leader_port=$((leader_base + i + 1))
            local p1_port=$((p1_base + i + 1))
            local p2_port=$((p2_base + i + 1))
            local learner_port=$((learner_base + i + 1))
            printf '    - ["s%d:%d", "s%d:%d", "s%d:%d", "s%d:%d"]\n' \
                "${leader_name}" "${leader_port}" \
                "${p1_name}" "${p1_port}" \
                "${p2_name}" "${p2_port}" \
                "${learner_name}" "${learner_port}"
        done
        echo ""
        echo "process:"
        for ((i=0; i<THREADS; i++)); do
            printf "  s%d: localhost\n" $((101 + i))
            printf "  s%d: p1\n" $((201 + i))
            printf "  s%d: p2\n" $((301 + i))
            printf "  s%d: learner\n" $((401 + i))
        done
        echo ""
        echo "host:"
        echo "  localhost: 127.0.0.1"
        echo "  p1: 127.0.0.1"
        echo "  p2: 127.0.0.1"
        echo "  learner: 127.0.0.1"
    } > "${outfile}"
    echo "${outfile}"
}

if [[ -z "${PAXOS_CFG}" ]]; then
    PAXOS_CFG="$(generate_paxos_config)"
else
    if [[ ! -f "${PAXOS_CFG}" ]]; then
        echo "Error: specified Paxos config ${PAXOS_CFG} not found." >&2
        exit 1
    fi
fi

if [[ ! -f "${CC_CFG}" ]]; then
    echo "Error: missing config ${CC_CFG}." >&2
    exit 1
fi

mkdir -p "${LOG_DIR}"

pre_run_cleanup() {
    echo "Cleaning up any lingering dbtest processes..."
    pkill -9 -f simpleTransactionRep 2>/dev/null || true
    pkill -9 -f dbtest 2>/dev/null || true
    rm -f nfs_sync_* 2>/dev/null || true
    local username=${USER:-$(whoami)}
    rm -rf /tmp/${username}_mako_rocksdb_shard* 2>/dev/null || true
}

pre_run_cleanup

PIDS=()
cleanup() {
    if [[ ${#PIDS[@]} -gt 0 ]]; then
        echo "Stopping follower processes..."
        for pid in "${PIDS[@]}"; do
            kill "$pid" >/dev/null 2>&1 || true
        done
        wait "${PIDS[@]}" 2>/dev/null || true
    fi
}
trap cleanup EXIT

check_logs_for_errors() {
    local fatal=0
    local pattern="Failed to bind to port"
    for role in p1 p2 learner leader; do
        local log="${LOG_DIR}/${role}.log"
        if [[ -f "${log}" ]] && grep -q "${pattern}" "${log}"; then
            echo ""
            echo "Detected port binding failures in ${log}."
            echo "This usually means the sandbox or OS denied opening listening sockets."
            echo "Try running on a host where TCP binds are permitted or choose a different --port-base."
            fatal=1
        fi
    done
    return ${fatal}
}

build_command() {
    local role="$1"
    local cmd=( "${DBTEST_BIN}"
        --num-threads "${THREADS}"
        --shard-index "${SHARD_INDEX}"
        --shard-config "${SHARD_CONFIG}"
        -P "${role}"
        -F "${PAXOS_CFG}"
        -F "${CC_CFG}"
        --is-replicated
    )
    cmd+=("${EXTRA_FLAGS[@]}")
    printf '%s\0' "${cmd[@]}"
}

run_node() {
    local role="$1"
    local log="${LOG_DIR}/${role}.log"
    mapfile -d '' cmd < <(build_command "${role}")
    "${cmd[@]}" > "${log}" 2>&1 &
    local pid=$!
    PIDS+=("${pid}")
    echo "Started ${role} (pid ${pid}) log -> ${log}"
}

echo "Launching local TPC-C benchmark:"
echo "  threads     = ${THREADS}"
echo "  warehouses  = ${WAREHOUSES}"
echo "  shards      = ${NSHARDS}"
echo "  shard cfg   = ${SHARD_CONFIG}"
echo "  paxos cfg   = ${PAXOS_CFG}"
echo "  logs        = ${LOG_DIR}"

run_node "p1"
run_node "p2"
if [[ "${include_learner}" -eq 1 ]]; then
    run_node "learner"
fi
sleep 3
echo "Starting leader/client (logs in ${LOG_DIR}/leader.log)..."
mapfile -d '' leader_cmd < <(build_command "localhost")
"${leader_cmd[@]}" |& tee "${LOG_DIR}/leader.log"

if ! check_logs_for_errors; then
    echo "Benchmark finished. Logs saved under ${LOG_DIR}."
else
    exit 1
fi
