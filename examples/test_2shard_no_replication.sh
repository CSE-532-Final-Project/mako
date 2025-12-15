#!/bin/bash

# Script to test 2-shard experiments without replication
# Each shard should:
# 1. Show "agg_persist_throughput" keyword
# 2. Have NewOrder_remote_abort_ratio < 20%

echo "========================================="
echo "Testing 2-shard setup without replication"
echo "========================================="

# Clean up old log files
rm -f nfs_sync_*

# Clean up RocksDB data from previous runs
USERNAME=${USER:-$(whoami)}
rm -rf /tmp/${USERNAME}_mako_rocksdb_shard*

trd=${1:-6}
script_name="$(basename "$0")"

# Determine transport type and create unique log prefix
transport="${MAKO_TRANSPORT:-rrr}"
log_prefix="${script_name}_${transport}"

ps aux | grep -i dbtest | awk "{print \$2}" | xargs kill -9 2>/dev/null
sleep 1

# Start both shards close together so they finish around the same time.
# This prevents race conditions where one shard is still sending RPCs
# while the other is shutting down.
echo "Starting shard 0..."
nohup bash bash/shard.sh 2 0 $trd localhost > ${log_prefix}_shard0-$trd.log 2>&1 &
SHARD0_PID=$!

echo "Starting shard 1..."
nohup bash bash/shard.sh 2 1 $trd localhost > ${log_prefix}_shard1-$trd.log 2>&1 &
SHARD1_PID=$!

# Brief delay for both shards to initialize before they start communicating
sleep 2

# Wait for benchmarks to complete (poll for completion markers)
echo "Waiting for benchmarks to complete..."
log_file0="${log_prefix}_shard0-$trd.log"
log_file1="${log_prefix}_shard1-$trd.log"
max_wait=120  # Maximum wait time in seconds
wait_count=0

while [ $wait_count -lt $max_wait ]; do
    shard0_done=0
    shard1_done=0

    # Check if throughput output appeared for each shard
    if [ -f "$log_file0" ] && grep -q "agg_persist_throughput" "$log_file0" 2>/dev/null; then
        shard0_done=1
    fi
    if [ -f "$log_file1" ] && grep -q "agg_persist_throughput" "$log_file1" 2>/dev/null; then
        shard1_done=1
    fi

    if [ $shard0_done -eq 1 ] && [ $shard1_done -eq 1 ]; then
        echo "Both benchmarks completed after ${wait_count}s"
        sleep 2  # Give a moment for final output
        break
    fi

    sleep 1
    wait_count=$((wait_count + 1))
    if [ $((wait_count % 10)) -eq 0 ]; then
        echo "  ... waiting (${wait_count}s elapsed, shard0=$shard0_done, shard1=$shard1_done)"
    fi
done

if [ $wait_count -ge $max_wait ]; then
    echo "Warning: Benchmarks did not complete within ${max_wait}s timeout"
fi

# Graceful shutdown: SIGTERM first
echo "Stopping shards (graceful)..."

# First, kill the parent bash scripts to prevent them from respawning dbtest
pkill -TERM -f "bash/shard.sh" 2>/dev/null || true

# Send SIGTERM to all dbtest processes
pkill -TERM dbtest 2>/dev/null || true
sleep 3

# Force kill any remaining processes
echo "Force killing remaining processes..."
pkill -9 -f "bash/shard.sh" 2>/dev/null || true
pkill -9 dbtest 2>/dev/null || true
killall -9 dbtest 2>/dev/null || true

# Wait for OS to clean up
sleep 2

# Check for and kill any remaining processes including zombies
remaining=$(ps aux | grep "dbtest" | grep -v grep | wc -l)
if [ "$remaining" -gt 0 ]; then
    echo "WARNING: $remaining dbtest processes still present after kill attempt"
    ps aux | grep "dbtest" | grep -v grep

    # Get PIDs and kill individually
    pids=$(ps aux | grep "dbtest" | grep -v grep | awk '{print $2}')
    for pid in $pids; do
        echo "Force killing PID $pid"
        kill -9 $pid 2>/dev/null || true
    done

    sleep 1
fi

# Final verification - reap zombie processes by explicitly waiting on child PIDs
for pid in $SHARD0_PID $SHARD1_PID; do
    wait $pid 2>/dev/null || true
done

echo ""
echo "========================================="
echo "Checking test results..."
echo "========================================="

failed=0

# Check each shard's output
for i in 0 1; do
    log="${log_prefix}_shard${i}-$trd.log"
    echo ""
    echo "Checking $log:"
    echo "-----------------"
    
    if [ ! -f "$log" ]; then
        echo "  ✗ Log file not found"
        failed=1
        continue
    fi
    
    # Check for agg_persist_throughput keyword
    if grep -q "agg_persist_throughput" "$log"; then
        echo "  ✓ Found 'agg_persist_throughput' keyword"
        # Show the line for reference
        grep "agg_persist_throughput" "$log" | tail -n 1 | sed 's/^/    /'
    else
        echo "  ✗ 'agg_persist_throughput' keyword not found"
        failed=1
    fi
    
    # Check NewOrder_remote_abort_ratio
    if grep -q "NewOrder_remote_abort_ratio:" "$log"; then
        # Extract the abort ratio value
        abort_ratio=$(grep "NewOrder_remote_abort_ratio:" "$log" | tail -n 1 | awk '{print $2}')
        
        if [ -z "$abort_ratio" ]; then
            echo "  ✗ Could not extract NewOrder_remote_abort_ratio value"
            failed=1
        else
            # Remove % sign if present and convert to float
            abort_value=$(echo "$abort_ratio" | sed 's/%//')
            
            # Check if value is less than 20 using awk (more portable than bc)
            if awk "BEGIN {exit !($abort_value < 20)}"; then
                echo "  ✓ NewOrder_remote_abort_ratio: $abort_ratio (< 20%)"
            else
                echo "  ✗ NewOrder_remote_abort_ratio: $abort_ratio (>= 20%)"
                failed=1
            fi
        fi
    else
        echo "  ✗ NewOrder_remote_abort_ratio not found"
        failed=1
    fi
done

echo ""
echo "========================================="
if [ $failed -eq 0 ]; then
    echo "All checks passed!"
    echo "========================================="
    exit 0
else
    echo "Some checks failed!"
    echo "========================================="
    echo ""
    echo "Debug information:"
    echo "Check ${log_prefix}_shard*-$trd for details"
    tail -n 10 ${log_prefix}_shard0-$trd.log ${log_prefix}_shard1-$trd.log
    exit 1
fi
