#!/bin/bash

# Gracefully stops a cluster started by start-cluster.sh (each node broadcasts LEAVE on shutdown).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$(dirname "$SCRIPT_DIR")/logs/cluster.pids"

if [ ! -f "$PID_FILE" ]; then
    echo "No running cluster found ($PID_FILE does not exist)."
    exit 0
fi

PIDS=$(cat "$PID_FILE")
kill $PIDS 2>/dev/null || true
for _ in $(seq 1 20); do
    if ! kill -0 $PIDS 2>/dev/null; then
        break
    fi
    sleep 0.5
done
kill -9 $PIDS 2>/dev/null || true
rm -f "$PID_FILE"
echo "Cluster stopped."
