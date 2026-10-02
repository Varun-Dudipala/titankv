#!/bin/bash

# Starts a local TitanKV cluster (3 nodes by default) in the background and waits until every
# node sees every other node alive. Stop it with ./scripts/stop-cluster.sh.
#
#   NODES=5 ./scripts/start-cluster.sh
#
# Runs in dev mode (no authentication, no WAL) unless TITANKV_CLUSTER_SECRET is set.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
NODES="${NODES:-3}"
BASE_PORT="${BASE_PORT:-9001}"
LOG_DIR="$PROJECT_DIR/logs"
PID_FILE="$LOG_DIR/cluster.pids"

if [ -z "${TITANKV_CLUSTER_SECRET:-}" ] && [ -z "${TITANKV_DEV_MODE:-}" ]; then
    export TITANKV_DEV_MODE=true
fi
export TITANKV_DATA_DIR="${TITANKV_DATA_DIR:-$PROJECT_DIR/data}"

if [ -f "$PID_FILE" ] && kill -0 $(cat "$PID_FILE") 2>/dev/null; then
    echo "A cluster is already running (PIDs $(cat "$PID_FILE")). Stop it with ./scripts/stop-cluster.sh"
    exit 1
fi

JAR_FILE=$(ls -t "$PROJECT_DIR"/target/titankv-*.jar 2>/dev/null | grep -v original | head -1 || true)
if [ -z "$JAR_FILE" ]; then
    echo "JAR not found, building..."
    (cd "$PROJECT_DIR" && mvn -q package -DskipTests)
    JAR_FILE=$(ls -t "$PROJECT_DIR"/target/titankv-*.jar | grep -v original | head -1)
fi

mkdir -p "$LOG_DIR"
PIDS=()
for ((i = 0; i < NODES; i++)); do
    PORT=$((BASE_PORT + i))
    ARGS=(--port "$PORT")
    # Every node lists the others as seeds, so any node can restart and rejoin
    SEEDS=""
    for ((j = 0; j < NODES; j++)); do
        if [ "$j" -ne "$i" ]; then
            SEEDS="${SEEDS:+$SEEDS,}localhost:$((BASE_PORT + j))"
        fi
    done
    if [ -n "$SEEDS" ]; then
        ARGS+=(--seeds "$SEEDS")
    fi
    nohup java -jar "$JAR_FILE" "${ARGS[@]}" > "$LOG_DIR/node$((i + 1)).log" 2>&1 &
    PIDS+=($!)
    echo "Started node $((i + 1)) on port $PORT (PID $!, metrics on $((PORT + 90)))"
done
echo "${PIDS[*]}" > "$PID_FILE"

echo -n "Waiting for the cluster to form"
for _ in $(seq 1 60); do
    READY=0
    for ((i = 0; i < NODES; i++)); do
        STATUS=$(curl -s "http://localhost:$((BASE_PORT + i + 90))/status" || true)
        ALIVE=$(echo "$STATUS" | sed -n 's/.*"alive_nodes": \([0-9]*\).*/\1/p')
        if [ "${ALIVE:-0}" = "$NODES" ] && { [ "$NODES" = 1 ] || echo "$STATUS" | grep -q '"ready": true'; }; then
            READY=$((READY + 1))
        fi
    done
    if [ "$READY" = "$NODES" ]; then
        echo ""
        echo "Cluster of $NODES nodes is up. Logs are in $LOG_DIR; stop it with ./scripts/stop-cluster.sh"
        exit 0
    fi
    echo -n "."
    sleep 1
done

echo ""
echo "Cluster did not form within 60s; stopping it. Check the logs in $LOG_DIR."
"$SCRIPT_DIR/stop-cluster.sh"
exit 1
