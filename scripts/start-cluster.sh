#!/bin/bash

# TitanKV Cluster Startup Script
# Starts a 3-node cluster for testing and development

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Default to dev mode unless a cluster secret is provided
if [ -z "$TITANKV_CLUSTER_SECRET" ] && [ -z "$TITANKV_DEV_MODE" ]; then
    export TITANKV_DEV_MODE=true
fi

# Find JAR file dynamically (supports version changes)
JAR_FILE=$(ls -t "$PROJECT_DIR/target/titankv-"*.jar 2>/dev/null | grep -v original | head -1)

# Check if JAR exists, build if not
if [ -z "$JAR_FILE" ] || [ ! -f "$JAR_FILE" ]; then
    echo "JAR file not found. Building project..."
    cd "$PROJECT_DIR"
    mvn package -DskipTests -q
    JAR_FILE=$(ls -t "$PROJECT_DIR/target/titankv-"*.jar 2>/dev/null | grep -v original | head -1)
    if [ -z "$JAR_FILE" ]; then
        echo "Error: Failed to build JAR file"
        exit 1
    fi
fi

# Cleanup function
cleanup() {
    if [ -f "$PROJECT_DIR/logs/cluster.pids" ]; then
        echo "Stopping cluster..."
        kill $(cat "$PROJECT_DIR/logs/cluster.pids") 2>/dev/null || true
        rm -f "$PROJECT_DIR/logs/cluster.pids"
    fi
}
trap cleanup EXIT INT TERM

# Create logs directory
mkdir -p "$PROJECT_DIR/logs"

echo "╔════════════════════════════════════════════════════════════╗"
echo "║              Starting TitanKV Cluster                      ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

# Start Node 1 (seed node)
echo "Starting Node 1 on port 9001..."
java -jar "$JAR_FILE" --port 9001 > "$PROJECT_DIR/logs/node1.log" 2>&1 &
NODE1_PID=$!
echo "  PID: $NODE1_PID"

# Wait for Node 1 to start
sleep 2

# Start Node 2
echo "Starting Node 2 on port 9002..."
java -jar "$JAR_FILE" --port 9002 --seeds localhost:9001 > "$PROJECT_DIR/logs/node2.log" 2>&1 &
NODE2_PID=$!
echo "  PID: $NODE2_PID"

# Start Node 3
echo "Starting Node 3 on port 9003..."
java -jar "$JAR_FILE" --port 9003 --seeds localhost:9001 > "$PROJECT_DIR/logs/node3.log" 2>&1 &
NODE3_PID=$!
echo "  PID: $NODE3_PID"

# Wait for cluster to form
sleep 2

echo ""
echo "════════════════════════════════════════════════════════════"
echo "Cluster started successfully!"
echo ""
echo "Nodes:"
echo "  - Node 1: localhost:9001 (PID: $NODE1_PID)"
echo "  - Node 2: localhost:9002 (PID: $NODE2_PID)"
echo "  - Node 3: localhost:9003 (PID: $NODE3_PID)"
echo ""
echo "Logs:"
echo "  - $PROJECT_DIR/logs/node1.log"
echo "  - $PROJECT_DIR/logs/node2.log"
echo "  - $PROJECT_DIR/logs/node3.log"
echo ""
echo "To stop the cluster:"
echo "  kill $NODE1_PID $NODE2_PID $NODE3_PID"
echo ""
echo "Or use: pkill -f 'titankv.*--port'"
echo "════════════════════════════════════════════════════════════"

# Save PIDs to file for easy cleanup
echo "$NODE1_PID $NODE2_PID $NODE3_PID" > "$PROJECT_DIR/logs/cluster.pids"
