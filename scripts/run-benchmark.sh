#!/bin/bash

# TitanKV Benchmark Script
# Runs the load generator against the cluster

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Default parameters
HOSTS="localhost:9001,localhost:9002,localhost:9003"
THREADS=50
OPS_PER_THREAD=10000
READ_RATIO=0.8
VALUE_SIZE=100

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --hosts|-h)
            HOSTS="$2"
            shift 2
            ;;
        --threads|-t)
            THREADS="$2"
            shift 2
            ;;
        --ops|-o)
            OPS_PER_THREAD="$2"
            shift 2
            ;;
        --read-ratio|-r)
            READ_RATIO="$2"
            shift 2
            ;;
        --value-size|-v)
            VALUE_SIZE="$2"
            shift 2
            ;;
        --single)
            HOSTS="localhost:9001"
            shift
            ;;
        --help)
            echo "TitanKV Benchmark Script"
            echo ""
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  -h, --hosts <hosts>       Comma-separated host:port list"
            echo "  -t, --threads <n>         Number of threads (default: 50)"
            echo "  -o, --ops <n>             Operations per thread (default: 10000)"
            echo "  -r, --read-ratio <ratio>  Read ratio 0.0-1.0 (default: 0.8)"
            echo "  -v, --value-size <bytes>  Value size in bytes (default: 100)"
            echo "      --single              Run against single node only"
            echo "      --help                Show this help"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

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

# Compile benchmark
echo "Compiling benchmark..."
mkdir -p "$PROJECT_DIR/target/benchmark"
javac -cp "$JAR_FILE" \
    -d "$PROJECT_DIR/target/benchmark" \
    "$PROJECT_DIR/benchmark/LoadGenerator.java"

echo ""
echo "╔════════════════════════════════════════════════════════════╗"
echo "║              TitanKV Benchmark                             ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

# Run benchmark
java -cp "$JAR_FILE:$PROJECT_DIR/target/benchmark" \
    com.titankv.benchmark.LoadGenerator \
    --hosts "$HOSTS" \
    --threads "$THREADS" \
    --ops "$OPS_PER_THREAD" \
    --read-ratio "$READ_RATIO" \
    --value-size "$VALUE_SIZE"
