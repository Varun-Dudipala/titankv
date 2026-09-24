#!/bin/bash

# Runs the load generator against a running node or cluster, or the protocol size comparison.
#
#   ./scripts/run-benchmark.sh --hosts localhost:9001,localhost:9002,localhost:9003 --threads 16
#   ./scripts/run-benchmark.sh --protocol
#
# All options other than --protocol are passed to LoadGenerator (see --help).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

JAR_FILE=$(ls -t "$PROJECT_DIR"/target/titankv-*.jar 2>/dev/null | grep -v original | head -1 || true)
if [ -z "$JAR_FILE" ]; then
    echo "JAR not found, building..."
    (cd "$PROJECT_DIR" && mvn -q package -DskipTests)
    JAR_FILE=$(ls -t "$PROJECT_DIR"/target/titankv-*.jar | grep -v original | head -1)
fi

mkdir -p "$PROJECT_DIR/target/benchmark"
javac -cp "$JAR_FILE" -d "$PROJECT_DIR/target/benchmark" "$PROJECT_DIR"/benchmark/*.java

MAIN=com.titankv.benchmark.LoadGenerator
if [ "${1:-}" = "--protocol" ]; then
    MAIN=com.titankv.benchmark.ProtocolOverhead
    shift
fi

# Keep client-side INFO logging out of the results
exec java -Dlogback.configurationFile=/dev/null -cp "$JAR_FILE:$PROJECT_DIR/target/benchmark" "$MAIN" "$@"
