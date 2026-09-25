#!/bin/bash

# Availability under failure: 16 clients run for 40s against a 3-node production-mode cluster
# (QUORUM, auth, fsynced WAL). Node 2 is killed with SIGKILL at 10s and restarted at 25s.
# Prints throughput and errors per second, and whether any read returned a value older than
# the client's last acknowledged write.
#
# Output: benchmark/results/suite/failover.md and failover.log

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
OUT="$PROJECT_DIR/benchmark/results/suite"
LOG="$OUT/failover.log"
KILL_AT=10
RESTART_AT=25
DURATION=40
mkdir -p "$OUT"

export TITANKV_CLUSTER_SECRET=failover-benchmark-secret
export TITANKV_DATA_DIR="$PROJECT_DIR/data"
HOSTS=localhost:9001,localhost:9002,localhost:9003

(cd "$PROJECT_DIR" && mvn -q -B package -DskipTests)
JAR_FILE=$(ls -t "$PROJECT_DIR"/target/titankv-*.jar | grep -v original | head -1)

"$SCRIPT_DIR/stop-cluster.sh" > /dev/null
rm -rf "$PROJECT_DIR/data"
"$SCRIPT_DIR/start-cluster.sh" > /dev/null
cleanup() {
    [ -n "${RESTARTED_PID:-}" ] && kill "$RESTARTED_PID" 2>/dev/null || true
    "$SCRIPT_DIR/stop-cluster.sh" > /dev/null
    rm -rf "$PROJECT_DIR/data"
}
trap cleanup EXIT

"$SCRIPT_DIR/run-benchmark.sh" --hosts "$HOSTS" --threads 16 --duration 5 --warmup 500 > /dev/null 2>&1

"$SCRIPT_DIR/run-benchmark.sh" --hosts "$HOSTS" --threads 16 --duration "$DURATION" --warmup 1000 \
    --retry --timeline > "$LOG" 2>&1 &
BENCH_PID=$!
until grep -q "^TIMELINE,1," "$LOG" 2>/dev/null; do sleep 0.2; done   # measured run has started

sleep $((KILL_AT - 1))
NODE2_PID=$(cut -d' ' -f2 "$PROJECT_DIR/logs/cluster.pids")
kill -9 "$NODE2_PID"
echo "t=${KILL_AT}s: killed node 2 (SIGKILL)"

sleep $((RESTART_AT - KILL_AT))
nohup java -jar "$JAR_FILE" --port 9002 --seeds localhost:9001,localhost:9003 \
    > "$PROJECT_DIR/logs/node2-restarted.log" 2>&1 &
RESTARTED_PID=$!
echo "t=${RESTART_AT}s: restarted node 2"

wait "$BENCH_PID"
grep -m1 "keys recovered" "$PROJECT_DIR/logs/node2-restarted.log" | sed 's/.*WAL enabled/node 2 WAL recovery:/' || true

python3 - "$LOG" "$OUT/failover.md" "$KILL_AT" "$RESTART_AT" <<'EOF'
import sys
log, out, kill_at, restart_at = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
lines = open(log).read().splitlines()
timeline = [tuple(map(int, l.split(",")[1:])) for l in lines if l.startswith("TIMELINE,")]
csv = next(l for l in lines if l.startswith("CSV,")).split(",")[1:]
def phase(lo, hi):
    ops = [o for s, o, e in timeline if lo < s <= hi]
    errs = sum(e for s, o, e in timeline if lo < s <= hi)
    return (sum(ops) / len(ops) if ops else 0), errs
healthy, healthy_err = phase(0, kill_at)
down, down_err = phase(kill_at + 1, restart_at)          # skip the second the kill lands in
back, back_err = phase(restart_at + 5, timeline[-1][0])  # allow time to restart and rejoin
md = ["# Failover benchmark", "",
      "16 clients (with failover), 3-node production cluster (QUORUM, auth, fsynced WAL).",
      f"Node 2 killed with SIGKILL at {kill_at}s, restarted at {restart_at}s.", "",
      "| Phase | Avg ops/sec | Errors |", "|---|---|---|",
      f"| All 3 nodes up (0–{kill_at}s) | {healthy:,.0f} | {healthy_err} |",
      f"| Node 2 down ({kill_at + 1}–{restart_at}s) | {down:,.0f} | {down_err} |",
      f"| Node 2 back ({restart_at + 5}s–end) | {back:,.0f} | {back_err} |", "",
      f"Total operations: {int(csv[8]):,}, errors: {csv[5]}, stale reads: {csv[7]}, read misses: {csv[6]}", "",
      "| Second | ops | errors |", "|---|---|---|"]
md += [f"| {s} | {o:,} | {e} |" for s, o, e in timeline]
open(out, "w").write("\n".join(md) + "\n")
print("\n".join(md[:12]))
EOF
