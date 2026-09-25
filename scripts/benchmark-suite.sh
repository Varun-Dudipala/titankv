#!/bin/bash

# Runs the full benchmark matrix, each configuration REPS times (default 5), and writes a summary
# with the median, min and max throughput plus latency and correctness counters:
#
#   ./scripts/benchmark-suite.sh            # all scenarios, about 20 minutes
#   REPS=3 DURATION=3 ./scripts/benchmark-suite.sh
#
# Output: benchmark/results/suite/summary.md, summary.csv and raw/*.log

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
REPS="${REPS:-5}"
DURATION="${DURATION:-8}"
OUT="$PROJECT_DIR/benchmark/results/suite"
RAW="$OUT/raw"
BENCH="$SCRIPT_DIR/run-benchmark.sh"

mkdir -p "$RAW"
rm -f "$RAW"/*.log
echo "scenario,setup,clients,workload,rep,ops_per_sec,p50_ms,p95_ms,p99_ms,max_ms,errors,misses,stale,ops" > "$OUT/summary.csv"

(cd "$PROJECT_DIR" && mvn -q -B package -DskipTests)

hosts() {
    local list=""
    for ((i = 0; i < $1; i++)); do
        list="${list:+$list,}localhost:$((9001 + i))"
    done
    echo "$list"
}

cluster_up() {   # cluster_up <nodes> [extra env assignments...]
    local nodes=$1
    shift
    "$SCRIPT_DIR/stop-cluster.sh" > /dev/null
    rm -rf "$PROJECT_DIR/data"
    env "$@" NODES="$nodes" "$SCRIPT_DIR/start-cluster.sh" > /dev/null
    # Unrecorded run so the servers' JIT is warm before the first measurement
    "$BENCH" --hosts "$(hosts "$nodes")" --threads 16 --duration 5 --warmup 500 > /dev/null 2>&1
}

cluster_down() {
    "$SCRIPT_DIR/stop-cluster.sh" > /dev/null
    rm -rf "$PROJECT_DIR/data"
}
trap cluster_down EXIT

# measure <scenario> <setup> <nodes> <clients> <workload> [extra LoadGenerator args...]
measure() {
    local scenario=$1 setup=$2 nodes=$3 clients=$4 workload=$5
    shift 5
    local ratio=0.8
    [ "$workload" = "writes" ] && ratio=0
    for ((rep = 1; rep <= REPS; rep++)); do
        local log="$RAW/${scenario}-${setup// /_}-${clients}c-${workload}-${rep}.log"
        "$BENCH" --hosts "$(hosts "$nodes")" --threads "$clients" --duration "$DURATION" \
            --warmup 1000 --read-ratio "$ratio" "$@" > "$log" 2>&1
        local csv
        csv=$(grep '^CSV,' "$log" | cut -d, -f2-)
        echo "$scenario,$setup,$clients,$workload,$rep,$csv" >> "$OUT/summary.csv"
        echo "  $scenario | $setup | $clients clients | $workload | rep $rep: $(echo "$csv" | cut -d, -f1) ops/sec"
    done
}

echo "== 1. Single node"
cluster_up 1
measure single "1 node" 1 16 mixed
measure single "1 node" 1 16 writes

echo "== 2. Cluster size (QUORUM)"
cluster_up 3
measure scaling "3 nodes" 3 16 mixed
cluster_up 5
measure scaling "5 nodes" 5 16 mixed

echo "== 3. Consistency level (3 nodes)"
for level in ONE QUORUM ALL; do
    cluster_up 3 TITANKV_READ_CONSISTENCY=$level TITANKV_WRITE_CONSISTENCY=$level
    measure consistency "$level" 3 16 mixed
done

echo "== 4. Value size (3 nodes, QUORUM)"
cluster_up 3
for size in 100 1000 10000; do
    measure value-size "${size} B" 3 16 mixed --value-size "$size"
done

echo "== 5. Concurrency (3 nodes, QUORUM, writes)"
for clients in 1 4 16 64; do
    measure concurrency "3 nodes" 3 "$clients" writes
done

echo "== 6. Production mode (auth + fsynced WAL, 3 nodes, QUORUM)"
cluster_up 3 TITANKV_CLUSTER_SECRET=benchmark-secret
measure production "3 nodes prod" 3 16 mixed
measure production "3 nodes prod" 3 16 writes
cluster_down

# Summary: median / min / max throughput and median latencies per configuration
python3 - "$OUT" "$REPS" "$DURATION" <<'EOF'
import csv, statistics, sys, collections
out, reps, duration = sys.argv[1], sys.argv[2], sys.argv[3]
rows = list(csv.DictReader(open(f"{out}/summary.csv")))
groups = collections.OrderedDict()
for r in rows:
    groups.setdefault((r["scenario"], r["setup"], r["clients"], r["workload"]), []).append(r)
lines = [f"# Benchmark suite: {reps} runs of {duration}s per configuration", "",
         "| Scenario | Setup | Clients | Workload | Median ops/sec | Min – max | Spread | p50 ms | p99 ms | Errors | Stale reads |",
         "|---|---|---|---|---|---|---|---|---|---|---|"]
for (scenario, setup, clients, workload), rs in groups.items():
    tp = [float(r["ops_per_sec"]) for r in rs]
    med = statistics.median(tp)
    spread = (max(tp) - min(tp)) / med * 100 if med else 0
    p50 = statistics.median(float(r["p50_ms"]) for r in rs)
    p99 = statistics.median(float(r["p99_ms"]) for r in rs)
    errors = sum(int(r["errors"]) for r in rs)
    stale = sum(int(r["stale"]) for r in rs)
    lines.append(f"| {scenario} | {setup} | {clients} | {workload} | {med:,.0f} | {min(tp):,.0f} – {max(tp):,.0f} "
                 f"| ±{spread / 2:.0f}% | {p50:.2f} | {p99:.2f} | {errors} | {stale} |")
open(f"{out}/summary.md", "w").write("\n".join(lines) + "\n")
print("\n".join(lines))
EOF
