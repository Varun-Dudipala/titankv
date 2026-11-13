#!/bin/bash

# Latency versus load on a 3-node cluster at QUORUM (80% reads, 100-byte values).
#
# 1. Closed loop: THREADS clients send as fast as they can, REPS times; the median throughput is
#    taken as the maximum.
# 2. Open loop: a fixed rate of 25%, 50%, 75% and 90% of that maximum, REPS times each, with
#    latency measured from each request's intended send time (no coordinated omission).
#    OPEN_THREADS clients share the rate, enough that the generator always has a client free.
#
#   ./scripts/benchmark-latency.sh                  # about 5 minutes
#   REPS=1 DURATION=10 ./scripts/benchmark-latency.sh
#
# Output: benchmark/results/suite/latency.md, latency.csv and raw/latency-*.log

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
REPS="${REPS:-3}"
DURATION="${DURATION:-15}"
THREADS="${THREADS:-16}"
OPEN_THREADS="${OPEN_THREADS:-64}"
LOADS="${LOADS:-25 50 75 90}"
OUT="$PROJECT_DIR/benchmark/results/suite"
RAW="$OUT/raw"
CSV="$OUT/latency.csv"
BENCH="$SCRIPT_DIR/run-benchmark.sh"
HOSTS=localhost:9001,localhost:9002,localhost:9003
COMMON=(--hosts "$HOSTS" --consistency QUORUM --read-ratio 0.8 --duration "$DURATION" --warmup 1000)

mkdir -p "$RAW"
rm -f "$RAW"/latency-*.log
echo "mode,load_pct,rep,target_ops_per_sec,achieved_ops_per_sec,p50_ms,p95_ms,p99_ms,p999_ms,max_ms,svc_p50_ms,svc_p99_ms,svc_p999_ms,svc_max_ms,errors,misses,stale,ops,unsent" > "$CSV"

(cd "$PROJECT_DIR" && mvn -q -B package -DskipTests)

cluster_down() {
    "$SCRIPT_DIR/stop-cluster.sh" > /dev/null
    rm -rf "$PROJECT_DIR/data"
}
cluster_down
trap cluster_down EXIT
NODES=3 "$SCRIPT_DIR/start-cluster.sh" > /dev/null
# Unrecorded run so the servers' JIT is warm before the first measurement
"$BENCH" --hosts "$HOSTS" --threads "$THREADS" --duration 5 --warmup 500 > /dev/null 2>&1

# run <log name> <load_pct> <rep> [LoadGenerator args...]: one run, appended to latency.csv
run() {
    local name=$1 load=$2 rep=$3
    shift 3
    local log="$RAW/latency-$name-$rep.log"
    "$BENCH" "${COMMON[@]}" "$@" > "$log" 2>&1
    local line
    line=$(grep '^LATENCY,' "$log" | cut -d, -f2-)
    local mode=${line%%,*}
    echo "$mode,$load,$rep,${line#*,}" >> "$CSV"
    echo "$line"
}

echo "== Closed loop, $THREADS clients"
for ((rep = 1; rep <= REPS; rep++)); do
    line=$(run closed 100 "$rep" --threads "$THREADS")
    echo "  rep $rep: $(echo "$line" | cut -d, -f3) ops/sec, p99 $(echo "$line" | cut -d, -f6) ms"
done
MAX=$(python3 -c "
import csv, statistics
rows = [r for r in csv.DictReader(open('$CSV')) if r['mode'] == 'closed']
print(round(statistics.median(float(r['achieved_ops_per_sec']) for r in rows)))")
echo "   median closed-loop throughput: $MAX ops/sec"

for load in $LOADS; do
    rate=$((MAX * load / 100))
    echo "== Open loop at $load% ($rate ops/sec), $OPEN_THREADS clients"
    for ((rep = 1; rep <= REPS; rep++)); do
        line=$(run "open-$load" "$load" "$rep" --threads "$OPEN_THREADS" --rate "$rate")
        echo "  rep $rep: achieved $(echo "$line" | cut -d, -f3) ops/sec, p99 $(echo "$line" | cut -d, -f6) ms," \
            "p99.9 $(echo "$line" | cut -d, -f7) ms"
    done
done
cluster_down

# Machine description for the report
if [ "$(uname -s)" = Darwin ]; then
    CPU="$(sysctl -n machdep.cpu.brand_string), $(sysctl -n hw.ncpu) CPUs, macOS $(sw_vers -productVersion)"
else
    CPU="$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | xargs), $(nproc) CPUs, $(uname -sr)"
fi
JDK=$(java -version 2>&1 | head -1)
COMMIT=$(git -C "$PROJECT_DIR" rev-parse --short HEAD 2>/dev/null || echo unknown)

python3 - "$CSV" "$OUT/latency.md" "$REPS" "$DURATION" "$THREADS" "$OPEN_THREADS" "$CPU" "$JDK" "$COMMIT" <<'EOF'
import csv, statistics, sys, collections, datetime
src, out, reps, duration, threads, open_threads, cpu, jdk, commit = sys.argv[1:]
rows = list(csv.DictReader(open(src)))
groups = collections.OrderedDict()
for r in rows:
    groups.setdefault((r["mode"], int(r["load_pct"])), []).append(r)
med = lambda rs, k: statistics.median(float(r[k]) for r in rs)
total = lambda rs, k: sum(int(r[k]) for r in rs)
lines = [
    "# Latency versus load", "",
    f"Machine: {cpu}; {jdk}. Commit {commit}, {datetime.date.today().isoformat()}.",
    "The 3 nodes and the load generator share this machine and talk over loopback.", "",
    f"3-node cluster, QUORUM reads and writes, RF 3, 80% reads, 100-byte values. {reps} runs of "
    f"{duration}s per row; each cell is the median across runs (errors, stale reads and unsent "
    f"requests are totals); the p99 range shows how much single runs vary. Closed loop uses "
    f"{threads} clients; open loop spreads the target rate over {open_threads} clients and measures latency from each request's intended send time.", "",
    "| Load | Mode | Target ops/sec | Achieved ops/sec | p50 ms | p95 ms | p99 ms | p99 ms, min – max "
    "| p99.9 ms | max ms | Service p99 ms | Errors | Stale reads | Unsent |",
    "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|",
]
for (mode, load), rs in sorted(groups.items(), key=lambda g: (g[0][0] == "closed", g[0][1])):
    label = "max (closed loop)" if mode == "closed" else f"{load}%"
    target = "–" if mode == "closed" else f"{med(rs, 'target_ops_per_sec'):,.0f}"
    lines.append(
        f"| {label} | {mode} | {target} | {med(rs, 'achieved_ops_per_sec'):,.0f} "
        f"| {med(rs, 'p50_ms'):.2f} | {med(rs, 'p95_ms'):.2f} | {med(rs, 'p99_ms'):.2f} "
        f"| {min(float(r['p99_ms']) for r in rs):.2f} – {max(float(r['p99_ms']) for r in rs):.2f} "
        f"| {med(rs, 'p999_ms'):.2f} | {med(rs, 'max_ms'):.2f} | {med(rs, 'svc_p99_ms'):.2f} "
        f"| {total(rs, 'errors')} | {total(rs, 'stale')} | {total(rs, 'unsent')} |")
lines += ["",
    "Open-loop latencies run from when a request was due to be sent to when its response arrived, so "
    "time spent queued behind a slow request counts. Service p99 is from the actual send, which is "
    "all a closed-loop run can see. Per-run numbers are in [`latency.csv`](latency.csv)."]
open(out, "w").write("\n".join(lines) + "\n")
print("\n".join(lines))
EOF
