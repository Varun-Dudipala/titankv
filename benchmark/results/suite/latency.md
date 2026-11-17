# Latency versus load

Machine: Apple M3, 8 CPUs, macOS 26.6.2; openjdk version "21.0.6" 2025-01-21 LTS. Commit d18a803, 2026-11-16.
The 3 nodes and the load generator share this machine and talk over loopback.

3-node cluster, QUORUM reads and writes, RF 3, 80% reads, 100-byte values. 3 runs of 15s per row; each cell is the median across runs (errors, stale reads and unsent requests are totals); the p99 range shows how much single runs vary. Closed loop uses 16 clients; open loop spreads the target rate over 64 clients and measures latency from each request's intended send time.

| Load | Mode | Target ops/sec | Achieved ops/sec | p50 ms | p95 ms | p99 ms | p99 ms, min – max | p99.9 ms | max ms | Service p99 ms | Errors | Stale reads | Unsent |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 25% | open | 14,221 | 14,221 | 0.08 | 0.14 | 0.19 | 0.18 – 1.68 | 0.71 | 3.15 | 0.15 | 0 | 0 | 0 |
| 50% | open | 28,442 | 28,440 | 0.15 | 0.22 | 0.35 | 0.28 – 0.59 | 2.03 | 4.62 | 0.30 | 0 | 0 | 0 |
| 75% | open | 42,663 | 42,657 | 0.24 | 0.48 | 0.84 | 0.80 – 1.01 | 2.28 | 9.28 | 0.74 | 0 | 0 | 0 |
| 90% | open | 51,196 | 51,189 | 0.42 | 1.02 | 1.74 | 1.30 – 2.13 | 5.23 | 106.36 | 1.38 | 0 | 0 | 0 |
| max (closed loop) | closed | – | 56,885 | 0.25 | 0.50 | 0.82 | 0.73 – 0.83 | 2.98 | 87.68 | 0.82 | 0 | 0 | 0 |

Open-loop latencies run from when a request was due to be sent to when its response arrived, so time spent queued behind a slow request counts. Service p99 is from the actual send, which is all a closed-loop run can see. Per-run numbers are in [`latency.csv`](latency.csv).
