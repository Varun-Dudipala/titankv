# Latency versus load

Machine: Apple M3, 8 CPUs, macOS 26.6.2; openjdk version "21.0.6" 2025-01-21 LTS. Code at `2ad99ea`, 2026-11-15.
The 3 nodes and the load generator share this machine and talk over loopback.

3-node cluster, QUORUM reads and writes, RF 3, 80% reads, 100-byte values. 3 runs of 15s per row; each cell is the median across runs (errors, stale reads and unsent requests are totals); the p99 range shows how much single runs vary. Closed loop uses 16 clients; open loop spreads the target rate over 64 clients and measures latency from each request's intended send time.

| Load | Mode | Target ops/sec | Achieved ops/sec | p50 ms | p95 ms | p99 ms | p99 ms, min – max | p99.9 ms | max ms | Service p99 ms | Errors | Stale reads | Unsent |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 25% | open | 15,544 | 15,544 | 0.08 | 0.11 | 0.13 | 0.12 – 1.81 | 0.57 | 2.21 | 0.09 | 0 | 0 | 0 |
| 50% | open | 31,088 | 31,087 | 0.10 | 0.16 | 0.33 | 0.19 – 0.38 | 1.85 | 8.58 | 0.28 | 0 | 0 | 0 |
| 75% | open | 46,632 | 46,627 | 0.26 | 0.54 | 1.25 | 0.88 – 6.54 | 2.87 | 7.03 | 0.96 | 0 | 0 | 0 |
| 90% | open | 55,958 | 55,950 | 0.58 | 1.70 | 3.07 | 2.65 – 143.69 | 27.50 | 73.53 | 1.95 | 0 | 0 | 0 |
| max (closed loop) | closed | – | 62,176 | 0.24 | 0.43 | 0.56 | 0.54 – 0.60 | 1.10 | 93.80 | 0.56 | 0 | 0 | 0 |

Open-loop latencies run from when a request was due to be sent to when its response arrived, so time spent queued behind a slow request counts. Service p99 is from the actual send, which is all a closed-loop run can see. Per-run numbers are in [`latency.csv`](latency.csv).
