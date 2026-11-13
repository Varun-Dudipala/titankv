# Latency versus load

Machine: Apple M3, 8 CPUs, macOS 26.6.2; openjdk version "21.0.6" 2025-01-21 LTS. Commit cc7a36a, 2026-11-12.
The 3 nodes and the load generator share this machine and talk over loopback.

3-node cluster, QUORUM reads and writes, RF 3, 80% reads, 100-byte values. 3 runs of 15s per row; each cell is the median across runs (errors, stale reads and unsent requests are totals); the p99 range shows how much single runs vary. Closed loop uses 16 clients; open loop spreads the target rate over 64 clients and measures latency from each request's intended send time.

| Load | Mode | Target ops/sec | Achieved ops/sec | p50 ms | p95 ms | p99 ms | p99 ms, min – max | p99.9 ms | max ms | Service p99 ms | Errors | Stale reads | Unsent |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 25% | open | 13,754 | 13,754 | 0.08 | 0.11 | 0.23 | 0.20 – 0.33 | 2.98 | 10.09 | 0.20 | 0 | 0 | 0 |
| 50% | open | 27,509 | 27,507 | 0.10 | 0.41 | 1.42 | 0.46 – 29.04 | 3.80 | 7.08 | 1.15 | 0 | 0 | 0 |
| 75% | open | 41,263 | 41,254 | 0.54 | 3.12 | 9.47 | 5.28 – 14.16 | 43.30 | 124.18 | 2.85 | 0 | 0 | 0 |
| 90% | open | 49,516 | 49,470 | 1.24 | 6.57 | 15.21 | 13.65 – 319.18 | 77.18 | 128.55 | 2.87 | 0 | 0 | 0 |
| max (closed loop) | closed | – | 55,018 | 0.25 | 0.53 | 0.90 | 0.87 – 1.07 | 4.18 | 73.44 | 0.90 | 0 | 0 | 0 |

Open-loop latencies run from when a request was due to be sent to when its response arrived, so time spent queued behind a slow request counts. Service p99 is from the actual send, which is all a closed-loop run can see. Per-run numbers are in [`latency.csv`](latency.csv).
