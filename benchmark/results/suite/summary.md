# Benchmark suite: 5 runs of 8s per configuration

| Scenario | Setup | Clients | Workload | Median ops/sec | Min – max | Spread | p50 ms | p99 ms | Errors | Stale reads |
|---|---|---|---|---|---|---|---|---|---|---|
| single | 1 node | 16 | mixed | 70,106 | 68,705 – 70,814 | ±2% | 0.20 | 0.69 | 0 | 0 |
| single | 1 node | 16 | writes | 70,139 | 68,533 – 73,006 | ±3% | 0.20 | 0.66 | 0 | 0 |
| scaling | 3 nodes | 16 | mixed | 24,457 | 20,512 – 25,469 | ±10% | 0.58 | 2.28 | 0 | 0 |
| scaling | 5 nodes | 16 | mixed | 19,247 | 16,301 – 21,071 | ±12% | 0.67 | 3.60 | 0 | 0 |
| consistency | ONE | 16 | mixed | 45,029 | 44,020 – 48,146 | ±5% | 0.28 | 1.55 | 0 | 0 |
| consistency | QUORUM | 16 | mixed | 24,105 | 22,273 – 24,648 | ±5% | 0.60 | 2.29 | 0 | 0 |
| consistency | ALL | 16 | mixed | 19,070 | 17,960 – 19,296 | ±4% | 0.76 | 2.39 | 0 | 0 |
| value-size | 100 B | 16 | mixed | 25,129 | 23,599 – 25,614 | ±4% | 0.57 | 1.96 | 0 | 0 |
| value-size | 1000 B | 16 | mixed | 23,097 | 22,712 – 23,659 | ±2% | 0.61 | 2.23 | 0 | 0 |
| value-size | 10000 B | 16 | mixed | 16,469 | 11,509 – 17,207 | ±17% | 0.82 | 4.25 | 0 | 0 |
| concurrency | 3 nodes | 1 | writes | 3,705 | 3,548 – 3,830 | ±4% | 0.25 | 0.47 | 0 | 0 |
| concurrency | 3 nodes | 4 | writes | 12,045 | 11,929 – 12,745 | ±3% | 0.30 | 0.82 | 0 | 0 |
| concurrency | 3 nodes | 16 | writes | 21,068 | 20,192 – 23,123 | ±7% | 0.67 | 2.50 | 0 | 0 |
| concurrency | 3 nodes | 64 | writes | 28,223 | 22,252 – 29,791 | ±13% | 2.02 | 7.41 | 0 | 0 |
| production | 3 nodes prod | 16 | mixed | 12,763 | 12,051 – 13,157 | ±4% | 1.10 | 3.66 | 0 | 0 |
| production | 3 nodes prod | 16 | writes | 4,415 | 4,379 – 4,628 | ±3% | 3.37 | 8.05 | 0 | 0 |
