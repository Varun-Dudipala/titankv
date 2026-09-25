# Benchmark suite: 5 runs of 8s per configuration

| Scenario | Setup | Clients | Workload | Median ops/sec | Min – max | Spread | p50 ms | p99 ms | Errors | Stale reads |
|---|---|---|---|---|---|---|---|---|---|---|
| single | 1 node | 16 | mixed | 61,973 | 61,009 – 67,475 | ±5% | 0.22 | 0.91 | 0 | 0 |
| single | 1 node | 16 | writes | 65,367 | 63,530 – 66,589 | ±2% | 0.22 | 0.76 | 0 | 0 |
| scaling | 3 nodes | 16 | mixed | 19,101 | 17,076 – 20,387 | ±9% | 0.72 | 2.73 | 0 | 0 |
| scaling | 5 nodes | 16 | mixed | 15,982 | 14,038 – 17,215 | ±10% | 0.83 | 4.32 | 0 | 0 |
| consistency | ONE | 16 | mixed | 39,872 | 36,591 – 40,672 | ±5% | 0.33 | 1.61 | 0 | 0 |
| consistency | QUORUM | 16 | mixed | 19,310 | 14,290 – 21,325 | ±18% | 0.71 | 2.43 | 0 | 0 |
| consistency | ALL | 16 | mixed | 14,732 | 13,630 – 16,439 | ±10% | 0.96 | 3.52 | 0 | 0 |
| value-size | 100 B | 16 | mixed | 18,780 | 15,343 – 20,383 | ±13% | 0.76 | 2.80 | 0 | 0 |
| value-size | 1000 B | 16 | mixed | 18,629 | 12,934 – 19,547 | ±18% | 0.76 | 2.66 | 0 | 0 |
| value-size | 10000 B | 16 | mixed | 12,142 | 6,975 – 12,799 | ±24% | 1.08 | 6.19 | 0 | 0 |
| concurrency | 3 nodes | 1 | writes | 3,408 | 2,268 – 3,516 | ±18% | 0.27 | 0.49 | 0 | 0 |
| concurrency | 3 nodes | 4 | writes | 10,511 | 10,418 – 10,666 | ±1% | 0.35 | 0.94 | 0 | 0 |
| concurrency | 3 nodes | 16 | writes | 16,914 | 10,739 – 17,424 | ±20% | 0.86 | 3.04 | 0 | 0 |
| concurrency | 3 nodes | 64 | writes | 16,690 | 15,269 – 17,370 | ±6% | 3.27 | 12.44 | 0 | 0 |
| production | 3 nodes prod | 16 | mixed | 10,302 | 8,784 – 10,530 | ±8% | 1.37 | 4.51 | 0 | 0 |
| production | 3 nodes prod | 16 | writes | 4,207 | 3,599 – 4,402 | ±10% | 3.58 | 8.61 | 0 | 0 |
