# Benchmark suite: 5 runs of 8s per configuration

| Scenario | Setup | Clients | Workload | Median ops/sec | Min – max | Spread | p50 ms | p99 ms | Errors | Stale reads |
|---|---|---|---|---|---|---|---|---|---|---|
| single | 1 node | 16 | mixed | 61,103 | 59,381 – 61,312 | ±2% | 0.25 | 0.53 | 0 | 0 |
| single | 1 node | 16 | writes | 59,194 | 58,191 – 60,211 | ±2% | 0.25 | 0.53 | 0 | 0 |
| scaling | 3 nodes | 16 | mixed | 22,560 | 21,048 – 22,956 | ±4% | 0.63 | 2.15 | 0 | 0 |
| scaling | 5 nodes | 16 | mixed | 21,057 | 17,197 – 21,595 | ±10% | 0.67 | 2.49 | 0 | 0 |
| consistency | ONE | 16 | mixed | 23,822 | 21,564 – 24,723 | ±7% | 0.54 | 2.53 | 0 | 0 |
| consistency | QUORUM | 16 | mixed | 22,134 | 20,358 – 22,928 | ±6% | 0.63 | 2.25 | 0 | 0 |
| consistency | ALL | 16 | mixed | 20,748 | 19,427 – 21,209 | ±4% | 0.70 | 2.15 | 0 | 0 |
| value-size | 100 B | 16 | mixed | 22,420 | 18,835 – 23,067 | ±9% | 0.63 | 2.13 | 0 | 0 |
| value-size | 1000 B | 16 | mixed | 20,604 | 19,479 – 21,499 | ±5% | 0.67 | 2.46 | 0 | 0 |
| value-size | 10000 B | 16 | mixed | 14,576 | 10,955 – 15,500 | ±16% | 0.90 | 4.95 | 0 | 0 |
| concurrency | 3 nodes | 1 | writes | 4,499 | 4,443 – 4,543 | ±1% | 0.21 | 0.38 | 0 | 0 |
| concurrency | 3 nodes | 4 | writes | 11,984 | 11,622 – 12,158 | ±2% | 0.31 | 0.74 | 0 | 0 |
| concurrency | 3 nodes | 16 | writes | 22,494 | 19,625 – 23,419 | ±8% | 0.63 | 2.31 | 0 | 0 |
| concurrency | 3 nodes | 64 | writes | 23,079 | 16,689 – 23,875 | ±16% | 2.36 | 8.17 | 0 | 0 |
| production | 3 nodes prod | 16 | mixed | 15,557 | 13,484 – 15,616 | ±7% | 0.88 | 3.47 | 0 | 0 |
| production | 3 nodes prod | 16 | writes | 5,864 | 5,364 – 6,037 | ±6% | 2.49 | 6.63 | 0 | 0 |
