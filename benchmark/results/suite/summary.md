# Benchmark suite: 5 runs of 8s per configuration

| Scenario | Setup | Clients | Workload | Median ops/sec | Min – max | Spread | p50 ms | p99 ms | Errors | Stale reads |
|---|---|---|---|---|---|---|---|---|---|---|
| single | 1 node | 16 | mixed | 153,818 | 151,759 – 154,861 | ±1% | 0.10 | 0.20 | 0 | 0 |
| single | 1 node | 16 | writes | 153,601 | 152,285 – 154,478 | ±1% | 0.10 | 0.20 | 0 | 0 |
| scaling | 3 nodes | 16 | mixed | 61,800 | 61,431 – 62,383 | ±1% | 0.24 | 0.54 | 0 | 0 |
| scaling | 5 nodes | 16 | mixed | 56,053 | 55,328 – 56,241 | ±1% | 0.26 | 0.73 | 0 | 0 |
| consistency | ONE | 16 | mixed | 96,427 | 94,655 – 96,961 | ±1% | 0.12 | 0.64 | 0 | 0 |
| consistency | QUORUM | 16 | mixed | 60,631 | 60,112 – 61,124 | ±1% | 0.25 | 0.55 | 0 | 0 |
| consistency | ALL | 16 | mixed | 45,303 | 45,171 – 45,641 | ±1% | 0.34 | 0.61 | 0 | 0 |
| value-size | 100 B | 16 | mixed | 61,028 | 60,138 – 61,265 | ±1% | 0.25 | 0.55 | 0 | 0 |
| value-size | 1000 B | 16 | mixed | 58,454 | 55,970 – 59,512 | ±3% | 0.25 | 0.61 | 0 | 0 |
| value-size | 10000 B | 16 | mixed | 47,251 | 45,846 – 48,609 | ±3% | 0.30 | 0.81 | 0 | 0 |
| concurrency | 3 nodes | 1 | writes | 18,236 | 17,896 – 18,292 | ±1% | 0.05 | 0.07 | 0 | 0 |
| concurrency | 3 nodes | 4 | writes | 34,829 | 34,387 – 35,239 | ±1% | 0.11 | 0.17 | 0 | 0 |
| concurrency | 3 nodes | 16 | writes | 45,582 | 45,219 – 46,712 | ±2% | 0.32 | 0.96 | 0 | 0 |
| concurrency | 3 nodes | 64 | writes | 51,663 | 50,993 – 52,180 | ±1% | 1.00 | 4.44 | 0 | 0 |
| production | 3 nodes prod | 16 | mixed | 3,271 | 2,826 – 3,757 | ±14% | 1.85 | 23.69 | 0 | 0 |
| production | 3 nodes prod | 16 | writes | 672 | 452 – 709 | ±19% | 22.79 | 45.45 | 0 | 0 |
