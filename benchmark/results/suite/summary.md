# Benchmark suite: 5 runs of 8s per configuration

| Scenario | Setup | Clients | Workload | Median ops/sec | Min – max | Spread | p50 ms | p99 ms | Errors | Stale reads |
|---|---|---|---|---|---|---|---|---|---|---|
| single | 1 node | 16 | mixed | 152,672 | 151,361 – 153,409 | ±1% | 0.10 | 0.22 | 0 | 0 |
| single | 1 node | 16 | writes | 152,737 | 152,382 – 153,672 | ±0% | 0.10 | 0.22 | 0 | 0 |
| scaling | 3 nodes | 16 | mixed | 61,091 | 60,765 – 61,513 | ±1% | 0.24 | 0.65 | 0 | 0 |
| scaling | 5 nodes | 16 | mixed | 54,914 | 54,475 – 55,520 | ±1% | 0.22 | 0.98 | 0 | 0 |
| consistency | ONE | 16 | mixed | 97,127 | 94,796 – 97,600 | ±1% | 0.10 | 0.85 | 0 | 0 |
| consistency | QUORUM | 16 | mixed | 60,796 | 60,010 – 61,053 | ±1% | 0.24 | 0.60 | 0 | 0 |
| consistency | ALL | 16 | mixed | 44,945 | 44,914 – 45,106 | ±0% | 0.34 | 0.64 | 0 | 0 |
| value-size | 100 B | 16 | mixed | 60,413 | 59,701 – 60,914 | ±1% | 0.24 | 0.61 | 0 | 0 |
| value-size | 1000 B | 16 | mixed | 58,971 | 58,244 – 59,180 | ±1% | 0.24 | 0.67 | 0 | 0 |
| value-size | 10000 B | 16 | mixed | 46,740 | 45,827 – 47,058 | ±1% | 0.28 | 1.29 | 0 | 0 |
| concurrency | 3 nodes | 1 | writes | 18,044 | 17,906 – 18,090 | ±1% | 0.06 | 0.07 | 0 | 0 |
| concurrency | 3 nodes | 4 | writes | 37,191 | 36,528 – 37,735 | ±2% | 0.11 | 0.16 | 0 | 0 |
| concurrency | 3 nodes | 16 | writes | 55,415 | 51,724 – 56,145 | ±4% | 0.25 | 0.92 | 0 | 0 |
| concurrency | 3 nodes | 64 | writes | 84,325 | 80,175 – 91,853 | ±7% | 0.62 | 3.02 | 0 | 0 |
| production | 3 nodes prod | 16 | mixed | 8,627 | 8,128 – 9,031 | ±5% | 0.17 | 13.40 | 0 | 0 |
| production | 3 nodes prod | 16 | writes | 1,062 | 976 – 1,492 | ±24% | 14.66 | 29.09 | 0 | 0 |
| production | 3 nodes prod | 64 | writes | 2,616 | 2,452 – 2,689 | ±5% | 23.14 | 51.99 | 0 | 0 |
| production | 3 nodes prod | 256 | writes | 6,836 | 6,812 – 7,094 | ±2% | 33.81 | 90.91 | 0 | 0 |
