# TitanKV Benchmark Results

## Environment

- 4 vCPU Linux VM (cloud), OpenJDK 21.0.10
- All nodes **and** the load generator run on the same machine and share its 4 CPUs, over loopback.
  A real deployment gives each node its own machine, so these numbers are a floor for CPU-bound
  workloads, while network latency is not included.
- fsync on this VM is fast (sub-millisecond); on spinning disks or slower SSDs durable-write
  throughput will be lower.
- Date: 2026-09-25 (all features: strict quorum, hinted handoff, anti-entropy, hybrid logical clocks)

## Method

`./scripts/run-benchmark.sh` runs `benchmark/LoadGenerator.java`:

- Closed loop: each client thread has its own `TitanKVClient` and issues one request at a time.
- Each thread first writes its 1,000-key space, so every read hits an existing key (read misses are
  reported; all runs below had 0), then runs an unmeasured warmup of 2,000 ops.
- Latency is recorded per operation; percentiles come from all measured operations.
- 100-byte random values. Reads and writes use QUORUM on the cluster (replication factor 3).

Reproduce:

```bash
mvn package -DskipTests
NODES=1 ./scripts/start-cluster.sh
./scripts/run-benchmark.sh --hosts localhost:9001 --threads 16 --ops 12000
./scripts/stop-cluster.sh

./scripts/start-cluster.sh                                   # 3 nodes, dev mode
TITANKV_CLUSTER_SECRET=secret ./scripts/start-cluster.sh     # 3 nodes, production mode
./scripts/run-benchmark.sh --hosts localhost:9001,localhost:9002,localhost:9003 --threads 16 --ops 6000
```

## Results

| Setup | Clients | Workload | Throughput (ops/sec) | p50 / p99 latency | Errors |
|---|---|---|---|---|---|
| 1 node, in-memory | 16 | 80% reads | 58,624 | 0.24 / 0.49 ms | 0 |
| 1 node, in-memory | 16 | 100% writes | 53,389 | 0.26 / 0.55 ms | 0 |
| 3 nodes, QUORUM, in-memory | 1 | 100% writes | 3,541 | 0.22 / 2.38 ms | 0 |
| 3 nodes, QUORUM, in-memory | 16 | 80% reads | 19,535 | 0.67 / 3.04 ms | 0 |
| 3 nodes, QUORUM, in-memory | 16 | 100% writes | 21,306 | 0.64 / 2.16 ms | 0 |
| 3 nodes, QUORUM, in-memory | 64 | 80% reads | 21,720 | 2.47 / 7.81 ms | 0 |
| 3 nodes, QUORUM, auth + fsynced WAL | 1 | 100% writes | 1,348 | 0.60 / 4.26 ms | 0 |
| 3 nodes, QUORUM, auth + fsynced WAL | 16 | 80% reads | 12,973 | 0.97 / 4.84 ms | 0 |
| 3 nodes, QUORUM, auth + fsynced WAL | 16 | 100% writes | 5,708 | 2.55 / 6.78 ms | 0 |

"In-memory" is dev mode (no WAL, no authentication). In production mode every write is appended
to the WAL on each of the 3 replicas and acknowledged only after fsync.

### Effect of WAL group commit

Same 3-node production cluster, 16 clients, measured when group commit was introduced:

| Workload | fsync per write | Group commit |
|---|---|---|
| 100% writes | 2,788 ops/sec | 4,691 ops/sec (+68%) |
| 80% reads | 7,710 ops/sec | 12,171 ops/sec (+58%) |

### Before the concurrency fix

Before request handling was made non-blocking, worker threads waited on replica responses that
needed those same workers. On this machine a 3-node cluster at 80% QUORUM reads ran at 237 ops/sec
with errors from 4 clients, and did not finish with 8 or more clients.

## Protocol size vs JSON over HTTP

`./scripts/run-benchmark.sh --protocol` compares bytes on the wire for a full round trip (request
and response) against a minimal JSON-over-HTTP/1.1 API with base64-encoded values:

| Value size | PUT: binary / JSON-HTTP | GET: binary / JSON-HTTP | Binary saves (PUT, GET) |
|---|---|---|---|
| 10 B | 74 / 200 | 74 / 181 | 63%, 59% |
| 100 B | 164 / 321 | 164 / 302 | 49%, 46% |
| 1,000 B | 1,064 / 1,522 | 1,064 / 1,503 | 30%, 29% |
| 10,000 B | 10,064 / 13,523 | 10,064 / 13,504 | 26%, 25% |

The fixed binary header is 29 bytes per request and 25 per response. Savings shrink as values grow
because base64's 33% expansion becomes the dominant JSON overhead.
