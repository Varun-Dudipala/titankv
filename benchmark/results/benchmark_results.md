# TitanKV Benchmark Results

## Environment

- 4 vCPU Linux VM (cloud), OpenJDK 21.0.10
- All nodes **and** the load generator run on the same machine and share its 4 CPUs, over loopback.
  A real deployment gives each node its own machine, so these numbers are a floor for CPU-bound
  workloads, while network latency is not included.
- fsync on this VM is fast (sub-millisecond); on spinning disks or slower SSDs durable-write
  throughput will be lower.
- Date: 2026-09-25, with all features (strict quorum, hinted handoff, anti-entropy, hybrid logical clocks)

## Method

`./scripts/benchmark-suite.sh` runs every configuration below **5 times for 8 seconds** each and
reports the median, the min–max range, and the spread (half the range as a share of the median).

- Each client thread has its own `TitanKVClient` and issues one request at a time (closed loop).
- Every fresh cluster first gets an unrecorded 5-second run, so the servers' JIT is warm.
- Each thread writes its 1,000-key space before measuring, so reads hit existing keys; "mixed" is
  80% reads and 20% writes of 100-byte values unless stated otherwise.
- **Reads are checked for correctness.** Each value carries the writing thread's sequence number,
  and a read that returns an older sequence than the thread's last acknowledged write counts as a
  stale read.
- Cluster runs use QUORUM reads and writes with replication factor 3, unless stated otherwise.

```bash
./scripts/benchmark-suite.sh        # everything below, about 20 minutes
./scripts/benchmark-failover.sh     # the node-failure run
```

Every run's numbers are in [`suite/summary.csv`](suite/summary.csv).

## Results

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

**Correctness:** across all 80 runs (14.6 million operations) there were **0 errors, 0 stale reads
and 0 read misses**.

How to read it:

- **Consistency levels** cost what theory predicts: ONE > QUORUM > ALL, since more replicas must
  answer before replying. The differences are small here because all replicas are on one machine.
- **Larger values** cost more: 10 KB values move 100 times the bytes of 100 B values.
- **Concurrency**: throughput grows from 1 to 16 clients and flattens at 64, where p50 latency
  rises instead. The 4 CPUs are saturated.
- **Cluster size**: 5 nodes do not beat 3 here, because every node and the load generator share
  the same 4 CPUs. Five JVMs split the same cores five ways. Horizontal scaling needs one machine
  per node; on one box this only shows that adding nodes costs little.
- **Production mode** adds authentication and an fsync on every replica before acknowledging.
  Writes-only drops to about 5.9K/sec, bounded by fsyncs even with group commit.

## Availability under node failure

`./scripts/benchmark-failover.sh`: 16 clients (failing over between nodes) on a 3-node
production-mode cluster for 40 seconds. Node 2 is killed with SIGKILL at 10s and restarted at 25s.

| Phase | Avg ops/sec | Errors |
|---|---|---|
| All 3 nodes up (0–10s) | 14,548 | 0 |
| Node 2 down (11–25s) | 17,365 | 0 |
| Node 2 restarting and warming up (26–40s) | 10,698 | 0 |

**538,683 operations with 0 errors, 0 stale reads and 0 read misses.** Node 2 recovered all 16,000
keys from its WAL, and anti-entropy sent it only the 1,045 keys written while it was down.

- **During the outage** QUORUM keeps working on the other two replicas. Throughput even rises,
  because two JVMs share the CPUs instead of three.
- **After the restart** there is a dip of about 10 seconds. The restarted JVM starts with a cold JIT
  and immediately takes a third of the traffic, plus hint delivery and repair.
- **This run found a client bug.** Throughput originally fell to about 2,200 ops/sec during the
  outage. The client slept its retry delay before failing over away from the dead node, and kept
  the node's circuit breaker open for 30 seconds. The client now fails over immediately, moves
  nodes with an open breaker to the back of the order, and re-probes them after 5 seconds. The same
  fix raised the chaos test from about 10K to about 147K acknowledged writes per run.

Per-second numbers are in [`suite/failover.md`](suite/failover.md).

## Profiling the replicated path

Why is one node about 3 times faster than three? Mostly because replication multiplies the work on
one shared machine. At QUORUM with RF 3, every write is applied on 3 replicas and every read touches
2, and each replica hop adds a network round trip and a context switch. On this 4-core box all
three JVMs and the load generator compete for the same cores, so about a third of single-node
throughput is the expected ceiling. On separate machines each node brings its own cores.

That ceiling was not being reached. Under load the 3-node cluster ran at 93% CPU with 41% in the
kernel. `strace -c` counted about 13.2 system calls per client operation, 8.2 of them `futex`
(threads waking each other up to hand work along). Four changes cut this to 8.7 system calls
per operation, 3.3 of them `futex`:

1. Reads contact only the replicas the consistency level needs, local one first and in-process.
2. The local replica's write runs on the calling thread when it does not wait for an fsync.
3. A response is written straight to the socket when nothing is queued ahead of it.
4. Node-to-node reads use `SO_TIMEOUT` instead of scheduling a timer per read.

Old and new builds alternated on the same machine (2 rounds × 5 runs each, medians):

| Scenario | Before | After | Change |
|---|---|---|---|
| 1 node, mixed | 38.5K | 65K | +69% |
| 3 nodes QUORUM, mixed | 12.2K | 19.4K | +59% |
| 3 nodes QUORUM, writes | 12.8K | 17.3K | +35% |
| 3 nodes production, mixed | 8.4K | 10.6K | +26% |
| 3 nodes production, writes | 3.3K | 4.0K | +21% |

All runs had 0 errors and 0 stale reads. Production-mode writes gain least because they wait for
fsyncs, not CPU.

Two more changes followed from the failover run, where 1–3 reads timed out just after the killed
node came back. Hint delivery and Merkle repair were sharing the connection pool with client
traffic. They now have their own pool, and a read that waits more than 50 ms on a replica also
asks a spare one (speculative retry). Repeated failover runs since then have had 0 errors.

## Effect of WAL group commit

Same 3-node production cluster, 16 clients, measured when group commit was introduced:

| Workload | fsync per write | Group commit |
|---|---|---|
| 100% writes | 2,788 ops/sec | 4,691 ops/sec (+68%) |
| 80% reads | 7,710 ops/sec | 12,171 ops/sec (+58%) |

## Before the concurrency fix

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
