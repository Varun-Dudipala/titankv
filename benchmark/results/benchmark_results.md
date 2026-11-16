# TitanKV Benchmark Results

## Environment

- Apple M3 laptop (8 CPUs, 16 GB), macOS 26.6.2, OpenJDK 21.0.6, on AC power.
- All nodes **and** the load generator run on the same machine and share its CPUs, over loopback.
  A real deployment gives each node its own machine, so these numbers are a floor for CPU-bound
  workloads, while network latency is not included.
- An fsync here (`FileChannel.force`) takes about 3 ms when nothing else is syncing. That bounds
  the production-mode numbers; see the notes under the table.
- Date: 2026-11-15, on the code that sends each request to a replica of its key
  (`2ad99ea`). Sections under [earlier measurements](#earlier-measurements) are older, from another
  machine, and say so.

A first attempt at this run was thrown away: the laptop was on battery and reached 7% halfway
through, and from that point throughput halved with spreads of ±40 to 60%. Everything below was
measured again on AC power; no row has a spread above ±3% except the two fsync-bound ones.

## Method

`./scripts/benchmark-suite.sh` runs every configuration below **5 times for 8 seconds** each and
reports the median, the min–max range, and the spread (half the range as a share of the median).

- Each client thread has its own `TitanKVClient` and issues one request at a time (closed loop,
  see below).
- Every fresh cluster first gets an unrecorded 5-second run, so the servers' JIT is warm.
- Each thread writes its 1,000-key space before measuring, so reads hit existing keys; "mixed" is
  80% reads and 20% writes of 100-byte values unless stated otherwise.
- **Reads are checked for correctness.** Each value carries the writing thread's sequence number,
  and a read that returns an older sequence than the thread's last acknowledged write counts as a
  stale read.
- Cluster runs use QUORUM reads and writes with replication factor 3, unless stated otherwise. The
  consistency scenario sets the level per request from the client (`--consistency`).

```bash
./scripts/benchmark-suite.sh        # everything below, about 20 minutes
./scripts/benchmark-failover.sh     # the node-failure run
```

Every run's numbers are in [`suite/summary.csv`](suite/summary.csv).

### Closed loop versus open loop

**Every load-test table in this file comes from closed-loop runs, except
[latency versus load](#latency-versus-load-open-loop).** In a closed loop each client sends its
next request only when the previous one returns. That is the right way to find maximum throughput,
but it understates latency. When a request stalls (a GC pause, a slow replica), the client stalls
with it and sends nothing; the requests that would have queued up behind the stall are never sent,
so they are never measured. This is *coordinated omission*: the generator slows down exactly when
the server does, and the p99 reflects only requests that got lucky. The closed-loop percentiles
below are therefore service times at saturation, not the latency a user would see at that load.

`LoadGenerator --rate <ops/sec>` runs an **open loop** instead. Requests follow a fixed schedule,
spread evenly across client threads, and each latency is measured from the time the request was
*due* to be sent, not when it actually went out. A thread that falls behind sends its overdue
requests at once and skips none, so the backlog shows up in the percentiles. The run also reports
the achieved rate (below the target means the system, or the generator, could not keep up) and
the service time from the actual send, for comparison with closed-loop numbers. Each thread still
has one request in flight, so `--threads` must be large enough that a thread is free when a
request is due.

`./scripts/benchmark-latency.sh` measures closed-loop maximum throughput on a 3-node QUORUM
cluster, then runs open loop at 25%, 50%, 75% and 90% of it. Its results are in
[latency versus load](#latency-versus-load-open-loop).

## Results (closed loop)

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

**Correctness:** across all 80 runs (37.9 million operations) there were **0 errors, 0 stale reads
and 0 read misses**.

How to read it:

- **Consistency levels** cost what theory predicts: ONE > QUORUM > ALL (96K, 61K, 45K). ONE is
  faster because a read is answered by the coordinator's own replica in-process, with no hop.
- **Larger values** cost more, but less than their size: 10 KB values run at 77% of the 100 B rate.
- **Concurrency**: write throughput grows from 18K with 1 client to 46K with 16 and 52K with 64,
  where p50 latency rises from 0.05 ms to 1.0 ms instead. The CPUs are saturated.
- **1 node vs 3 nodes**: one node does 2.5 times what three do. Replication triples the work, and
  all three share this machine. See [earlier measurements](#earlier-measurements) for a profile.
- **Cluster size**: 5 nodes run at 91% of 3 nodes here, because every node and the load generator
  share the same 8 CPUs. Horizontal scaling needs one machine per node; on one box this only shows
  that adding nodes costs little.
- **Production mode** adds authentication and an fsync on every replica before acknowledging.
  Writes fall to about 670 a second, and the mixed workload to about 3,300. These are bounded by
  the disk, not the code: three nodes share one drive, and each write waits for a sync on two of
  them. On the cloud VM used for the earlier measurements, where a sync returned in under a
  millisecond, the same scenario ran at 4,400 writes a second. These two rows also vary most from
  run to run (±14% and ±19%).

### Against the earlier build, on this machine

The numbers above cannot be compared with the older tables further down, which come from a
4-vCPU cloud VM. To see what the changes since then cost, the build from before them (`cc7a36a`)
and the current one were run alternately on this machine, 16 clients unless stated, 3 runs of 8
seconds each, two rounds for the first row:

| Scenario | Earlier build | Current | Change |
|---|---|---|---|
| 5 nodes, 80% reads | 56,585 ops/sec | 55,515 ops/sec | −2% |
| 3 nodes, 80% reads | 60,395 | 60,049 | −1% |
| 3 nodes, 64 clients, writes only | 51,108 | 51,532 | +1% |

All within run-to-run variation. The per-peer thread pools, client-chosen versions, the reply
that reports a superseded write, saved membership and catch-up cost nothing measurable.

**This comparison found a regression that no test had.** The first time it was run, the 5-node
row read 43,500 for the current build: 23% slower, with 3 nodes unchanged. A coordinator that is
a replica of the key answers from its own copy; one that is not has to ask the replicas. Clients
had been placing nodes on their ring by address and the cluster by resolved IP, which agree when
everything resolves alike, as on one machine. When the cluster moved to placing nodes by id, the
two rings stopped agreeing. On three nodes every node holds every key, so nothing changed; on
five, two requests in five had begun to land on a node that did not hold the key. The client now
asks each node for its id and builds the cluster's own ring. The same runs showed that 32 threads
per peer were 6% slower than 8 on five nodes, and the default was lowered.

## Latency versus load (open loop)

`./scripts/benchmark-latency.sh`: 3-node dev-mode cluster, QUORUM, 80% reads of 100-byte values.
Closed-loop maximum first (16 clients), then open loop at a fixed share of it with the rate spread
over 64 clients. 3 runs of 15 seconds per row, medians:

| Load | Achieved ops/sec | p50 ms | p99 ms | p99 ms, min – max of 3 runs | p99.9 ms | Service p99 ms |
|---|---|---|---|---|---|---|
| 25% (open) | 15,544 | 0.08 | 0.13 | 0.12 – 1.81 | 0.57 | 0.09 |
| 50% (open) | 31,087 | 0.10 | 0.33 | 0.19 – 0.38 | 1.85 | 0.28 |
| 75% (open) | 46,627 | 0.26 | 1.25 | 0.88 – 6.54 | 2.87 | 0.96 |
| 90% (open) | 55,950 | 0.58 | 3.07 | 2.65 – 143.69 | 27.50 | 1.95 |
| max (closed) | 62,176 | 0.24 | 0.56 | 0.54 – 0.60 | 1.10 | 0.56 |

Every open-loop run reached its target rate and had 0 errors, 0 stale reads and 0 read misses; so
did the closed-loop runs (all 15 runs: 9.5 million operations). Full table:
[`suite/latency.md`](suite/latency.md).

- **The closed-loop p99 is not the latency at that throughput.** At full speed, closed loop
  reports a p99 of 0.56 ms. Sending 90% of that rate on a fixed schedule gives a median p99 of
  3.07 ms, and one of the three runs reached 144 ms. In the closed loop, a stalled client simply
  stopped sending, so the requests that would have waited behind each stall were never measured.
- **Latency stays flat to about half of maximum throughput** (p99 0.33 ms at 50%), then rises:
  p50 goes from 0.10 ms at 50% to 0.58 ms at 90%.
- **Service time hides the queueing.** At 90% load the time from actual send to response has a
  p99 of 1.95 ms, while the latency from intended send time is 3.07 ms: requests spend the
  difference waiting for their client to finish an earlier slow request.
- **Single runs vary a lot at the tail.** One stall (GC, the OS scheduling a node's threads away
  on a shared laptop) dominates a 15-second run's p99 and p99.9, which is why the range is shown.
- Each of the 64 client threads keeps one request in flight, like a client with 64 connections;
  a stall on one connection delays the requests queued for that connection.

## Availability under node failure

`./scripts/benchmark-failover.sh`: 16 closed-loop clients (failing over between nodes) on a 3-node
production-mode cluster for 40 seconds. Node 2 is killed with SIGKILL at 10s and restarted at 25s.

| Phase | Avg ops/sec | Errors |
|---|---|---|
| All 3 nodes up (0–10s) | 3,407 | 0 |
| Node 2 down (11–25s) | 4,434 | 0 |
| Node 2 back (30s–end) | 2,280 | 0 |

**130,958 operations with 0 errors, 0 stale reads and 0 read misses** (the run before it: 115,368
operations, also 0 errors).
Node 2 recovered its keys from its WAL; hints and Merkle anti-entropy sent it the writes it missed.

- **During the outage** QUORUM keeps working on the other two replicas. Throughput even rises,
  because two nodes share the disk instead of three.
- **After the restart** throughput dips. The restarted JVM starts with a cold JIT and immediately
  takes a third of the traffic, while also receiving hints and Merkle repair.
- **This run found a startup bug.** An earlier run on this code had 1 error in 94,027 operations:
  one write refused with "Not enough replicas", one second after node 2 was restarted. The node's
  log showed the refusal 30 ms *before* its cluster manager had finished starting. The server
  accepts connections first, and for as long as resolving the seeds' host names took, a starting
  node counted as having no seeds and so as ready. The node now cannot report ready at any point
  while it starts, and the two runs above are from after that fix.
- **An earlier version of this run found a client bug.** Throughput originally fell sharply
  during the outage. The client slept its retry delay before failing over away from the dead
  node, and kept the node's circuit breaker open for 30 seconds. The client now fails over
  immediately, moves nodes with an open breaker to the back of the order, and re-probes them
  after 5 seconds.

Per-second numbers are in [`suite/failover.md`](suite/failover.md).

## Earlier measurements

Everything from here on was measured in October 2026 on a 4-vCPU Linux cloud VM (OpenJDK 21.0.10,
sub-millisecond fsync), on the code as it was then. The numbers are not comparable with the
tables above; the before-and-after comparisons within each section are, since each was made on
one machine.

### Profiling the replicated path

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

Old and new builds alternated on the same machine (closed loop, 2 rounds × 5 runs each, medians):

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

### Effect of WAL group commit

Same 3-node production cluster, 16 closed-loop clients, measured when group commit was introduced:

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
