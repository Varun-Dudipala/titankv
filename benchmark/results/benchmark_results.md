# TitanKV Benchmark Results

## Environment

- Apple M3 laptop (8 CPUs, 16 GB), macOS 26.6.2, OpenJDK 21.0.6, on AC power.
- All nodes **and** the load generator run on the same machine and share its CPUs, over loopback.
  A real deployment gives each node its own machine, so these numbers are a floor for CPU-bound
  workloads, while network latency is not included.
- An fsync here (`FileChannel.force`) takes about 3 ms when nothing else is syncing, and the
  three nodes share one drive. That sets the latency of a durable write, not its throughput;
  see the notes under the table.
- Date: 2026-11-16, on the code that lets one fsync cover any number of writes and sends queued
  replica writes together (`d18a803`). Sections under [earlier measurements](#earlier-measurements)
  are older, from another machine, and say so.

Runs on battery are not used. One earlier attempt reached 7% battery halfway through, and from
that point throughput halved with spreads of ±40 to 60%; another, at 40%, came out 2 to 8% lower
than on AC power. The closed-loop table below was measured on AC power. The latency and failover
runs, and the old-against-new comparisons, were made on battery at about 40% and say so.

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

**Correctness:** across all 90 runs (40.2 million operations) there were **0 errors, 0 stale reads
and 0 read misses**.

How to read it:

- **Consistency levels** cost what theory predicts: ONE > QUORUM > ALL (96K, 61K, 45K). ONE is
  faster because a read is answered by the coordinator's own replica in-process, with no hop.
- **Larger values** cost more, but less than their size: 10 KB values run at 77% of the 100 B rate.
- **Concurrency**: write throughput grows from 18K with 1 client to 55K with 16 and 84K with 64.
  Past 16 clients the gain comes from batching: writes for a peer that arrive while its senders
  are busy go out in one request (see below).
- **1 node vs 3 nodes**: one node does 2.5 times what three do. Replication triples the work, and
  all three share this machine. See [earlier measurements](#earlier-measurements) for a profile.
- **Cluster size**: 5 nodes run at 90% of 3 nodes here, because every node and the load generator
  share the same 8 CPUs. Horizontal scaling needs one machine per node; on one box this only shows
  that adding nodes costs little.
- **Production mode** adds authentication and an fsync on every replica before acknowledging.
  A write then takes 15 to 35 ms, because it waits for a sync on two of three nodes that share
  one drive. Throughput depends on how many writes share each sync: 1,062 a second with 16
  clients, 2,616 with 64 and 6,836 with 256, at a p99 of 29, 52 and 91 ms. The mixed workload
  runs at 8,627 a second.

### Durable writes: what was limiting them

Until this version the production rows read 672 writes a second with 16 clients, and this file
said they were "bounded by the disk, not the code". That was wrong, and measuring with more
clients showed it: the old build did *fewer* writes a second with 64 clients than with 16.

A write waiting for the log to be synced held a thread: a worker on the replica, a pool thread
on the coordinator, and one of 8 blocking calls per peer. So one fsync could cover at most about
8 writes, whatever the load. Thread dumps under load showed every worker parked on the sync lock.

Two changes removed the limit. A write is now applied and logged by the thread handling it,
which moves on; one thread fsyncs the log and each fsync releases every write logged before it
started. And writes for a peer that queue up while its senders are busy are sent as one request,
which the replica makes durable with one fsync.

Old and new builds, run one after the other on this machine (3 nodes, QUORUM, auth, fsynced WAL,
writes only, 8-second runs, median of 3; 2 runs for the old build at 256 clients):

| Clients | Before | After | p50 before | p50 after |
|---|---|---|---|---|
| 16 | 654 writes/sec | 967 | 23 ms | 16 ms |
| 64 | 292 | 2,642 | 180 ms | 23 ms |
| 256 | 295 | 9,208 | 804 ms | 25 ms |

The same change helps without a WAL, where a peer's senders are busy for CPU reasons instead:

| Scenario (in-memory, QUORUM) | Before | After | Change |
|---|---|---|---|
| 3 nodes, 16 clients, writes only | 46,944 ops/sec | 57,666 | +23% |
| 3 nodes, 64 clients, writes only | 51,751 | 96,796 | +87% |
| 3 nodes, 16 clients, 80% reads | 60,549 | 61,717 | +2% |
| 5 nodes, 16 clients, 80% reads | 52,627 | 52,988 | +1% |
| 1 node, 16 clients, writes only | 147,039 | 148,729 | +1% |

Both comparisons were run on battery at about 45% charge, which costs a few percent against the
table above; the two builds were measured under the same conditions. All runs had 0 errors.

On the cloud VM used for the earlier measurements, where a sync returned in under a millisecond,
the old build ran at 4,400 writes a second with 16 clients. That figure has not been re-measured.

### Against the earlier build, on this machine

The numbers above cannot be compared with the older tables further down, which come from a
4-vCPU cloud VM. To see what the changes since then cost, the build from before them (`cc7a36a`)
and the build of 2026-11-15 were run alternately on this machine, 16 clients unless stated, 3
runs of 8 seconds each, two rounds for the first row. (This was before the write batching
described above, which is why the last row differs from today's table.)

| Scenario | Earlier build | Build of 2026-11-15 | Change |
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
| 25% (open) | 14,221 | 0.08 | 0.19 | 0.18 – 1.68 | 0.71 | 0.15 |
| 50% (open) | 28,440 | 0.15 | 0.35 | 0.28 – 0.59 | 2.03 | 0.30 |
| 75% (open) | 42,657 | 0.24 | 0.84 | 0.80 – 1.01 | 2.28 | 0.74 |
| 90% (open) | 51,189 | 0.42 | 1.74 | 1.30 – 2.13 | 5.23 | 1.38 |
| max (closed) | 56,885 | 0.25 | 0.82 | 0.73 – 0.83 | 2.98 | 0.82 |

Every open-loop run reached its target rate and had 0 errors, 0 stale reads and 0 read misses; so
did the closed-loop runs (all 15 runs: 8.7 million operations). Full table:
[`suite/latency.md`](suite/latency.md). Measured on battery, which is why the maximum is below
the 61,091 in the closed-loop table.

- **The closed-loop p99 is not the latency at that throughput.** At full speed, closed loop
  reports a p99 of 0.82 ms. Sending 90% of that rate on a fixed schedule gives a median p99 of
  1.74 ms and a worst request of 106 ms. In the closed loop, a stalled client simply stopped
  sending, so the requests that would have waited behind each stall were never measured.
- **Latency stays flat to about half of maximum throughput** (p99 0.35 ms at 50%), then rises:
  p50 goes from 0.15 ms at 50% to 0.42 ms at 90%.
- **Service time hides the queueing.** At 90% load the time from actual send to response has a
  p99 of 1.38 ms, while the latency from intended send time is 1.74 ms: requests spend the
  difference waiting for their client to finish an earlier slow request.
- **Single runs vary a lot at the tail, and so do whole sessions.** One stall (GC, the OS
  scheduling a node's threads away on a shared laptop) dominates a 15-second run's p99 and
  p99.9, which is why the range is shown. The same script on the same code half an hour
  earlier gave a p99 at 90% load of 28 ms (9 to 78 ms across its three runs), while the
  machine's power source was being changed; the build before this one, run just before the
  table above, gave 5.70 ms (3.19 to 24.21). Treat the 90% row as "a few milliseconds, with
  occasional runs far worse", not as a number.
- Each of the 64 client threads keeps one request in flight, like a client with 64 connections;
  a stall on one connection delays the requests queued for that connection.

## Availability under node failure

`./scripts/benchmark-failover.sh`: 16 closed-loop clients (failing over between nodes) on a 3-node
production-mode cluster for 40 seconds. Node 2 is killed with SIGKILL at 10s and restarted at 25s.

| Phase | Avg ops/sec | Errors |
|---|---|---|
| All 3 nodes up (0–10s) | 4,488 | 0 |
| Node 2 down (11–25s) | 4,983 | 0 |
| Node 2 back (30s–end) | 2,826 | 0 |

**158,712 operations with 0 errors, 0 stale reads and 0 read misses.**
Node 2 recovered its keys from its WAL; hints and Merkle anti-entropy sent it the writes it missed.

- **During the outage** QUORUM keeps working on the other two replicas. Throughput even rises,
  because two nodes share the disk instead of three.
- Throughput in every phase is about a third higher than before writes stopped holding threads
  through an fsync (3,407, 4,434 and 2,280 on the previous build).
- **After the restart** throughput dips. The restarted JVM starts with a cold JIT and immediately
  takes a third of the traffic, while also receiving hints and Merkle repair.
- **This run found a startup bug.** An earlier run on this code had 1 error in 94,027 operations:
  one write refused with "Not enough replicas", one second after node 2 was restarted. The node's
  log showed the refusal 30 ms *before* its cluster manager had finished starting. The server
  accepts connections first, and for as long as resolving the seeds' host names took, a starting
  node counted as having no seeds and so as ready. The node now cannot report ready at any point
  while it starts, and the run above is from after that fix.
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
