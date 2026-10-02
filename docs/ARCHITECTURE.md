# TitanKV Architecture

TitanKV is a leaderless, Dynamo-style key-value store. Every node runs the same components and any
node can serve any request.

```
┌──────────────────────────────── TitanKV node ────────────────────────────────┐
│                                                                              │
│  TcpServer (NIO selector) ──► ConnectionHandler ──► ReplicationManager       │
│        binary protocol          per-connection       coordinates replicas    │
│                                 command queue         ├─ ReadRepairHandler   │
│                                                       ├─ HintedHandoff       │
│                                                       └─ AntiEntropy         │
│                                                                              │
│  ClusterManager ◄── GossipProtocol (UDP)          InMemoryStore: map + WAL   │
│   membership, failure detection,                  (the local replica)        │
│   ConsistentHash ring, hybrid logical clock                                  │
│                                                                              │
│  DataRebalancer (streams keys to joining nodes)                              │
│  MetricsHttpServer (/metrics /health /ready /status)                         │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Data placement

`ConsistentHash` places 150 virtual nodes per physical node on a 64-bit ring (MurmurHash3 of
`host:port#i` in a `ConcurrentSkipListMap`). A key's **replicas** are the first 3 distinct nodes
clockwise from the key's hash. Adding or removing a node moves about 1/N of the keys, and virtual
nodes spread that movement across all nodes.

**Strict replica sets.** A node that is down, crashed or shut down, stays on the ring, so a key's
replica set only changes when a node joins or an operator removes one. Requests never substitute
another node for a down replica (a "sloppy quorum"). This is what makes the quorum overlap
guarantee hold during failures. An earlier version used sloppy quorums, and the chaos test caught it
losing acknowledged writes: after a node recovered, a read quorum of the original replicas could miss
writes that had gone to a stand-in node.

## Request path

Clients send any request to any node. The receiving node **coordinates** it; there is no primary
and no redirect. The client library hashes keys to pick a node only to spread load, and fails over
to another node if one is down.

### Write (PUT / DELETE)

1. The coordinator assigns a version from its hybrid logical clock, after observing the client's
   causal context (see below). A PUT with TTL also gets an absolute expiry.
2. It sends the versioned value to the key's replicas that are up, in parallel on the replication
   pool. The local replica is written in-process; remote replicas get `PUT_INTERNAL` /
   `DELETE_INTERNAL` over authenticated connections.
3. Each replica keeps the value only if its version is newer than what it has (`putIfNewer`). A
   DELETE writes a **tombstone**: a null value with a version.
4. Replicas that are down, or whose write fails, get a **hint** (below). Hints never count toward
   the consistency level.
5. The coordinator replies once 1, 2 or 3 replicas acknowledge (ONE, QUORUM, ALL: the level the
   client put in the request, or the server's default), with the assigned version. If too few replicas are up it fails immediately; if too many fail, or 5 seconds pass,
   the client gets an error.

The acknowledgements needed come from the replication factor (capped by cluster size), never from
how many replicas happen to be up.

### Read (GET / EXISTS)

1. The coordinator asks only as many live replicas as the consistency level needs (1, 2 or 3),
   local replica first, since it answers in-process without a network round trip.
2. It returns the newest version among those answers (see the order below). A tombstone as the
   newest version means "not found".
3. If a contacted replica fails, the next live replica is asked in its place. If one is merely
   slow (no answer within 50 ms, `TITANKV_SPECULATIVE_RETRY_MS`), a spare replica is asked as well
   and whichever answers first counts: **speculative retry**, as in Cassandra. A single sweeper
   thread checks pending reads, so reads that answer quickly pay nothing for it.
4. Once every contacted replica has answered, any that returned an older version, or none, is sent
   the newest one: **read repair**. Replicas that were not asked are brought up to date by hinted
   handoff and anti-entropy instead.

Asking only the replicas needed, rather than all three, is what Cassandra and Dynamo do too. At
QUORUM it saves a third of the read traffic between nodes.

With QUORUM reads and writes, R + W = 2 + 2 > 3 = N: every read quorum shares a replica with every
write quorum, so a QUORUM read returns the latest acknowledged QUORUM write.

## Versions and clocks

Every write carries a 64-bit version from a **hybrid logical clock** (HLC): wall-clock milliseconds in
the high 48 bits and a logical counter in the low 16. Last write wins by version.

Plain wall clocks make last-write-wins lose data under clock skew: if a coordinator's clock is 30
seconds behind, its writes carry older versions than writes it follows and are silently discarded.
TitanKV prevents this in two ways:

- **Node clocks never move backwards past a version they have seen.** An HLC issues
  `max(wall clock, last issued or observed + 1)`, and a node observes the version of every replicated
  write it receives and every read it coordinates. A replica with a slow clock therefore still
  versions its next write after the writes it has stored.
- **Clients carry causal context.** A client remembers the newest version it has read or written and
  sends it with each PUT and DELETE; the coordinator observes it before assigning a version. A client's
  later write therefore always wins over anything it saw, even if the coordinator never saw that
  version. Context can be handed between clients (`getCausalContext` / `observeCausalContext`).

Versions more than 60 seconds ahead of the local clock are not adopted, so one node with a badly
wrong clock cannot drag the whole cluster into the future. `ClockSkewTest` runs a node 30 seconds
slow and shows a write losing without context and winning with it.

**One order for versions, everywhere.** Two coordinators can issue the same HLC timestamp for
concurrent writes to a key (same millisecond, counter 0). So versions are totally ordered: higher
timestamp, then a tombstone (deletes win ties, as in Cassandra), then the greater value. Replicas
(`putIfNewer`), the read coordinator and anti-entropy all use this one comparison
(`KeyValuePair.compareVersions`), so replicas that applied the two writes in different orders still
converge. An earlier version compared timestamps only on replicas but values on reads: the replicas
kept whichever write arrived first, and read repair's fix was rejected forever as "not newer".

What remains is inherent to last-write-wins: two clients writing the same key concurrently, neither
having seen the other's write, resolve to one winner and the other write is dropped without a
conflict being reported.

## Convergence: three mechanisms

Replicas can miss writes (a node was down, a request failed). TitanKV repairs them three ways, from
fastest to most thorough:

| Mechanism | When it runs | What it fixes |
|---|---|---|
| **Hinted handoff** | When a down replica comes back | Writes the replica missed while it was down |
| **Read repair** | On every read | Stale replicas of keys that are read |
| **Anti-entropy** | Every 60s with a random peer, and after a node leaves | Every key two replicas share, read or not |

### Hinted handoff

When a replica is down or a write to it fails, the coordinator stores the write as a hint for that
node. Hints are coalesced per key (only the newest version matters) and appended, CRC-checked, to
`<data dir>/node-<port>/hints/<node>.hints` so they survive a coordinator restart. When gossip reports
the node as recovered, joined or restarted (and every 2 seconds after that), hints are delivered;
delivery stops at the first failure and resumes later. No hints are kept for a node that has
been down longer than 3 hours, or beyond 100,000 per node; anti-entropy covers those cases.

### Merkle-tree anti-entropy

For two nodes, each builds a Merkle tree over the keys both of them replicate. Keys go to one of
1,024 leaves by the top 10 bits of their hash. A leaf's hash XORs the digests of its entries (key,
version, expiry and a hash of the value, tombstones included), and each parent hashes its two
children. A repair round:

1. fetches the peer's root hash (`MERKLE_TREE`, 8 bytes) and stops if it matches;
2. otherwise fetches the whole tree (16 KB) and walks both trees top-down to the leaves that differ;
3. fetches the peer's (key, version) digests for the differing leaves (`MERKLE_LEAF`, up to 64
   leaves per request, one pass over the store on each side) and copies whichever side holds the
   newer version of each key to the other side. If both hold the same timestamp with different
   contents, both versions are exchanged and each side keeps the winner by the order above.

Identical replicas cost one small round trip, and replicas that differ in a few keys transfer only
those keys. When a node leaves or is removed, its keys gain new replicas that hold none of the
data, so every node immediately runs a round with each peer.

## Threading model

```
selector thread ──► worker pool (max(4, CPUs)) ──► replication pool (max(16, 4×CPUs))
  accepts, reads,     decodes auth, runs local      blocking socket I/O to
  writes sockets      commands, starts replicated   remote replicas
                      ones and returns at once
```

- **One selector thread** does all socket I/O without blocking.
- **Worker threads never wait on replicas.** A replicated command returns a `CompletableFuture`;
  when it completes, the response is queued and the connection's next command is scheduled. Commands
  on one connection still run in order, so responses match request order.
- **Replica I/O** runs on a separate pool because it uses blocking client sockets. Node-to-node
  clients retry once on a fresh connection if a pooled one turns out to be stale (the peer
  restarted), and do not use the client circuit breaker, since gossip already tracks liveness.
- **Repair traffic is kept apart.** Hint delivery and Merkle repair use their own small connection
  pool per peer (2 connections), so a burst of repair after a node restarts cannot make client
  reads wait for a connection.

Separating workers from replica I/O fixed a distributed deadlock. Workers used to block while waiting for replica
responses, and those responses (including a node's requests to itself) needed a free worker on the
same pools. With a handful of concurrent clients every worker was waiting, and requests only
finished when 5-second timeouts fired.

### Keeping the replicated path cheap

Profiling a 3-node cluster under load (`strace -c`, JFR) showed 93% CPU, 41% of it in the kernel,
and about 13 system calls per operation, 8 of them `futex` calls from handing work between
threads. The changes that cut it to about 9 per operation (3 `futex`):

- Reads contact only the replicas the consistency level needs (above).
- The local replica's write runs on the calling thread when it does not wait for a WAL fsync,
  instead of being handed to the replication pool.
- A response is written straight to the socket when nothing is queued ahead of it, instead of
  registering `OP_WRITE` and waking the selector.
- Node-to-node reads use the socket's `SO_TIMEOUT` rather than a timer task per read.

Measured by alternating old and new builds on the same machine, this raised 3-node QUORUM
throughput by 59% (mixed) and 35% (writes), and single-node throughput by 69%. See
[benchmark results](../benchmark/results/benchmark_results.md#profiling-the-replicated-path).

## Membership and failure detection

`GossipProtocol` works like Cassandra's gossiper, over UDP on port + 1000.

- Every node has a heartbeat `(generation, version)`: the generation is its start time and the
  version increments every second.
- Each second a node bumps its version and sends a **digest** of every member's heartbeat to 3
  random live peers, plus, a quarter of the time, one node it considers down (so recoveries and
  healed partitions are noticed).
- A receiver adopts any heartbeat newer than the one it knows and records that it has heard from
  that node, so liveness spreads transitively. Unknown members in a digest are added, so new nodes
  become known cluster-wide.
- A node keeps sending JOIN to its seeds until it knows another member. Every node should list the
  others as seeds, so any node can restart and rejoin.
- `ClusterManager` marks a node SUSPECT after 3 seconds without a newer heartbeat and DEAD after 10
  seconds. Down nodes stay members and stay on the ring. A newer heartbeat makes the node ALIVE
  again; a newer *generation* means the process restarted, which also triggers an immediate
  anti-entropy repair with it.
- **Readiness.** A node started with seeds rejects client requests until it has found another member,
  and `/ready` reports 503. Without this, a restarted node would briefly act as a one-node cluster and
  acknowledge writes with a single copy. The chaos test caught exactly that when a seed node restarted.
- **Shutting down is not leaving.** On graceful shutdown a node broadcasts LEAVE. Its peers mark it
  DEAD at once instead of after 10 seconds, but it stays a member and stays on the ring, as in
  Cassandra: a shutdown is usually a restart, and taking the node off the ring would give its keys
  to nodes that do not have them. Heartbeats from the generation that shut down are ignored, so
  stale gossip cannot revive it; the restarted process (a newer generation) rejoins. An earlier
  version removed the node from the ring on LEAVE. With two of three nodes stopped, the survivor
  then believed it had lost its seeds and refused every request, even at ONE.
- **Removal.** An operator removes a dead node for good with `removenode`: the coordinating node
  removes it and gossips REMOVE to every member three times. The removed generation is remembered,
  so stale digests cannot re-add the node, while a restart (newer generation) can. Hints held for it
  are dropped.
- **Clocks.** Silence is measured with `System.nanoTime()`, so an NTP step of the wall clock cannot
  make every peer look dead at once.

**Security.** With a cluster secret, every packet carries an HMAC-SHA256 that receivers verify. Each
message carries a send timestamp that strictly increases per sender; receivers drop messages more
than 5 minutes old and any message not newer than the last one from that sender (replays).

## Storage and durability

`InMemoryStore` holds entries in a `ConcurrentHashMap<String, KeyValuePair>`. Each entry has the
value (null for a tombstone), a version and an optional expiry.

**Write-ahead log.** Unless disabled (dev mode disables it by default), every mutation appends a
record `[magic][version][op][key len][value len][timestamp][expires][key][value][CRC32]` to
`<data dir>/node-<port>/wal.log`, and is acknowledged only after fsync.

- The WAL append happens inside the map's per-key `compute`, so for any one key the WAL order
  matches the order updates were applied in memory.
- **Group commit.** Writers append under a lock but fsync outside it. A writer waits until the WAL is
  durable up to its own record; whichever writer performs the fsync covers everything appended so
  far. This raised durable write throughput by 68%.
- **Snapshots.** When the WAL exceeds 128 MB, all live entries are written to `snapshot.tmp`, which is
  fsynced and atomically renamed to `snapshot.dat`, and then the WAL is truncated. Mutations hold a
  read lock and snapshots the write lock, so the WAL is only truncated once every record in it is
  already in the map, and therefore in the snapshot.
- **Recovery** replays the snapshot and then the WAL, stopping at the first truncated or corrupt
  record, which is what a crash in the middle of an append leaves behind.

**Memory limit.** Writes that would exceed `TITANKV_MAX_MEMORY_MB` are rejected with an error, after
purging expired entries. Live data is never evicted, since that would silently lose acknowledged
writes.

**Expiry.** Expired entries are removed lazily on access and by a cleanup task every minute, which
also purges tombstones older than the grace period (24 hours by default). A replica that is away
longer than that and missed a delete could resurrect the key. This is the same trade-off as
Cassandra's `gc_grace_seconds`.

## Topology changes

- **Join.** A new node gossips its way in and takes over parts of the ring. Nodes that share keys with
  it stream those keys (tombstones included, original versions kept) to it, and anti-entropy covers
  anything missed. The new node is a replica from the moment it is known, before streaming ends, so
  a QUORUM read in that window can miss a recent write. Closing the window would need Cassandra's
  pending ranges: write to old and new replicas, read from the old ones, until bootstrap completes. The nodes that gave up ranges keep their copies until an operator runs `cleanup`,
  which writes each such key to all its current replicas and only then deletes the local copy.
- **Restart** (after a crash or a graceful shutdown). Peers deliver the node's hints and run a
  Merkle-tree repair with it, which sends only the keys its WAL did not have (all of them if it ran
  without a WAL).
- **Removal.** `removenode` on a DEAD node changes the ring; anti-entropy fills the new replicas.

## Testing strategy

- **Unit tests** for the protocol, hash ring, store, WAL recovery (including torn writes and
  concurrent snapshots), hinted handoff and configuration.
- **Integration tests** start real clusters of 1–5 nodes inside the JVM, on real sockets.
- **Chaos test.** Eight writers overwrite 200 keys with increasing versions at QUORUM on a 5-node
  production-mode cluster while nodes restart one at a time: once crashing (no LEAVE), once shutting
  down gracefully (a rolling restart). Each key has one writer, so every read a writer makes of its
  own keys must return at least the version it last had acknowledged. Afterwards every key must hold
  a version between its last acknowledged and last attempted write: no acknowledged write is lost,
  and no value appears that was never written. It found three real bugs: sloppy quorums, stale
  connections combined with the circuit breaker, and the seed-restart split brain.

## Security summary

| Surface | Protection |
|---|---|
| Gossip | HMAC-SHA256 per packet with the cluster secret, freshness window, per-sender replay check |
| Internal replica commands | Cluster token (AUTH) and source IP must belong to a cluster member |
| Admin commands (`removenode`, `cleanup`) | Cluster token |
| Client commands | Optional client token (`TITANKV_CLIENT_TOKEN`) |
| Protocol parsing | Magic bytes, length limits (64 KB keys, 16 MB values), incomplete-frame timeout |

Token comparison is constant-time. Traffic is not encrypted.
