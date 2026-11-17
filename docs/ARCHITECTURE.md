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
│  MetricsHttpServer (/metrics /health /ready /status /dashboard /api/*)       │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Data placement

`ConsistentHash` places 150 virtual nodes per physical node on a 64-bit ring (MurmurHash3 of
`<node id>#i` in a `ConcurrentSkipListMap`). Positions come from the node's id, not its address:
the id is the same on every node and survives a change of IP. They used to come from the resolved
IP, so a container that came back with another address silently took over different ranges. A key's **replicas** are the first 3 distinct nodes
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
and no redirect. Which node a client picks is a matter of cost, not correctness: a coordinator
that is a replica of the key answers from its own copy, and one that is not has to ask the
replicas. So the client library asks each node for its id (`NODE_ID`), places the nodes on a ring
of its own exactly as the cluster does, and sends each request to the key's first replica, failing
over to the next ones if it is down. A node that has not answered yet is placed by its address
until it does.

That the client's ring must *be* the cluster's ring was learned from a benchmark. Clients used to
place nodes by address and the cluster by resolved IP, which agree when everything resolves alike,
as on one machine. When the cluster moved to placing nodes by id (so that a node keeps its ranges
when its address changes), the two rings stopped agreeing and nothing failed: every test passed.
Re-running the benchmarks against the earlier build on the same machine showed a 5-node cluster
23% slower, and a 3-node cluster unchanged. With three replicas on three nodes every node holds
every key, so the first node is always a replica; on five, two requests in five had begun to land
on a node that did not hold the key.

### Write (PUT / DELETE)

1. The write arrives with the version its client chose (see below); the coordinator checks it is
   not far ahead of its own clock. A PUT with TTL also gets an absolute expiry, from the
   coordinator's clock.
2. It sends the versioned value to the key's replicas that are up, in parallel on the replication
   pool. The local replica is written in-process; remote replicas get `PUT_INTERNAL` /
   `DELETE_INTERNAL` over authenticated connections.
3. Each replica keeps the value only if its version is newer than what it has (`putIfNewer`). A
   DELETE writes a **tombstone**: a null value with a version.
4. Replicas that are down, or whose write fails, get a **hint** (below). Hints never count toward
   the consistency level.
5. The coordinator replies once 1, 2 or 3 replicas acknowledge (ONE, QUORUM, ALL: the level the
   client put in the request, or the server's default). If too few replicas are up it fails
   immediately; if too many fail, or 5 seconds pass, the client gets an error.
6. A replica that already holds a newer version of the key keeps it and says so. If any
   acknowledging replica did, the coordinator replies STALE with that version instead of OK, and
   the client sends the write again with a later version (see below).

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

**The client chooses the version**, as Cassandra's drivers do. Each client has an HLC and issues
`max(wall clock, last issued or seen + 1)` for every PUT and DELETE. Three things follow:

- **A client's writes keep their order**, whichever nodes coordinate them and whatever those
  nodes' clocks say, including writes that failed or timed out: the client knows the version of
  every write it attempted. A call that fails over to another node carries the same version, so
  the two nodes store one version, not two competing ones.
- **A write is ordered after everything its client has seen.** A read moves the client's clock
  past the version read, and the reply to a write carries the newest version the coordinator has
  seen. A client whose wall clock is behind therefore catches up to the cluster with its first
  reply. The clock value can be handed to another client or process (`getCausalContext` /
  `observeCausalContext`), which then writes after everything the first one did and saw.
- **Node clocks do not decide which write wins.** They are used for expiry times, tombstone age,
  and one check: a node refuses a version more than 60 seconds ahead of its own clock. Accepted,
  such a version would outrank every write to the key until real time caught up. Everything on
  a node that asks the time (expiry, tombstone age, gossip timestamps, that check) reads the
  node's one clock object, so a test can set one node's clock wrong inside a running cluster:
  `NodeClockTest` shows a node 30 seconds fast ending a 20-second TTL for every replica, and a
  node 70 seconds fast being dropped by its peers until it is corrected.
- **A client follows other clocks only so far.** It adopts a version from a reply or a read only
  if it is at most 30 seconds ahead of its own wall clock. Without that limit, one client 58
  seconds fast (inside what nodes accept) pulled every node's and client's clock forward with it,
  and a correct client then had every write refused by any node whose clock ran a few seconds
  slow. Now the damage stays with the keys that client wrote: another client cannot overwrite
  them until real time catches up, and gets an error saying so.
- **A write that would lose is sent again.** A version is only as good as its client's clock: a
  client 30 seconds behind another, or writing in the same millisecond, can issue a lower version
  for a later write. The replicas would take it, acknowledge it, and never return it. Instead a
  replica that holds a newer version reports it, the coordinator answers STALE, and the client
  retries with a version after the one it lost to (up to three times; after that it is racing
  other writers and last-write-wins picks one). At QUORUM a write's acknowledging replicas always
  include one that holds any write acknowledged before it, so **a write that starts after another
  was acknowledged wins, whatever the two clients' clocks say.** At ONE there is no such overlap.

An earlier version had the coordinator choose the version from its own HLC, after observing the
newest version the client had seen. A fault-injection test broke it. A write that fails for lack
of a quorum can stay on its coordinator, with a version the client never learned. When that
coordinator's clock was ahead and the client's next write was acknowledged through another node,
the failed write carried the higher version and replaced the acknowledged one on every replica
once the network healed. `ClockSkewTest` keeps that scenario, and runs node and client clocks
wrong in both directions.

The first version of this change left that last point out, and a test of two clients writing one
key in turn failed one run in three: a delete and the put after it, from different clients, fell
in the same millisecond, and the put got the lower version. `ClockSkewTest` now has four clients
with clocks up to 30 seconds apart taking 400 turns on one key.

What remains is inherent to last-write-wins: writes that overlap in time resolve to one winner,
by version. A write sent without a version (the wire protocol allows it) gets one from the
coordinator's HLC, which is newer than anything that node has stored or coordinated.

**One order for versions, everywhere.** Two clients can issue the same HLC timestamp for
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
delivery stops at the first failure and resumes later. Once all of a node's hints are delivered
its file is emptied, including hints that were loaded from disk after a restart. A hint file that
ends in a record torn by a crash is rewritten on load, for the same reason the WAL is cut back.
No hints are kept for a node that has been down longer than 3 hours, or beyond 100,000 hints or
64 MB per node (hints are held in memory with their values, outside the store's limit);
anti-entropy covers those cases.

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
selector thread ──► worker pool (max(4, CPUs)) ──► one replica pool per peer (max(8, CPUs) each)
  accepts, reads,     decodes auth, runs local      blocking socket I/O to
  writes sockets      commands, starts replicated   that peer
                      ones and returns at once
```

- **One selector thread** does all socket I/O without blocking.
- **A full worker queue is an answer, not a wait.** The workers' queue holds 1,000 requests per
  worker. A request that does not fit is answered "Server busy", and the client sends it to
  another node. The pool used to run such a request on the thread that submitted it
  (`CallerRunsPolicy`), which for a new request is the selector: one fsync or one slow replica
  on that thread and no connection was read, answered or accepted until it returned, at exactly
  the moment the node was overloaded. `BusyServerTest` runs one worker with a queue of one
  against sixteen writers.
- **Worker threads never wait on replicas.** A replicated command returns a `CompletableFuture`;
  when it completes, the response is queued and the connection's next command is scheduled. Commands
  on one connection still run in order, so responses match request order.
- **Replica writes are sent together when they queue up.** A replica call holds a thread and a
  connection until the replica answers. When a peer answers in a fraction of a millisecond, each
  write goes out on its own. When it waits for its disk, calls take milliseconds and writes for
  that peer queue; a sender that becomes free then takes everything queued (up to 512 writes or
  1 MB) and sends it as one request (`PUT_BATCH_INTERNAL`), which the replica makes durable with
  one fsync and answers write by write. Nothing waits to fill a batch. Without this a
  coordinator could have at most 8 writes in flight to a peer, and durable throughput fell from
  2,100 to 1,000 writes a second when clients went from 64 to 256; with it, it rises from 2,600
  to about 7,000. In-memory writes gained too: 52,000 to 84,000 a second with 64 clients.
  A peer that answers "Unknown command" (an older version) is sent single writes from then on.
- **Replica I/O** runs on separate pools because it uses blocking client sockets, one pool per
  peer. Calls stuck on a slow or unreachable peer fill only that peer's threads and its queue of
  1,024; further calls to it fail at once (writes become hints, reads go to another replica),
  and calls to healthy peers are unaffected. An earlier version shared one pool between all
  peers. A fault-injection test with one replica answering a second late showed the coordinator
  falling to about 20 requests a second (threads ÷ the slow peer's latency), for every key,
  including keys that replica did not hold; with a pool per peer it stays above 800. Node-to-node
  clients retry once on a fresh connection if a pooled one turns out to be stale (the peer
  restarted), and do not use the client circuit breaker, since gossip already tracks liveness.
  A client's connection pool allows a fixed number of connections per node; a thread that finds
  them all in use waits and takes the next one returned.
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
- The local replica's write runs on the calling thread instead of being handed to the
  replication pool. (With a WAL the thread logs the write and moves on; the write counts toward
  the consistency level once it is durable.)
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
- **Every member is on the ring, up or down.** A node adopts every member a digest lists, including
  ones reported down (as DEAD), because a down member is still a replica of its keys.
- **Saved membership.** A node with a WAL also saves its member list, and the nodes that were
  removed, to `<data dir>/node-<port>/members` (checksummed, replaced atomically) whenever the
  list changes. On restart every saved member is back on the ring at once, marked DEAD until gossip
  hears from it. The ring is whole from the first moment, whoever happens to be up.
- **Readiness.** A node started with seeds rejects client requests, and `/ready` reports 503, until
  it knows the whole membership. Every digest says whether its sender knows the whole membership
  and whether the packet lists all of it. A node is ready when one of these holds:
  - a digest with both flags has been applied;
  - it restarted with a saved membership and 5 seconds have passed without such a digest (its
    peers may all be down; what it saved was complete when it stopped);
  - it has nothing saved and has learned as many members as it has seeds: a new cluster forming,
    where the seed list is the membership.

  The node is not ready at any moment before that, including while it starts: the server
  accepts connections before the cluster manager is up, and for as long as resolving the seeds'
  host names took, a starting node used to count as having no seeds and so as ready. A failover
  benchmark hit that window once in 94,000 requests (`StartupReadinessTest`).

  Three weaker gates each failed. With none, a restarted node acted as a one-node cluster and
  acknowledged writes with a single copy, as the chaos test found when a seed node restarted.
  "Knows one other member" let a restarted node serve with a partial ring, missing members that
  were down; the chaos test's stale-read check caught that on a slow CI runner. "Has applied any
  digest" let two nodes that restarted together take each other's two-node view for the cluster:
  after a full stop of five nodes, the first two back reported ready and acknowledged QUORUM
  writes for keys whose three replicas were all still down. `MembershipTest` and
  `SavedMembershipTest` reproduce the last two.
- **Shutting down is not leaving.** On graceful shutdown a node broadcasts LEAVE. Its peers mark it
  DEAD at once instead of after 10 seconds, but it stays a member and stays on the ring, as in
  Cassandra: a shutdown is usually a restart, and taking the node off the ring would give its keys
  to nodes that do not have them. Heartbeats from the generation that shut down are ignored, so
  stale gossip cannot revive it; the restarted process (a newer generation) rejoins. An earlier
  version removed the node from the ring on LEAVE. With two of three nodes stopped, the survivor
  then believed it had lost its seeds and refused every request, even at ONE.
- **Removal.** An operator removes a dead node for good with `removenode`: the coordinating node
  removes it and gossips REMOVE to every member three times. The removed generation is remembered,
  saved, and repeated in every digest (16 removed nodes per packet, in rotation), so stale digests
  cannot re-add the node, while a restart (newer generation) can. Hints held for it are dropped.
  The three REMOVE messages alone were not enough: a node that was down or partitioned for those
  two seconds never heard of the removal, kept the node on its ring, and placed keys differently
  from everyone else. `MembershipTest` cuts a node off during a removal and requires it to catch up.
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
- **Group commit.** A write is applied and appended on the thread that handles it, and that
  thread moves on. One thread fsyncs the log for as long as writes are waiting, and each fsync
  releases every write appended before it started; the reply to a write goes out when its record
  is durable. The busier the node, the more writes share an fsync (a unit test hands the store
  3,000 writes and counts about 60 fsyncs); an idle node syncs each write as it arrives.

  The first version had the writers do this themselves: each waited, and whichever got the lock
  fsynced for the rest. That raised durable throughput by 68% over an fsync per write, but every
  waiting write still held a thread, so one fsync could never cover more writes than the node
  had worker threads (8). On a 3-node cluster that capped durable writes at a few hundred a
  second however many clients there were. Thread dumps under load showed exactly that: every
  worker parked on the sync lock.
- **Snapshots.** When the WAL exceeds 128 MB, all live entries are written to `snapshot.tmp`, which is
  fsynced and atomically renamed to `snapshot.dat`, and then the WAL is truncated. Mutations hold a
  read lock and snapshots the write lock, so the WAL is only truncated once every record in it is
  already in the map, and therefore in the snapshot.
- **Recovery** replays the snapshot and then the WAL, stopping at the first truncated or corrupt
  record. If nothing intact follows it, it is what a crash in the middle of an append leaves
  behind, and the WAL is cut back to that point, so new records are never appended behind an
  unreadable one. If intact records do follow (every place the record magic appears after it is
  tried), the log is damaged in the middle: the node refuses to start and leaves the file as it
  is. A snapshot is written whole and renamed into place, so any unreadable record in one is
  damage. Two earlier versions got this wrong in opposite directions. The first left a torn record
  in place: writes after the restart were acknowledged, then lost at the next restart, when replay
  stopped at the torn record before reaching them. The second cut the log at the first bad record
  wherever it was: one flipped byte early in a log of 1,000 records left the node running with 10
  of them and deleted the other 990 from disk.
- **A WAL failure stops the store.** After a failed append the log may end in half a record, and
  after a failed fsync the kernel may have dropped the pages it could not write, so a later fsync
  that succeeds proves nothing. Either way the store refuses every further write and `/health`
  reports 503 until the node is restarted; reads still work.
- **Renames are made durable.** After a snapshot is renamed into place, and after the member list
  is, the directory is fsynced, before the WAL is emptied. Otherwise a power cut could leave the
  old snapshot beside an empty log.
- A snapshot that cannot be written (a full disk) is retried after 30 seconds, not on every write.

**Memory limit.** Writes that would exceed `TITANKV_MAX_MEMORY_MB` are rejected with an error, after
purging expired entries. Live data is never evicted, since that would silently lose acknowledged
writes. Deletes, and overwrites no larger than what they replace, are always accepted: they used to
be refused like any other write, so a full node could not be emptied.

**Expiry.** A value whose TTL has run out becomes a tombstone with the same version, as in
Cassandra: lazily on access, and by a cleanup task every minute. Reads, anti-entropy, snapshots and
WAL replay all see an expired value as that tombstone, whether or not it has been converted yet.
The value's memory is released; the key and its version stay. An earlier version removed expired
entries outright. If a replica had missed the short-lived write, its older version then looked
newest: a QUORUM read returned a value that an acknowledged write had replaced, and read repair
copied it back to the other replicas.

The cleanup task also purges tombstones older than the grace period (24 hours by default, counted
from the delete or from the expiry time). A replica that is away longer than that and missed a
delete or an expired write could resurrect the key. This is the same trade-off as Cassandra's
`gc_grace_seconds`. Until then an expired key still costs its key and about 48 bytes.

Each replica judges expiry by its own wall clock, and an expiry tombstone wins the tie with its own
value. So a TTL ends when the first replica's clock says so: a node whose clock runs fast ends it
early for the whole cluster, by as much as its clock is ahead. (When expired values simply
vanished, the fast node dropped only its own copy.)

A write with a TTL that is sent twice with one version, after a failover or on a retried
connection, gets its expiry from each coordinator's clock. The later expiry wins between such
copies, as the last step of the version order, so the replicas settle on one. Before that, neither
copy replaced the other, and anti-entropy exchanged the key on every round without converging.

## Topology changes

- **Join.** A new node gossips its way in and takes over parts of the ring. Nodes that share keys with
  it stream those keys (tombstones included, original versions kept) to it, and anti-entropy covers
  anything missed. The new node is a replica for writes from the moment it is known, but answers
  no reads until it has caught up (below). The nodes that gave up ranges keep their copies until
  an operator runs `cleanup`, which writes each such key to all its current replicas and only
  then deletes the local copy.
- **Catching up before answering reads.** A node that starts with no data in a cluster (it has
  just joined, it runs without a WAL, or its disk was replaced) runs a Merkle repair with every
  peer that is up, and until one pass of that has succeeded it refuses client requests (`/ready`
  is 503) and answers replica reads with an error, so the coordinator asks another replica. It
  accepts writes throughout. Quorum overlap assumes a replica still holds what it acknowledged.
  Without this, a node that restarted empty answered "not found" at once: together with one
  replica that had missed a write, it outvoted the one replica that held it, and an acknowledged
  write read as missing (`CatchUpTest` reproduces it). For a join, a key's new replica set is two
  of its old replicas and the new node; any write acknowledged by two of the old three is on at
  least one of those two, so reads answered by them alone see it.

  What is left of the join window: nodes learn of a join a gossip round apart. A coordinator
  still on the old ring after the new node has caught up can acknowledge a write on replicas
  that a reader on the new ring does not ask. Closing that needs Cassandra's pending ranges:
  write to old and new replicas until every node has the new ring.
- **Restart** (after a crash or a graceful shutdown). Peers deliver the node's hints and run a
  Merkle-tree repair with it, which sends only the keys its WAL did not have (all of them if it ran
  without a WAL). The restarted node sends nothing: the members it rediscovers are not new to the
  cluster. It used to treat each as a node that had just joined and send it every key they share.
- **Removal.** `removenode` on a DEAD node changes the ring; anti-entropy fills the new replicas.

## Dashboard

`MetricsHttpServer` also serves `/dashboard`, a single HTML page with inline styles and script
(a `Content-Security-Policy` header forbids it to load anything else), and the JSON it polls once
a second:

| Endpoint | Source |
|---|---|
| `/api/node` | The node's own counters (`MetricsCollector`), pending hints per target (`HintedHandoff`), store size, memory and WAL fsync count (`InMemoryStore`), readiness, and its member list with each member's status and heartbeat age |
| `/api/cluster` | The serving node's member list, each member with its own `/api/node` (or `null`) |
| `/api/ring` | `ConsistentHash.getVirtualNodes()`: every position as a fraction of the ring, its owner, and each node's share of the hash space as first replica |
| `/api/replicas?key=K` | `ClusterManager.getReplicasForKey`: the replicas in ring order, up or not |

**One page, every node.** Operation counts, pending hints and readiness are known only to the node
they describe, and gossip does not carry them. The node serving the page therefore fetches
`/api/node` from every other member and embeds the answers. HTTP ports are not gossiped, so it
tries `host:port + offset` (the offset being its own HTTP port minus its own client port) and
then, for a member on another host, its own HTTP port, and remembers which one answered. An answer is embedded only if it is
one well-formed JSON object that names that member: a guessed port can belong to something else,
this node included. The browser could not do this itself: under
Docker the members are known by service names it cannot resolve, on ports that are mapped
differently on the host. A member that does not answer is reported with `"self": null`, while its
gossip status still comes from the serving node, so "dead according to gossip" and "not answering
HTTP" are separate facts on the page.

The fetches never hold up a request for long. Each peer is asked at most once per 500 ms however
many pages are open, on a thread pool of its own; a request for `/api/cluster` waits at most
300 ms for answers and otherwise returns the last ones (dropped after 3 s). Counting live keys
walks the store, so the count is reused for a second, or for 20 times as long as the last count
took if that is longer; one thread counts at a time and the others report the previous count.

The page does not trust what it is given either. Each node's numbers are drawn inside their own
error handler, so a member that reports something unreadable is shown without numbers, and a
failed round is logged and the next one scheduled regardless. A monitoring page that stops
polling keeps showing its last numbers as if they were current.

QUORUM and ALL are reported against the copies the cluster keeps (`min(replication factor,
members)`), the same number the coordinator counts acknowledgements against.

**Read-only.** The server accepts only `GET`, and the handlers call nothing that writes, deletes
or changes membership. Answers contain no secret or token. Rates are not computed on the server:
the page derives them from two samples of a node's counters and that node's own timestamps.

## Testing strategy

- **Unit tests** for the protocol, hash ring, store, WAL recovery (including torn writes and
  concurrent snapshots), hinted handoff, the client connection pool and configuration.
- **Integration tests** start real clusters of 1–5 nodes inside the JVM, on real sockets.
- **Chaos test.** Eight writers overwrite 200 keys with increasing versions at QUORUM on a 5-node
  production-mode cluster while nodes restart one at a time: once crashing (no LEAVE), once shutting
  down gracefully (a rolling restart). Each key has one writer, so every read a writer makes of its
  own keys must return at least the version it last had acknowledged. Afterwards every key must hold
  a version between its last acknowledged and last attempted write: no acknowledged write is lost,
  and no value appears that was never written. It found three real bugs: sloppy quorums, stale
  connections combined with the circuit breaker, and the seed-restart split brain.

- **Fault injection.** Every node consults a `LinkFilter` before it sends a gossip datagram or a
  node-to-node request; production nodes use a no-op. In tests a `FaultInjector` cuts links in one
  or both directions (refusing requests at once, or swallowing them until a timeout) and adds
  latency, the way Cassandra's in-JVM tests filter messages. It can also lose the reply to a
  request that was carried out, between nodes and to clients: the sender then sees a failure for
  something that happened, which is what makes retries and failover with a reused version occur.
  `PartitionTest` uses it for a 2 | 3 split, an isolated node, a one-way cut and a slow replica.
  A shared `Nemesis` cycles through isolation, splits, one-way cuts, lost replies, crashes,
  crashes during a partition, and a node cut off and then crashed. `PartitionChaosTest` runs the
  chaos workload under it, once with correct clocks and once with node clocks up to 20 seconds
  and client clocks up to 15 seconds wrong, and afterwards also requires all three replicas of
  every key to hold the same entry. Clients are not partitioned: they can always reach every node.
- **Many writers per key, with a checked history.** The chaos workloads above give each key one
  writer, so one client's clock orders that key's writes whatever the cluster does; they could
  not see a write from one client lost to another's, a delete undone, or a failed write surfacing
  over a later one, and three bugs of exactly that kind got past them. `MultiWriterChaosTest` has
  eight clients reading, writing (some with a TTL) and deleting the same twenty keys under the
  nemesis, with correct and with skewed clocks. Every operation is recorded with its start and
  end time, and `HistoryChecker` then checks the whole history: a read may not return a value
  nobody wrote, a value before its write started, or a value (or a missing key) that another
  write, acknowledged after it finished and before the read started, had replaced. A write whose
  outcome is unknown may take effect at any later time. This is the guarantee QUORUM gives a
  last-write-wins register; it is weaker than linearizability, which TitanKV does not claim. The
  checker has its own tests, and the chaos test was checked against a deliberately broken build:
  with the STALE reply switched off it reported 760 violations in one run.
- **Real processes, killed.** `ProcessCrashTest` runs three nodes as separate JVMs and kills them
  with SIGKILL under the same shared-key workload. A node then dies between any two instructions,
  mid-append and mid-request, and returns as a new process with only what it wrote to disk.
- **What the tests do not show.** SIGKILL does not lose data the kernel has accepted but not yet
  written, so no test can find a missing fsync or directory sync; that needs a power cut or a
  fault-injecting filesystem. Tests skew the clocks that issue and check versions (clients' and
  nodes' hybrid clocks); expiry, tombstone age and gossip freshness read the machine's wall clock
  directly, which no test skews. Nothing expires during a chaos run. Clients are never cut off
  from nodes, only nodes from each other.

## Security summary

| Surface | Protection |
|---|---|
| Gossip | HMAC-SHA256 per packet with the cluster secret, freshness window, per-sender replay check |
| Internal replica commands | Cluster token (AUTH) and source IP must belong to a cluster member. Members' names are resolved once and remembered; when a connection matches none, they are resolved again (at most once per 10 seconds), so a member that returns with a new address is not locked out |
| Admin commands (`removenode`, `cleanup`) | Cluster token |
| Client commands | Optional client token (`TITANKV_CLIENT_TOKEN`) |
| Protocol parsing | Magic bytes, length limits (64 KB keys, 16 MB values) |
| Connections | Closed by the selector after 30 seconds without a whole request; at most 10,000 at once |
| HTTP port (metrics, dashboard) | None. Read-only (`GET` only) and free of secrets, but it shows addresses, key placement and traffic: keep it on a private network |

Token comparison is constant-time. Traffic is not encrypted.
