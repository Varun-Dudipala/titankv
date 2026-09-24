# TitanKV Architecture

TitanKV is a leaderless, Dynamo-style key-value store. Every node runs the same components and
any node can serve any request.

```
┌──────────────────────────────── TitanKV node ────────────────────────────────┐
│                                                                              │
│  TcpServer (NIO selector) ──► ConnectionHandler ──► ReplicationManager ──┐   │
│        binary protocol          per-connection       coordinates the     │   │
│                                 command queue        key's replicas      │   │
│                                                                          ▼   │
│  ClusterManager ◄── GossipProtocol (UDP)         InMemoryStore: map + WAL    │
│   membership, failure detection,                 (local replica)            │
│   ConsistentHash ring                                                        │
│                                                                              │
│  DataRebalancer (streams keys to joining/recovered nodes)                    │
│  MetricsHttpServer (/metrics /health /ready /status)                         │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Data placement: consistent hashing

`ConsistentHash` places 150 virtual nodes per physical node on a 64-bit ring (MurmurHash3 of
`host:port#i`, stored in a `ConcurrentSkipListMap`). A key's replicas are the first 3 distinct
*available* nodes clockwise from the key's hash. Adding or removing a node moves about 1/N of the
keys, and virtual nodes spread that movement across all nodes.

Because unavailable nodes are skipped, a key whose replica is down is temporarily written to the
next live node (a "sloppy" replica set). This keeps writes available at the cost of the strict
quorum-overlap guarantee while membership is changing.

## Request path

Clients send any request to any node. The node that receives it is the **coordinator** for that
request; there is no primary and no redirect. The client library hashes keys to pick a node only
to spread load, and fails over to the next node if one is down.

### Write (PUT / DELETE)

1. The coordinator assigns a timestamp (wall clock, forced strictly increasing per node) and, for
   a PUT with TTL, an absolute expiry.
2. It sends the versioned value to all replicas in parallel on the replication pool. The local
   replica is applied directly to the local store; remote replicas get `PUT_INTERNAL` /
   `DELETE_INTERNAL` over authenticated connections.
3. Each replica applies it only if it is newer than what it holds (`putIfNewer`). A DELETE writes a
   tombstone: a null value with a timestamp.
4. The coordinator replies once the consistency level is met: 1, 2 or 3 acknowledgements for
   ONE, QUORUM or ALL. If too many replicas fail, or 5 seconds pass, the client gets an error.

The number of acknowledgements required comes from the replication factor (capped by cluster
size), not from how many replicas are currently alive. With 2 of 3 replicas down, QUORUM fails
instead of quietly succeeding on one copy.

### Read (GET / EXISTS)

1. The coordinator queries all replicas in parallel (the local one in-process).
2. As soon as enough replicas have answered for the consistency level, it returns the newest
   version among those answers: highest timestamp, ties broken by comparing values so every
   coordinator picks the same winner. A tombstone as the newest version means "not found".
3. After all replicas answer (or the timeout passes), any replica that returned an older version,
   or no version, is sent the newest one. This is **read repair**.

With QUORUM for both reads and writes, R + W = 2 + 2 > 3 = N, so every QUORUM read overlaps at
least one replica that acknowledged the latest QUORUM write, as long as the replica set is stable.

### Conflict resolution

Last write wins by timestamp. Tombstones take part like any other version, so a replica that
missed a delete cannot bring the value back: the tombstone is newer and read repair overwrites it.
Tombstones are purged after a grace period (24 hours by default). A replica that misses a delete
and stays away longer than that could resurrect the key.

Because timestamps come from coordinator clocks, clock skew can let an earlier write win. This is
the main trade-off of last-write-wins; hybrid logical clocks would remove it.

## Threading model

```
selector thread ──► worker pool (max(4, CPUs)) ──► replication pool (max(16, 4×CPUs))
  accepts, reads,     decodes auth, runs local      blocking socket I/O to
  writes sockets      commands, starts replicated   remote replicas
                      ones and returns at once
```

- **One selector thread** does all socket I/O without blocking.
- **Worker threads never wait on replicas.** A replicated command returns a `CompletableFuture`;
  when it completes, the response is queued and the connection's next command is scheduled.
  Commands on one connection still run strictly in order, so responses match request order.
- **Replica I/O** runs on a separate pool because it uses blocking client sockets.

The separation matters. In an earlier version, workers blocked while waiting for replica
responses, and those responses (including a node's requests to itself) needed a free worker on the
same pools. Under a handful of concurrent clients every worker was waiting, and requests only
finished when 5-second timeouts fired: a distributed thread-starvation deadlock.

## Membership and failure detection: gossip

`GossipProtocol` works like Cassandra's gossiper, over UDP on port + 1000.

- Every node has a heartbeat `(generation, version)`: the generation is its start time and the
  version increments every second.
- Each second a node bumps its version and sends a **digest** of every member's heartbeat to 3
  random live peers, plus, a quarter of the time, to one node it considers down (so recoveries and
  healed partitions are noticed).
- A receiver adopts any heartbeat newer than the one it knows and records that it has heard from
  that node. Liveness therefore spreads transitively: node A learns B is alive from C, without
  B contacting A. Unknown members in a digest are added, so new nodes become known cluster-wide.
- A new node sends JOIN to its seeds every round until it knows another member.
- `ClusterManager` marks a node SUSPECT after 3 seconds without a newer heartbeat and DEAD after 10
  seconds; DEAD nodes leave the hash ring but stay members, so they still count toward consistency
  requirements. A newer heartbeat makes the node ALIVE again. A restarted node has a newer
  generation, so it is recognised as the same node coming back.
- On graceful shutdown a node broadcasts LEAVE and is removed. The departed generation is
  remembered so stale digests cannot re-add it, while a restart (newer generation) can.

**Security.** With a cluster secret, every packet carries an HMAC-SHA256 that receivers verify.
Each message carries a send timestamp that strictly increases per sender; receivers drop messages
more than 5 minutes old, and any message not newer than the last one seen from that sender
(replays).

## Storage and durability

`InMemoryStore` holds entries in a `ConcurrentHashMap<String, KeyValuePair>`. Each entry has the
value (null for a tombstone), a version timestamp and an optional expiry.

**Write-ahead log.** Unless disabled (dev mode disables it by default), every mutation appends a
record `[magic][version][op][key len][value len][timestamp][expires][key][value][CRC32]` to
`<data dir>/node-<port>/wal.log` and is acknowledged only after fsync.

- The WAL append happens inside the map's per-key `compute`, so for any one key the WAL order
  matches the order updates were applied in memory.
- **Group commit.** Writers append under a lock but fsync outside it. A writer waits until the
  WAL is durable up to its own record; whichever writer performs the fsync covers everything
  appended so far. On a 3-node cluster this raised durable write throughput by 68%.
- **Snapshots.** When the WAL exceeds 128 MB, the store writes all live entries to
  `snapshot.tmp`, fsyncs it, atomically renames it to `snapshot.dat`, and truncates the WAL.
  Mutations hold a read lock and snapshots the write lock, so the WAL is only truncated once every
  record in it is already reflected in the map, and therefore in the snapshot.
- **Recovery** replays the snapshot and then the WAL, stopping at the first truncated or corrupt
  record, which is what a crash in the middle of an append leaves behind.

**Memory limit.** Writes that would exceed `TITANKV_MAX_MEMORY_MB` are rejected with an error,
after purging expired entries. Live data is never evicted, since that would silently lose
acknowledged writes.

**Expiry.** Expired entries are removed lazily on access and by a cleanup task every minute, which
also purges tombstones older than the grace period.

## Rebalancing

When a node joins or recovers, every node that is also a replica for some of its keys streams those
keys, tombstones included, to it with their original timestamps. Because replicas keep the newer
version, the transfer is idempotent and safe to run concurrently from several nodes.

## Security summary

| Surface | Protection |
|---|---|
| Gossip | HMAC-SHA256 per packet with the cluster secret, freshness window, per-sender replay check |
| Internal replica commands | Cluster token (AUTH) and source IP must belong to a cluster member |
| Client commands | Optional client token (`TITANKV_CLIENT_TOKEN`) |
| Protocol parsing | Magic bytes, length limits (64 KB keys, 16 MB values), incomplete-frame timeout |

Token comparison is constant-time. Traffic is not encrypted.
