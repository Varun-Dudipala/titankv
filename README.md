# TitanKV

[![CI](https://github.com/Varun-Dudipala/titankv/actions/workflows/ci.yml/badge.svg)](https://github.com/Varun-Dudipala/titankv/actions/workflows/ci.yml)
[![Java](https://img.shields.io/badge/Java-17+-orange?logo=openjdk)](https://openjdk.org/)
[![License](https://img.shields.io/badge/License-MIT-blue)](LICENSE)

A distributed key-value store in Java, built from the designs of Amazon's Dynamo and Apache
Cassandra: consistent hashing, leaderless replication with tunable consistency, gossip membership,
hinted handoff, Merkle-tree anti-entropy, hybrid logical clocks, and a write-ahead log. A chaos test
crashes nodes under load and checks that no acknowledged write is lost.

## Scope

**What this is:** a working implementation of the core techniques of a Dynamo-style database, with
integration tests that run real multi-node clusters and benchmarks you can reproduce.

**What this is not:** a production database. It has not been externally audited; see
[Known limitations](#known-limitations).

## Features

**Data distribution and replication**
- **Consistent hashing** with 150 virtual nodes per node (MurmurHash3) and replication factor 3
- **Leaderless**: any node coordinates any request, as in Dynamo. The client library asks each node
  its id, builds the cluster's own ring, and sends each request to a replica of its key to save a hop
- **Tunable consistency** (ONE / QUORUM / ALL), chosen **per request** by the client, with **strict
  quorums**: a key's replicas never change because a node is down or restarting, so R + W > N
  guarantees QUORUM reads see QUORUM writes

**Convergence**
- **Read repair**: reads return once the consistency level is met, then fix stale replicas in the background
- **Hinted handoff**: writes for a down replica are kept (persisted, coalesced per key) and delivered when it returns
- **Merkle-tree anti-entropy**: replica pairs compare 1024-leaf hash trees every minute and sync only the keys that differ
- **Last-write-wins with hybrid logical clocks**, issued by the client: a client's writes keep their
  order across nodes, failovers and failed attempts, whatever the nodes' clocks say; equal versions
  break ties the same way on every node; deletes and expired values leave tombstones, so nothing is
  resurrected

**Membership and operations**
- **Gossip** (UDP, Cassandra-style heartbeat digests) with HMAC-SHA256 authentication and replay
  protection; nodes are SUSPECT after 3s and DEAD after 10s of silence
- **Admin**: `removenode` for dead nodes, `cleanup` after joins, `status`, streaming to joining nodes;
  admin commands require the cluster token
- **Metrics** over HTTP (`/metrics` in Prometheus text format, `/health`, `/ready`, `/status`)

**Storage and networking**
- **Write-ahead log** with CRC-checked records, snapshots, and **group-commit** fsync: one sync
  covers every write that arrived during the one before, on the coordinator and on each replica
- **Non-blocking NIO server**; worker threads never wait on replicas
- **Binary protocol** over TCP with 29-byte request and 25-byte response headers
- **TTL** per key, **client circuit breaker**, connection pooling

## Quick start

### Docker (3-node cluster)

```bash
git clone https://github.com/Varun-Dudipala/titankv.git && cd titankv
docker compose up -d --build            # nodes on localhost:9001-9003, metrics on 9091-9093
curl localhost:9091/status

docker compose kill titankv-2           # writes and reads keep working with a node down
docker compose start titankv-2          # it catches up from hints
docker compose down -v
```

### Local

Requires Java 17+. Maven itself is not needed: `./mvnw` (or `mvnw.cmd` on Windows) downloads
the pinned Maven version on first use.

```bash
./mvnw clean package                    # builds target/titankv-1.0.0.jar, runs unit tests
./scripts/start-cluster.sh              # 3 nodes on 9001-9003 (dev mode); waits until the cluster forms
./scripts/titankv-cli.sh localhost:9001 # interactive shell
./scripts/stop-cluster.sh
```

```
titankv> put user:1 Ada
OK
titankv> put session:9 abc 30000        # expires in 30 seconds
OK
titankv> get user:1
Ada
titankv> status
vm:9001                  ALIVE    vm:9001                  (this node)
vm:9002                  ALIVE    vm:9002                  last heard 0.8s ago
vm:9003                  ALIVE    vm:9003                  last heard 0.8s ago
```

Run nodes by hand (every node lists the others as seeds so any node can restart and rejoin):

```bash
export TITANKV_CLUSTER_SECRET="$(openssl rand -base64 32)"
java -jar target/titankv-1.0.0.jar --port 9001 --seeds localhost:9002,localhost:9003
java -jar target/titankv-1.0.0.jar --port 9002 --seeds localhost:9001,localhost:9003
java -jar target/titankv-1.0.0.jar --port 9003 --seeds localhost:9001,localhost:9002
```

Without a cluster secret a node refuses to start unless run with `--dev` / `TITANKV_DEV_MODE=true`.

### Client library

```java
try (TitanKVClient client = new TitanKVClient("localhost:9001", "localhost:9002", "localhost:9003")) {
    client.put("user:123", "Ada Lovelace");
    client.put("session:abc", token, 30_000);          // expires after 30 seconds
    Optional<String> name = client.getString("user:123");
    boolean exists = client.exists("session:abc");
    client.delete("user:123");

    client.put("page-views", "1042", ConsistencyLevel.ONE);         // per request: ONE, QUORUM or ALL
    client.getString("balance:7", ConsistencyLevel.ALL);
}
```

Without a level, requests use the client's default (`ClientConfig.builder().consistency(...)`), and
otherwise the server's (QUORUM unless configured).

The client sends each request to the first replica of its key (it learns the nodes' ids, and so
their places on the ring, from the nodes themselves), pools connections, fails over to the key's
other replicas and then the other nodes on error, and chooses each write's version from its own hybrid logical clock, so its
writes are always ordered after what it has read or written, including writes that failed (see
[clocks](docs/ARCHITECTURE.md#versions-and-clocks)).

## Consistency and failure behaviour

TitanKV is an **AP** system with tunable consistency: during a partition, each side keeps serving the
keys for which it can reach enough replicas, and replicas converge afterwards.

With replication factor 3:

| Level | Acknowledgements | Keeps working with |
|---|---|---|
| ONE | 1 | 2 replicas down; may read stale data |
| QUORUM | 2 | 1 replica down; 2 + 2 > 3, so every QUORUM read overlaps every acknowledged QUORUM write |
| ALL | 3 | no replica down |

| Failure | What happens |
|---|---|
| A node crashes | Marked SUSPECT after 3s, DEAD after 10s. QUORUM continues on the other 2 replicas; writes it misses become hints on the coordinator. |
| A node shuts down (SIGTERM, rolling restart) | It tells its peers, which mark it DEAD at once. It stays a replica of its keys (a shutdown is not a decommission), so its writes become hints until it is back. |
| It comes back | Gossip sees a newer heartbeat and hints are delivered; if the process restarted, an immediate Merkle-tree repair sends it only the keys it is missing; read repair fixes anything else it serves. |
| Two of a key's three replicas are down | QUORUM and ALL fail with "Not enough replicas" instead of accepting a write on one copy; ONE still works. |
| A node is gone for good | `removenode <id>` drops it cluster-wide; nodes that were down or cut off at the time learn of it from later gossip. Anti-entropy re-replicates its keys to their new replicas. |
| A node joins | It takes over ranges and receives writes at once, but answers no reads until it has been repaired from every peer that is up. Reads for its keys are served by the two older replicas meanwhile. |
| A node restarts with no data (no WAL, or a replaced disk) | The same: it answers no reads, as coordinator or replica, until it has caught up from its peers. An empty replica's "not found" never counts toward a read quorum. |
| A node restarts | It has its saved member list at once, down members included, and rejects client requests (`/ready` is 503) until a peer that knows the whole membership confirms it, or 5 seconds pass without one doing so. A node with nothing saved waits for such a peer, or for every seed. |
| The whole cluster stops and only some nodes return | They know the full ring from disk, so they refuse QUORUM for keys whose replicas are still down instead of accepting them onto the wrong nodes. |
| Network partition | Each side serves keys with enough reachable replicas; afterwards hints, read repair and anti-entropy converge them. |
| Crash mid-write | The WAL is fsynced before acknowledging; recovery replays snapshot + WAL and cuts a torn final record off the log. |
| The WAL or a snapshot is damaged anywhere else | The node refuses to start and leaves the files as they are, rather than drop every write after the damage. Moving the file aside lets it start and be repaired from its peers. |
| A WAL write or fsync fails | The node stops accepting writes (`/health` returns 503) until it is restarted; it does not carry on appending behind a possibly half-written record. |
| Clock skew between nodes | Does not affect which write wins: the client chooses each write's version. Node clocks still decide when a TTL runs out and when a tombstone is purged: a key expires when the first replica's clock says so. |
| A client's clock is wrong | A write behind a newer version is reported (STALE) and sent again with a later one, so at QUORUM a write that starts after another was acknowledged still wins. A version over 60s ahead of a node's clock is refused. Other clients do not adopt a version more than 30s ahead of their own clock: a key written by a client that far ahead cannot be overwritten until real time reaches it, and the writer is told so. |
| More requests than a node can take | Requests wait in a bounded queue (1,000 per worker); beyond it the node answers "Server busy" at once and the client takes the request to another node. Nothing runs on the network thread, so the node keeps reading, answering and accepting connections. |
| A node's clock is over a minute ahead | Its gossip is refused, so the others take it for dead and carry on at QUORUM without it (they log the clock difference from 30 seconds). It is a member again once its clock is corrected. |
| A member comes back with a new IP address (a recreated container) | Its peers look its name up again when a node-to-node connection matches no member, at most once per 10 seconds, instead of refusing it until they restart. |
| A client sends part of a request and goes quiet | The server closes the connection after 30 seconds without a whole request. Connections idle between requests are kept; at most 10,000 are open at once. |
| Concurrent writes to one key | Last write wins by (timestamp, then tombstone, then value), the same on every replica; no conflict is surfaced to the application. |

## Configuration

Every setting is an environment variable or the equivalent system property (`titankv.dev.mode`, ...).

| Variable | Default | Purpose |
|---|---|---|
| `TITANKV_DEV_MODE` | `false` | Allow running without a cluster secret; disables the WAL by default |
| `TITANKV_CLUSTER_SECRET` | none | Signs gossip (HMAC) and authenticates node-to-node commands |
| `TITANKV_CLIENT_TOKEN` | none | If set, clients must authenticate with this token |
| `TITANKV_INTERNAL_TOKEN` | cluster secret | Token for node-to-node and admin commands |
| `TITANKV_READ_CONSISTENCY` / `_WRITE_` / `_DELETE_` | `QUORUM` | Default level for requests that do not choose one |
| `TITANKV_SPECULATIVE_RETRY_MS` | `50` | Also ask a spare replica when one has not answered a read by then; `0` disables |
| `TITANKV_DATA_DIR` | `data` | WAL, snapshots, hints and the saved member list go in `<dir>/node-<port>` |
| `TITANKV_WAL_ENABLED` | on unless dev mode | Write-ahead log |
| `TITANKV_WAL_FSYNC` | `true` | fsync before acknowledging a write (group-committed) |
| `TITANKV_WAL_MAX_MB` | `128` | WAL size that triggers a snapshot |
| `TITANKV_MAX_MEMORY_MB` | `512` | Writes beyond this are rejected (never silently evicted); deletes always pass |
| `TITANKV_MAX_CONNECTIONS` | `10000` | Connections beyond this are refused |
| `TITANKV_FRAME_TIMEOUT_MS` | `30000` | How long a connection may take to deliver a whole request |
| `TITANKV_TOMBSTONE_GRACE_MS` | `86400000` | How long tombstones of deleted and expired keys are kept |
| `TITANKV_ANTI_ENTROPY_INTERVAL_MS` | `60000` | Merkle-tree repair with a random peer; `0` disables |
| `TITANKV_METRICS_PORT` | node port + 90 | `/metrics`, `/health`, `/ready`, `/status` |
| `TITANKV_ADVERTISE_HOST` (`--advertise-host`) | host part of the node id | The name or address other nodes and clients reach this node at. Needed when the node id is a plain label (`node-1`) and the nodes are on different machines |
| `TITANKV_WORKER_THREADS` | max(4, CPUs) | Request worker threads |
| `TITANKV_WORKER_QUEUE` | 1,000 per worker | Requests that may wait for a worker; further ones are answered "Server busy" |
| `TITANKV_REPLICATION_THREADS` | max(8, CPUs) | Threads for replica I/O, per peer node |

Ports per node: client TCP on `--port`, gossip UDP on port + 1000, metrics HTTP on port + 90.

## Binary protocol

```
Request:  [MAGIC:3][VERSION:1][CMD:1][KEY_LEN:4][VAL_LEN:4][TIMESTAMP:8][EXPIRES:8][KEY][VALUE]
Response: [MAGIC:3][VERSION:1][STATUS:1][VAL_LEN:4][TIMESTAMP:8][EXPIRES:8][VALUE]
```

Every frame starts with `TKV` and the protocol version (2). A node refuses frames of another
version, and gossip messages carry a version of their own, so a client or node from a build with
a different protocol is turned away with a message that says so instead of being misread. There
is no rolling upgrade across a version change.

Big-endian; `VAL_LEN` of -1 means no value (distinct from an empty value). The low 6 bits of `CMD`
are the command and the top 2 bits an optional consistency level for GET, EXISTS, PUT and DELETE
(0 = server default, 1 = ONE, 2 = QUORUM, 3 = ALL), so choosing a level costs no bytes. For client
PUT/DELETE, `TIMESTAMP` is the write's version, chosen by the client (0 lets the node choose), and
`EXPIRES` carries the TTL in milliseconds; responses to PUT/DELETE return the newest version the
node has seen, which the client's clock follows. Replica messages carry absolute versions and
expiry times.

| Command | Code | Command | Code | Status | Code |
|---|---|---|---|---|---|
| GET | 0x01 | STATUS | 0x08 | OK | 0x00 |
| PUT | 0x02 | REMOVE_NODE | 0x09 | NOT_FOUND | 0x01 |
| DELETE | 0x03 | CLEANUP | 0x0A | ERROR | 0x02 |
| PING | 0x04 | GET/PUT/DELETE_INTERNAL | 0x11–0x13 | PONG | 0x03 |
| EXISTS | 0x05 | MERKLE_TREE / MERKLE_LEAF | 0x14–0x15 | EXISTS_TRUE / _FALSE | 0x04 / 0x05 |
| AUTH | 0x07 | NODE_ID | 0x0B | STALE | 0x06 |
| | | PUT_BATCH_INTERNAL | 0x16 | | |

PUT_BATCH_INTERNAL carries several replica writes in one request, used when writes to a peer
queue up. NODE_ID returns the answering node's id, which is what places it on the ring. STALE answers a PUT
or DELETE that was replicated but is already superseded: a replica holds a
newer version of the key, returned in `TIMESTAMP`. The client library retries with a later version.

Internal commands (0x11 and up) are node-to-node; they need the cluster token and a connection from
a cluster member. REMOVE_NODE and CLEANUP need the cluster token too.

## Architecture

```
          client ──► any node (coordinator)
                         │  hash ring → the key's 3 replicas
           ┌─────────────┼─────────────┐
           ▼             ▼             ▼
        replica A     replica B     replica C       reply after 2 acks (QUORUM)
        WAL + map     WAL + map     (down: hint kept on coordinator)
           ▲             ▲             ▲
           ├── read repair, hinted handoff, Merkle-tree anti-entropy ──┤
           └────────── UDP gossip: membership and failure detection ───┘
```

[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) covers the request paths, threading model, gossip,
clocks, the three convergence mechanisms, WAL recovery and the design trade-offs.

## Performance

Medians of 5 runs on an Apple M3 laptop (8 CPUs) running all nodes and the load generator together
([method, spreads and all scenarios](benchmark/results/benchmark_results.md)):

| Setup | 16 clients, 80% reads | 16 clients, writes only |
|---|---|---|
| 1 node, in-memory | 153,818 ops/sec | 153,601 ops/sec |
| 3 nodes, QUORUM, in-memory | 61,800 ops/sec | 45,582 ops/sec |
| 3 nodes, QUORUM, auth + fsynced WAL | 3,271 ops/sec | 672 ops/sec |

Every read in the benchmark is checked against the client's last acknowledged write: across 80
runs and 37.9M operations there were 0 errors and 0 stale reads. With a node killed mid-run
(`scripts/benchmark-failover.sh`), a production cluster served 131K operations in 40 seconds with
0 errors, and the node recovered from its WAL plus hints and Merkle repair.

How to read these:

- **Three nodes do 40% of one node's throughput** because replication triples the work and all
  nodes share one machine. They are a floor, not a measure of scaling.
- **The durable rows are bound by the disk.** A sync takes about 3 ms on this laptop, three nodes
  share one drive, and every write waits for two of them. On a cloud VM with sub-millisecond
  syncs the same scenario ran at 4,400 writes a second.
- **Throughput at saturation hides latency.** Sent at a fixed rate instead of as fast as possible,
  the 3-node cluster holds a p99 of 0.33 ms at half its maximum throughput and 3.1 ms at 90% of it
  ([latency versus load](benchmark/results/benchmark_results.md#latency-versus-load-open-loop)).
- **The changes since the first version cost nothing measurable.** The build from before them and
  the current one, run alternately on this machine, are within 2% of each other. That comparison
  also found a 23% regression on five nodes that every test had passed
  ([details](benchmark/results/benchmark_results.md#against-the-earlier-build-on-this-machine)).

```bash
./scripts/benchmark-suite.sh      # every scenario, 5 runs each
./scripts/benchmark-latency.sh    # latency at fixed rates (open loop)
./scripts/benchmark-failover.sh   # kill -9 a node under load
./scripts/run-benchmark.sh --protocol
```

## Testing

```bash
./mvnw test       # 249 unit tests
./mvnw verify     # + 89 integration tests on real in-JVM clusters, and a 75% line-coverage gate
```

The integration suite includes:

- **Chaos**: 8 writers at QUORUM on a 5-node production-mode cluster while nodes crash and restart,
  and again with graceful rolling restarts. No read may go back past an acknowledged write, and at
  the end every key must hold a version between its last acknowledged and last attempted write
- production mode (auth, signed gossip, fsynced WAL), 32 concurrent clients with read-your-writes checks
- QUORUM with one and two replicas down, per-request ONE when QUORUM is impossible, node and client
  clocks that are wrong (including clients 30 seconds apart taking turns on one key), racing
  writers, concurrent writes with equal versions
- gossip convergence on 5 nodes, crash detection, graceful shutdown and restart, `removenode` (also
  reaching a node that was cut off at the time), join + `cleanup`, admin commands requiring the cluster token
- a node that restarts empty or joins, held back from reads until it has caught up; frames and
  gossip from another protocol version
- stalled connections, the connection limit, WAL damage in the middle of the log, a failing WAL,
  torn hint files, a full store that must still take deletes, a server with one worker and sixteen
  writers (every request is answered, most of them "busy")
- one node's clock set wrong inside a running cluster: 30 seconds fast, it ends a 20-second TTL
  for everyone at once; 70 seconds fast, the others drop it and take it back when it is corrected
- restarts with saved membership: a full stop after which only two of five nodes return, a node
  that returns alone, and nodes with nothing saved restarting together
- read repair, hinted handoff, Merkle anti-entropy (including tombstones), TTL, metrics endpoints
- **Network faults**, injected between nodes: a 2 | 3 partition (each side serves exactly the keys it
  holds a quorum of, and refuses the rest), an isolated node, a link cut in one direction, a replica
  that answers a second late, and a second chaos test that mixes all of these with crashes, lost
  replies, and node and client clocks up to 20 seconds wrong, then checks that every replica converges
- **Many writers per key**: eight clients read, write and delete the same twenty keys under those
  faults, every operation is recorded, and the whole history is checked: no read may return
  anything an acknowledged write had already replaced. With the STALE reply deliberately switched
  off, the same test reports hundreds of violations
- **Real process kills**: three nodes as separate JVMs, killed with SIGKILL under that workload

WAL tests cover crash recovery across snapshots, concurrent writers during snapshots, torn writes
(including writes made after recovering from one) and expired values.

## Known limitations

- **Last write wins.** Concurrent writes to the same key resolve by version; the losing write is
  discarded rather than surfaced as a conflict (vector clocks with siblings would expose it).
- **At ONE, client clocks can misorder writes.** A write behind a newer version is only reported if
  an acknowledging replica holds that version, which QUORUM guarantees and ONE does not.
- **No linearizable operations.** There is no compare-and-set or transactions (would need Paxos/Raft per key).
- **Memory-bound storage.** All data lives in memory, backed by the WAL; there is no LSM tree.
- **A join is not atomic.** A joining node answers no reads until it has caught up, which closes
  the window in which it could answer "not found" for data it had not received yet. Two things
  remain. While it catches up, its keys have two answering replicas, so QUORUM reads of them fail
  if one of those is down. And nodes learn of the join a gossip round apart: a coordinator still on
  the old ring after the new node has caught up can acknowledge a write on replicas a reader on the
  new ring does not ask. Cassandra's pending ranges (write to old and new replicas until
  bootstrap completes) close that too.
- **Fixed failure-detection timeouts** (3s/10s) rather than an adaptive phi-accrual detector.
- **Snapshots pause writes** while all entries are written out (when the WAL passes 128 MB).
- **Hints are not fsynced**; a coordinator crash can lose some, which anti-entropy then repairs.
  Hints are also dropped beyond 100,000 or 64 MB per down node.
- **TTL precision is the clock skew between nodes.** Each replica expires a key by its own clock,
  and the first to do so wins: a node whose clock runs fast ends a TTL early for the whole cluster.
- **Gossip digests fit in one UDP packet** (about 30 members with short node IDs; larger clusters
  send a rotating subset each round). A node with nothing saved only trusts a digest that lists
  the whole membership, so a brand-new node cannot join a cluster too large for one packet.
- **A new cluster needs every seed once.** Until a member list has been saved, a node serves only
  when it has learned as many members as it has seeds, so list every initial node as a seed and
  start them all. Nodes without a WAL (dev mode) save nothing and need this after every full stop.
- **Node ids are identities.** Ring positions come from the node id (`--node-id`, by default
  `hostname:port`), so a node must keep its id across restarts. Its address may change
  (`--advertise-host`).
- Traffic is not encrypted (no TLS).

## License

MIT License - see [LICENSE](LICENSE)
