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
- **Leaderless**: any node coordinates any request, as in Dynamo; clients need no knowledge of placement
- **Tunable consistency** (ONE / QUORUM / ALL) with **strict quorums**: a key's replicas never change
  because a node is down, so R + W > N guarantees QUORUM reads see QUORUM writes

**Convergence**
- **Read repair**: reads return once the consistency level is met, then fix stale replicas in the background
- **Hinted handoff**: writes for a down replica are kept (persisted, coalesced per key) and delivered when it returns
- **Merkle-tree anti-entropy**: replica pairs compare 1024-leaf hash trees every minute and sync only the keys that differ
- **Last-write-wins with hybrid logical clocks**, plus client causal context, so clock skew cannot make a
  client's later write lose to its earlier one; deletes are tombstones, so nothing is resurrected

**Membership and operations**
- **Gossip** (UDP, Cassandra-style heartbeat digests) with HMAC-SHA256 authentication and replay
  protection; nodes are SUSPECT after 3s and DEAD after 10s of silence
- **Admin**: `removenode` for dead nodes, `cleanup` after joins, `status`, rebalancing to joining nodes
- **Metrics** over HTTP (`/metrics` in Prometheus text format, `/health`, `/ready`, `/status`)

**Storage and networking**
- **Write-ahead log** with CRC-checked records, snapshots, and **group-commit** fsync
- **Non-blocking NIO server**; worker threads never wait on replicas
- **Binary protocol** over TCP with 29-byte request headers: 46–49% fewer bytes per round trip than
  JSON over HTTP for 100-byte values ([measured](benchmark/results/benchmark_results.md#protocol-size-vs-json-over-http))
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

Requires Java 17+ and Maven 3.6+.

```bash
mvn clean package                       # builds target/titankv-1.0.0.jar, runs unit tests
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
}
```

The client spreads requests across nodes by consistent hashing, pools connections, fails over to
another node on error, and tracks the newest version it has seen so its writes are always ordered
after what it has read or written (see [clocks](docs/ARCHITECTURE.md#versions-and-clocks)).

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
| It comes back | Gossip sees a newer heartbeat; hints are delivered; if the process restarted, peers also stream it their keys; read repair and anti-entropy fix anything left. |
| Two of a key's three replicas are down | QUORUM and ALL fail with "Not enough replicas" instead of accepting a write on one copy; ONE still works. |
| A node is gone for good | `removenode <id>` drops it cluster-wide; anti-entropy re-replicates its keys to their new replicas. |
| A restarted node has not found the cluster yet | It rejects client requests (`/ready` is 503) rather than acting as a one-node cluster. |
| Network partition | Each side serves keys with enough reachable replicas; afterwards hints, read repair and anti-entropy converge them. |
| Crash mid-write | The WAL is fsynced before acknowledging; recovery replays snapshot + WAL and ignores a torn final record. |
| Clock skew | Hybrid logical clocks never go backwards past a version already seen; clients carry causal context. |
| Concurrent writes to one key | Last write wins by (timestamp, value); no conflict is surfaced to the application. |

## Configuration

Every setting is an environment variable or the equivalent system property (`titankv.dev.mode`, ...).

| Variable | Default | Purpose |
|---|---|---|
| `TITANKV_DEV_MODE` | `false` | Allow running without a cluster secret; disables the WAL by default |
| `TITANKV_CLUSTER_SECRET` | none | Signs gossip (HMAC) and authenticates node-to-node commands |
| `TITANKV_CLIENT_TOKEN` | none | If set, clients must authenticate with this token |
| `TITANKV_READ_CONSISTENCY` / `_WRITE_` / `_DELETE_` | `QUORUM` | `ONE`, `QUORUM` or `ALL` |
| `TITANKV_DATA_DIR` | `data` | WAL, snapshots and hints go in `<dir>/node-<port>` |
| `TITANKV_WAL_ENABLED` | on unless dev mode | Write-ahead log |
| `TITANKV_WAL_FSYNC` | `true` | fsync before acknowledging a write (group-committed) |
| `TITANKV_WAL_MAX_MB` | `128` | WAL size that triggers a snapshot |
| `TITANKV_MAX_MEMORY_MB` | `512` | Writes beyond this are rejected (never silently evicted) |
| `TITANKV_TOMBSTONE_GRACE_MS` | `86400000` | How long deleted-key tombstones are kept |
| `TITANKV_ANTI_ENTROPY_INTERVAL_MS` | `60000` | Merkle-tree repair with a random peer; `0` disables |
| `TITANKV_METRICS_PORT` | node port + 90 | `/metrics`, `/health`, `/ready`, `/status` |
| `TITANKV_WORKER_THREADS` | max(4, CPUs) | Request worker threads |
| `TITANKV_REPLICATION_THREADS` | max(16, 4 × CPUs) | Threads for replica I/O |

Ports per node: client TCP on `--port`, gossip UDP on port + 1000, metrics HTTP on port + 90.

## Binary protocol

```
Request:  [MAGIC:4][CMD:1][KEY_LEN:4][VAL_LEN:4][TIMESTAMP:8][EXPIRES:8][KEY][VALUE]
Response: [MAGIC:4][STATUS:1][VAL_LEN:4][TIMESTAMP:8][EXPIRES:8][VALUE]
```

Big-endian; `VAL_LEN` of -1 means no value (distinct from an empty value). For client PUT/DELETE,
`TIMESTAMP` is optional causal context and `EXPIRES` carries the TTL in milliseconds; responses to
PUT/DELETE return the version assigned. Replica messages carry absolute versions and expiry times.

| Command | Code | Command | Code | Status | Code |
|---|---|---|---|---|---|
| GET | 0x01 | STATUS | 0x08 | OK | 0x00 |
| PUT | 0x02 | REMOVE_NODE | 0x09 | NOT_FOUND | 0x01 |
| DELETE | 0x03 | CLEANUP | 0x0A | ERROR | 0x02 |
| PING | 0x04 | GET/PUT/DELETE_INTERNAL | 0x11–0x13 | PONG | 0x03 |
| EXISTS | 0x05 | MERKLE_TREE / MERKLE_LEAF | 0x14–0x15 | EXISTS_TRUE / _FALSE | 0x04 / 0x05 |
| AUTH | 0x07 | | | | |

Internal commands (0x11 and up) are node-to-node; they need the cluster token and a connection from
a cluster member.

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

Medians of 5 runs on a 4-vCPU VM running all nodes and the load generator together
([method, spreads and all scenarios](benchmark/results/benchmark_results.md)):

| Setup | 16 clients, 80% reads | 16 clients, writes only |
|---|---|---|
| 1 node, in-memory | 61,103 ops/sec | 59,194 ops/sec |
| 3 nodes, QUORUM, in-memory | 22,560 ops/sec | 22,494 ops/sec |
| 3 nodes, QUORUM, auth + fsynced WAL | 15,557 ops/sec | 5,864 ops/sec |

Every read in the benchmark is checked against the client's last acknowledged write: across 80
runs and 14.6M operations there were 0 errors and 0 stale reads. With a node killed mid-run
(`scripts/benchmark-failover.sh`), a production cluster served 538K operations in 40 seconds with
0 errors, recovered the node from its WAL, and repaired only the 1,045 keys it missed.

```bash
./scripts/benchmark-suite.sh      # every scenario, 5 runs each
./scripts/benchmark-failover.sh   # kill -9 a node under load
./scripts/run-benchmark.sh --protocol
```

## Testing

```bash
mvn test       # 179 unit tests
mvn verify     # + 52 integration tests on real in-JVM clusters, and a 75% line-coverage gate
```

The integration suite includes:

- **Chaos**: 8 writers at QUORUM on a 5-node production-mode cluster while nodes crash and restart;
  every key must hold a version between its last acknowledged and last attempted write
- production mode (auth, signed gossip, fsynced WAL), 32 concurrent clients with read-your-writes checks
- QUORUM with one and two replicas down, clock skew with and without causal context
- gossip convergence on 5 nodes, crash detection, restart, graceful leave, `removenode`, join + `cleanup`
- read repair, hinted handoff, Merkle anti-entropy (including tombstones), TTL, metrics endpoints

WAL tests cover crash recovery across snapshots, concurrent writers during snapshots and torn writes.

## Known limitations

- **Last write wins.** Concurrent writes to the same key resolve by version; the losing write is
  discarded rather than surfaced as a conflict (vector clocks with siblings would expose it).
- **No linearizable operations.** There is no compare-and-set or transactions (would need Paxos/Raft per key).
- **Memory-bound storage.** All data lives in memory, backed by the WAL; there is no LSM tree.
- **Fixed failure-detection timeouts** (3s/10s) rather than an adaptive phi-accrual detector.
- **Hints are not fsynced**; a coordinator crash can lose some, which anti-entropy then repairs.
- **Gossip digests fit in one UDP packet** (about 30 members with short node IDs; larger clusters
  send a rotating subset each round).
- Traffic is not encrypted (no TLS).

## License

MIT License - see [LICENSE](LICENSE)
