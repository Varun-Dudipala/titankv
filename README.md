# TitanKV

[![Java](https://img.shields.io/badge/Java-17+-orange?logo=openjdk)](https://openjdk.org/)
[![Maven](https://img.shields.io/badge/Maven-3.6+-red?logo=apachemaven)](https://maven.apache.org/)
[![License](https://img.shields.io/badge/License-MIT-blue)](LICENSE)

A distributed key-value store in Java, built from the ideas in Amazon's Dynamo paper and Apache
Cassandra: consistent hashing, leaderless replication with tunable consistency, gossip-based
membership and failure detection, read repair, and a write-ahead log.

## Scope

**What this is:** a working implementation of core distributed-storage techniques, with tests
that run real multi-node clusters and benchmarks you can reproduce.

**What this is not:** a production database. It has not been externally audited; see
[Known limitations](#known-limitations).

## Features

- **Consistent hashing** with 150 virtual nodes per node (MurmurHash3) and replication factor 3
- **Leaderless replication**: any node coordinates any request (Dynamo-style), so clients need no
  knowledge of data placement
- **Tunable consistency**: ONE / QUORUM / ALL for reads, writes and deletes. QUORUM always means a
  majority of the replication factor, so it fails rather than silently weakening when replicas are down
- **Last-write-wins** conflict resolution with coordinator timestamps; deletes write tombstones,
  so stale replicas cannot resurrect deleted keys
- **Read repair**: reads return once the consistency level is met, then fix stale replicas in the background
- **Gossip membership** (UDP, Cassandra-style heartbeat versions) with HMAC-SHA256 authentication
  and replay protection; nodes are marked SUSPECT after 3s and DEAD after 10s of silence
- **Write-ahead log** with CRC-checked records, snapshots, and group-commit fsync; recovery
  tolerates a torn final write
- **Non-blocking NIO server**: a selector thread plus workers that never block on replicas
- **Custom binary protocol** over TCP with 29-byte request headers; 46–49% fewer bytes per round
  trip than JSON over HTTP for 100-byte values ([measured](benchmark/results/benchmark_results.md))
- **TTL** per key, **data rebalancing** to joining or recovered nodes, **client circuit breaker**,
  and **Prometheus-style metrics** over HTTP

## Quick start

Requires Java 17+ and Maven 3.6+.

```bash
git clone https://github.com/Varun-Dudipala/titankv.git
cd titankv
mvn clean package            # builds target/titankv-1.0.0.jar and runs the unit tests

./scripts/start-cluster.sh   # 3 local nodes on 9001-9003 (dev mode); waits until the cluster forms
java -cp target/titankv-1.0.0.jar com.titankv.TitanKVClient localhost:9001 put greeting hello
java -cp target/titankv-1.0.0.jar com.titankv.TitanKVClient localhost:9003 get greeting
curl localhost:9091/status   # cluster view from node 1
./scripts/stop-cluster.sh
```

Run a single node by hand:

```bash
TITANKV_DEV_MODE=true java -jar target/titankv-1.0.0.jar --port 9001
java -jar target/titankv-1.0.0.jar --port 9002 --seeds localhost:9001 --dev   # join it
```

Without `--dev` / `TITANKV_DEV_MODE=true` a node refuses to start unless a cluster secret is set.

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

The client routes each key to a node by consistent hashing, pools connections, retries on another
node if one fails, and stops sending to a node after 5 consecutive failures (circuit breaker).

## Configuration

Every setting is an environment variable or the equivalent system property (`titankv.dev.mode`, ...).

| Variable | Default | Purpose |
|---|---|---|
| `TITANKV_DEV_MODE` | `false` | Allow running without a cluster secret; disables the WAL by default |
| `TITANKV_CLUSTER_SECRET` | none | Signs gossip (HMAC) and authenticates node-to-node commands |
| `TITANKV_CLIENT_TOKEN` | none | If set, clients must authenticate with this token |
| `TITANKV_READ_CONSISTENCY` / `_WRITE_` / `_DELETE_` | `QUORUM` | `ONE`, `QUORUM` or `ALL` |
| `TITANKV_DATA_DIR` | `data` | WAL and snapshots go in `<dir>/node-<port>` |
| `TITANKV_WAL_ENABLED` | on unless dev mode | Write-ahead log |
| `TITANKV_WAL_FSYNC` | `true` | fsync before acknowledging a write (group-committed) |
| `TITANKV_WAL_MAX_MB` | `128` | WAL size that triggers a snapshot |
| `TITANKV_MAX_MEMORY_MB` | `512` | Writes beyond this are rejected (never silently evicted) |
| `TITANKV_TOMBSTONE_GRACE_MS` | `86400000` | How long deleted-key tombstones are kept |
| `TITANKV_METRICS_PORT` | node port + 90 | `/metrics`, `/health`, `/ready`, `/status` |
| `TITANKV_WORKER_THREADS` | max(4, CPUs) | Request worker threads |
| `TITANKV_REPLICATION_THREADS` | max(16, 4 × CPUs) | Threads for replica I/O |

Ports per node: client TCP on `--port`, gossip UDP on port + 1000, metrics HTTP on port + 90.

### Consistency levels

With replication factor 3:

| Level | Acknowledgements | Tolerates |
|---|---|---|
| ONE | 1 | 2 replicas down, may read stale data |
| QUORUM | 2 | 1 replica down; since 2 + 2 > 3, a QUORUM read overlaps every acknowledged QUORUM write |
| ALL | 3 | no replica down |

## Binary protocol

```
Request:  [MAGIC:4][CMD:1][KEY_LEN:4][VAL_LEN:4][TIMESTAMP:8][EXPIRES:8][KEY][VALUE]
Response: [MAGIC:4][STATUS:1][VAL_LEN:4][TIMESTAMP:8][EXPIRES:8][VALUE]
```

Big-endian. `VAL_LEN` of -1 means no value (distinct from an empty value). For a client PUT,
`EXPIRES` carries the TTL in milliseconds; replication messages carry absolute expiry times.

| Command | Code | | Status | Code |
|---|---|---|---|---|
| GET | 0x01 | | OK | 0x00 |
| PUT | 0x02 | | NOT_FOUND | 0x01 |
| DELETE | 0x03 | | ERROR | 0x02 |
| PING | 0x04 | | PONG | 0x03 |
| EXISTS | 0x05 | | EXISTS_TRUE | 0x04 |
| AUTH | 0x07 | | EXISTS_FALSE | 0x05 |
| GET/PUT/DELETE_INTERNAL | 0x11–0x13 | | | |

Internal commands are node-to-node replica operations; they require the cluster token and a
connection from a known cluster member.

## Architecture

```
            client ──► any node (coordinator)
                           │  consistent hash → 3 replicas
             ┌─────────────┼─────────────┐
             ▼             ▼             ▼
          replica A     replica B     replica C      ack after 2 of 3 (QUORUM)
          WAL + map     WAL + map     WAL + map
             ▲             ▲             ▲
             └──── UDP gossip: membership, heartbeats ────┘
```

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the write and read paths, the threading
model, gossip and failure detection, WAL recovery, and design trade-offs.

## Performance

On a 4-vCPU VM running all nodes and the load generator together
([details and method](benchmark/results/benchmark_results.md)):

| Setup | 16 clients, 80% reads | 16 clients, writes only |
|---|---|---|
| 1 node, in-memory | 51,262 ops/sec | 48,728 ops/sec |
| 3 nodes, QUORUM, in-memory | 15,339 ops/sec | 18,981 ops/sec |
| 3 nodes, QUORUM, auth + fsynced WAL | 12,171 ops/sec | 4,691 ops/sec |

```bash
./scripts/run-benchmark.sh --hosts localhost:9001,localhost:9002,localhost:9003 --threads 16
./scripts/run-benchmark.sh --protocol     # binary protocol vs JSON-over-HTTP byte counts
```

## Testing

```bash
mvn test       # unit tests
mvn verify     # unit tests + integration tests that start real in-JVM clusters
mvn test jacoco:report   # coverage report in target/site/jacoco/index.html
```

Integration tests cover production mode (authentication, signed gossip, fsynced WAL), 32 concurrent
clients with read-your-writes checks, QUORUM behaviour with one and two nodes down, a 5-node cluster
converging through gossip, crash detection and recovery, graceful leave, read repair (including
tombstones beating stale values), and TTL. WAL tests cover crash recovery across snapshots,
concurrent writers during snapshots, and torn writes.

## Known limitations

- **Clock-based conflict resolution.** Last-write-wins uses the coordinator's wall clock; with clock
  skew a later write can lose to an earlier one. Vector clocks or hybrid logical clocks would fix this.
- **Sloppy replica selection.** When a replica is down, its keys are written to the next live node
  on the ring, so the read/write overlap guarantee of QUORUM holds only while membership is stable.
- **No hinted handoff or Merkle-tree anti-entropy.** A replica that misses writes catches up through
  read repair and when it rejoins (data rebalancing streams keys to it), not continuously.
- **Dead nodes stay members** until they return; there is no command to decommission one.
- **Memory-bound.** All data lives in memory, backed by the WAL; there is no LSM tree.
- **Gossip digests fit in one UDP packet**, about 30 members with short node IDs; larger clusters
  send a rotating subset each round.
- Node-to-node and client traffic is not encrypted (no TLS).

## Future work

- [ ] Hybrid logical clocks for conflict resolution
- [ ] Hinted handoff and Merkle-tree anti-entropy
- [ ] LSM-tree storage for data larger than memory
- [ ] TLS for client and internal traffic

## License

MIT License - see [LICENSE](LICENSE)
