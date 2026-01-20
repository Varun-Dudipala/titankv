# TitanKV

[![Java](https://img.shields.io/badge/Java-17+-orange?logo=openjdk)](https://openjdk.org/)
[![Maven](https://img.shields.io/badge/Maven-3.6+-red?logo=apachemaven)](https://maven.apache.org/)
[![License](https://img.shields.io/badge/License-MIT-blue)](LICENSE)
[![Tests](https://img.shields.io/badge/Tests-170%20passing-brightgreen)]()

A distributed key-value store built in Java, implementing core concepts from Amazon DynamoDB and Apache Cassandra.

## Scope

**What this is:** An educational implementation of distributed systems concepts including consistent hashing, quorum replication, gossip protocols, and read repair.

**What this is not:** A production-ready database. This project has not been externally audited and should not be used to store sensitive data.

## Features

- **Consistent Hashing** with 150 virtual nodes per physical node
- **Binary Protocol** with 29-byte request headers (vs ~100+ bytes for JSON/HTTP)
- **NIO TCP Server** using selector-based event loop
- **Tunable Consistency** (ONE / QUORUM / ALL)
- **Gossip Protocol** for cluster membership with HMAC authentication
- **Read Repair** to fix stale replicas automatically
- **Circuit Breaker** pattern for fault tolerance
- **Write-Ahead Log** for crash recovery

## Quick Start

### Prerequisites

- Java 17+
- Maven 3.6+

### Build and Test

```bash
git clone https://github.com/Varun-Dudipala/titankv.git
cd titankv
mvn clean package

# Run tests
mvn test
```

### Run Single Node

```bash
TITANKV_DEV_MODE=true java -jar target/titankv-1.0.0.jar --port 9001
```

### Run 3-Node Cluster

```bash
# Terminal 1 - Seed node
TITANKV_DEV_MODE=true java -jar target/titankv-1.0.0.jar --port 9001

# Terminal 2
TITANKV_DEV_MODE=true java -jar target/titankv-1.0.0.jar --port 9002 --seeds localhost:9001

# Terminal 3
TITANKV_DEV_MODE=true java -jar target/titankv-1.0.0.jar --port 9003 --seeds localhost:9001
```

Or use: `./scripts/start-cluster.sh`

### Client Usage

```java
try (TitanKVClient client = new TitanKVClient("localhost:9001")) {
    client.put("user:123", "John Doe");
    Optional<String> value = client.getString("user:123");
    client.delete("user:123");
}
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      TitanKV Cluster                        │
├───────────────────┬───────────────────┬─────────────────────┤
│      Node A       │      Node B       │      Node C         │
│    Port: 9001     │    Port: 9002     │    Port: 9003       │
├───────────────────┼───────────────────┼─────────────────────┤
│   Hash Ring       │   Hash Ring       │   Hash Ring         │
│   Storage Engine  │   Storage Engine  │   Storage Engine    │
└─────────┬─────────┴─────────┬─────────┴──────────┬──────────┘
          │                   │                    │
          └───────── Gossip Protocol ──────────────┘
```

## Configuration

### Security

**Never commit secrets.** Use environment variables:

```bash
export TITANKV_CLUSTER_SECRET="$(openssl rand -base64 32)"
export TITANKV_CLIENT_TOKEN="your-client-token"
```

### Consistency Levels

| Level | Required Responses | Trade-off |
|-------|-------------------|-----------|
| ONE | 1 | Fast, eventual consistency |
| QUORUM | (RF/2) + 1 | Balanced |
| ALL | All replicas | Strong consistency, lower availability |

```bash
export TITANKV_READ_CONSISTENCY=QUORUM
export TITANKV_WRITE_CONSISTENCY=QUORUM
```

## Binary Protocol

```
Request:  [MAGIC:4][CMD:1][KEY_LEN:4][VAL_LEN:4][TS:8][EXP:8][KEY][VALUE]
Response: [MAGIC:4][STATUS:1][VAL_LEN:4][TS:8][EXP:8][VALUE]
```

| Command | Code | | Status | Code |
|---------|------|-|--------|------|
| GET | 0x01 | | OK | 0x00 |
| PUT | 0x02 | | NOT_FOUND | 0x01 |
| DELETE | 0x03 | | ERROR | 0x02 |
| PING | 0x04 | | PONG | 0x03 |

## Project Structure

```
titankv/
├── src/main/java/com/titankv/
│   ├── TitanKVServer.java        # Server entry point
│   ├── TitanKVClient.java        # Client library
│   ├── core/                     # Storage engine
│   ├── cluster/                  # Gossip, consistent hashing
│   ├── network/                  # NIO server, binary protocol
│   └── consistency/              # Replication, read repair
├── src/test/java/                # 170 tests
├── benchmark/                    # Load generator
└── scripts/                      # Cluster management
```

## Benchmarking

```bash
./scripts/run-benchmark.sh --single --threads 20 --ops 2000
```

See `benchmark/results/` for methodology and raw output.

## Testing

```bash
# All tests
mvn test

# Specific test
mvn test -Dtest=ConsistentHashTest

# With coverage report
mvn test jacoco:report
# Report: target/site/jacoco/index.html
```

## Design Decisions

**Why Consistent Hashing?** Traditional `hash(key) % N` remaps all keys when N changes. Consistent hashing only remaps K/N keys on average.

**Why Binary Protocol?** Lower serialization overhead and persistent connections vs HTTP/JSON.

**Why NIO?** Single thread handles thousands of connections without thread-per-connection overhead.

## Future Work

- [ ] LSM tree for persistent storage
- [ ] Bloom filters for negative lookups
- [ ] Merkle trees for anti-entropy
- [ ] Kubernetes operator

## License

MIT License - See [LICENSE](LICENSE)
