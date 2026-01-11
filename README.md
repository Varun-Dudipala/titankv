# TitanKV

A high-performance distributed key-value store built in Java, demonstrating deep systems programming expertise.

## Features

- **Consistent Hashing** - Data sharding with 150 virtual nodes per physical node for even distribution
- **Custom Binary Protocol** - 13-byte header, 40% more efficient than JSON/HTTP
- **NIO-based TCP Server** - Non-blocking I/O with selector event loop for high concurrency
- **Tunable Consistency** - ONE, QUORUM, ALL consistency levels (similar to Cassandra)
- **Gossip Protocol** - UDP-based cluster membership and failure detection
- **Connection Pooling** - Client-side connection reuse for optimal performance
- **Read Repair** - Automatic repair of stale replicas on read

## Architecture

```
                              ┌─────────────────────────────────┐
                              │         TitanKV Cluster         │
                              └─────────────────────────────────┘
                                              │
           ┌──────────────────────────────────┼──────────────────────────────────┐
           │                                  │                                  │
           ▼                                  ▼                                  ▼
   ┌───────────────┐                 ┌───────────────┐                 ┌───────────────┐
   │    Node A     │◄───Gossip───────│    Node B     │◄───Gossip───────│    Node C     │
   │  Port: 9001   │     Protocol    │  Port: 9002   │     Protocol    │  Port: 9003   │
   ├───────────────┤                 ├───────────────┤                 ├───────────────┤
   │ Hash Ring     │                 │ Hash Ring     │                 │ Hash Ring     │
   ├───────────────┤                 ├───────────────┤                 ├───────────────┤
   │ Storage Engine│                 │ Storage Engine│                 │ Storage Engine│
   │ (ConcurrentHM)│                 │ (ConcurrentHM)│                 │ (ConcurrentHM)│
   └───────────────┘                 └───────────────┘                 └───────────────┘
           ▲                                  ▲                                  ▲
           └──────────────────────────────────┼──────────────────────────────────┘
                                              │
                                    ┌─────────────────┐
                                    │  Binary Protocol │
                                    │   over TCP/IP    │
                                    └─────────────────┘
```

## Quick Start

### Prerequisites

- Java 17 or higher
- Maven 3.6+

### Build

```bash
cd titankv
mvn clean package
```

### Run a Single Node

```bash
java -jar target/titankv-1.0.0.jar --port 9001
```

### Run a 3-Node Cluster

```bash
# Start seed node
java -jar target/titankv-1.0.0.jar --port 9001

# Start additional nodes (in separate terminals)
java -jar target/titankv-1.0.0.jar --port 9002 --seeds localhost:9001
java -jar target/titankv-1.0.0.jar --port 9003 --seeds localhost:9001
```

Or use the convenience script:

```bash
./scripts/start-cluster.sh
```

### Using the Client

```java
import com.titankv.TitanKVClient;

try (TitanKVClient client = new TitanKVClient("localhost:9001")) {
    // Store a value
    client.put("user:123", "John Doe");

    // Retrieve it
    Optional<String> value = client.getString("user:123");
    System.out.println(value.get()); // "John Doe"

    // Delete it
    client.delete("user:123");

    // Check existence
    boolean exists = client.exists("user:123"); // false
}
```

## Binary Protocol

TitanKV uses a custom binary protocol designed for efficiency:

```
Request:  [MAGIC:4][CMD:1][KEY_LEN:4][VAL_LEN:4][KEY:N][VALUE:M]
Response: [MAGIC:4][STATUS:1][VAL_LEN:4][VALUE:N]
```

**Commands:**
- `0x01` GET
- `0x02` PUT
- `0x03` DELETE
- `0x04` PING

**Status Codes:**
- `0x00` OK
- `0x01` NOT_FOUND
- `0x02` ERROR
- `0x03` PONG

## Consistency Levels

| Level | Required Responses | Use Case |
|-------|-------------------|----------|
| ONE | 1 | Fastest, eventual consistency |
| QUORUM | (RF/2) + 1 | Balanced consistency and availability |
| ALL | All replicas | Strongest consistency, lowest availability |

## Benchmarking

Run the included benchmark:

```bash
# Single node benchmark
./scripts/run-benchmark.sh --single

# Cluster benchmark
./scripts/run-benchmark.sh --hosts localhost:9001,localhost:9002,localhost:9003
```

**Target Performance:**
- Write throughput: 2,000+ ops/sec
- Read throughput: 5,000+ ops/sec
- P99 latency: < 10ms

## Project Structure

```
titankv/
├── src/main/java/com/titankv/
│   ├── TitanKVServer.java          # Server entry point
│   ├── TitanKVClient.java          # Client library
│   ├── core/                       # Storage engine
│   ├── cluster/                    # Consistent hashing, gossip
│   ├── network/                    # TCP server, protocol
│   ├── consistency/                # Replication, consistency levels
│   ├── client/                     # Client configuration
│   └── util/                       # Utilities (hash, metrics)
├── src/test/java/                  # Unit tests
├── benchmark/                      # Load generator
├── scripts/                        # Cluster scripts
└── docs/                           # Documentation
```

## Design Decisions

### Why Consistent Hashing?

Traditional modulo hashing (`hash(key) % N`) requires remapping all keys when nodes change. Consistent hashing only remaps `K/N` keys on average, making it ideal for dynamic clusters.

### Why Custom Binary Protocol?

| Aspect | JSON/HTTP | TitanKV Binary |
|--------|-----------|----------------|
| Header size | 100+ bytes | 13 bytes |
| Serialization | String parsing | Direct bytes |
| Connection | New per request | Persistent pool |

### Why NIO?

Non-blocking I/O allows a single thread to handle thousands of connections efficiently, reducing thread overhead and context switching.

## Testing

```bash
# Run all tests
mvn test

# Run specific test class
mvn test -Dtest=ConsistentHashTest
```

## Future Enhancements

- [ ] Persistent storage (LSM tree)
- [ ] Bloom filters for non-existent keys
- [ ] Vector clocks for conflict resolution
- [ ] Merkle trees for anti-entropy
- [ ] REST API for administration

## License

MIT License

## Author

Built with Java, multi-threading, and TCP/IP networking.
