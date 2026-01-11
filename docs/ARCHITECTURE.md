# TitanKV Architecture

This document describes the internal architecture and design decisions of TitanKV.

## System Overview

TitanKV is a distributed key-value store designed for high throughput and low latency. It follows a shared-nothing architecture where each node is independent and communicates via a gossip protocol.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                             TitanKV Node                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │  TCP Server │  │   Cluster   │  │ Replication │  │   Storage   │        │
│  │    (NIO)    │  │   Manager   │  │   Manager   │  │   Engine    │        │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘        │
│         │                │                │                │                │
│         ▼                ▼                ▼                ▼                │
│  ┌─────────────────────────────────────────────────────────────────┐       │
│  │                     Consistent Hash Ring                         │       │
│  └─────────────────────────────────────────────────────────────────┘       │
│         │                │                │                │                │
│         ▼                ▼                ▼                ▼                │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │   Binary    │  │   Gossip    │  │ Consistency │  │ ConcurrentHM│        │
│  │  Protocol   │  │  Protocol   │  │   Levels    │  │    Store    │        │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘        │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Storage Engine (`com.titankv.core`)

The storage engine uses a `ConcurrentHashMap` for thread-safe in-memory storage.

**Key Classes:**
- `KVStore` - Interface defining storage operations
- `InMemoryStore` - ConcurrentHashMap implementation
- `KeyValuePair` - Value wrapper with timestamp and TTL

**Features:**
- Thread-safe concurrent access
- TTL support with automatic expiration
- Timestamp-based conflict resolution

```java
public interface KVStore {
    Optional<KeyValuePair> put(String key, byte[] value);
    Optional<KeyValuePair> get(String key);
    Optional<KeyValuePair> delete(String key);
    boolean exists(String key);
}
```

### 2. Network Layer (`com.titankv.network`)

**TCP Server:**
- NIO-based with Selector for non-blocking I/O
- Single event loop thread for accepting connections
- Worker thread pool for request processing

```
┌──────────────────────────────────────────────────────────────┐
│                        TCP Server                             │
├──────────────────────────────────────────────────────────────┤
│  ┌─────────────┐    ┌─────────────────────────────────────┐  │
│  │   Selector  │───▶│         Connection Handlers          │  │
│  │ Event Loop  │    │  (per-connection read/write buffers) │  │
│  └─────────────┘    └─────────────────────────────────────┘  │
│         │                          │                          │
│         ▼                          ▼                          │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │               Worker Thread Pool                         │ │
│  │           (command processing)                           │ │
│  └─────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────┘
```

**Binary Protocol:**
- 13-byte fixed header for requests
- 9-byte fixed header for responses
- Magic bytes for protocol validation
- Length-prefixed variable fields

### 3. Consistent Hashing (`com.titankv.cluster`)

**Hash Ring Implementation:**
- Uses MurmurHash3 for key hashing
- 150 virtual nodes per physical node
- `ConcurrentSkipListMap` for O(log n) lookups

```
                    Hash Ring (0 to 2^64)

                    ┌───────────────┐
                 ╱  │   VNode A-42  │  ╲
               ╱    └───────────────┘    ╲
             ╱                              ╲
    ┌───────────────┐              ┌───────────────┐
    │   VNode C-87  │              │   VNode B-15  │
    └───────────────┘              └───────────────┘
             ╲                              ╱
               ╲    ┌───────────────┐    ╱
                 ╲  │   VNode A-91  │  ╱
                    └───────────────┘

    Key "user:123" → hash → find next VNode clockwise → Node A
```

**Key Distribution:**
- Keys are distributed evenly across nodes
- Adding/removing nodes only remaps ~K/N keys
- Virtual nodes ensure balanced distribution

### 4. Cluster Management (`com.titankv.cluster`)

**Gossip Protocol:**
- UDP-based for lightweight communication
- Heartbeat interval: 1 second
- Fanout: 3 nodes per gossip round

**Node States:**
```
JOINING → ALIVE → SUSPECT → DEAD
                     ↓
                  LEAVING
```

**Failure Detection:**
- Suspect after 3 missed heartbeats (3s)
- Dead after 10 seconds
- Read repair triggers automatic recovery

### 5. Consistency & Replication (`com.titankv.consistency`)

**Consistency Levels:**

| Level | Formula | Behavior |
|-------|---------|----------|
| ONE | 1 | Return after first response |
| QUORUM | (RF/2)+1 | Majority must respond |
| ALL | RF | All replicas must respond |

**Write Path:**
```
Client → Primary Node → Parallel writes to RF replicas
                     → Return when consistency level met
```

**Read Path:**
```
Client → Primary Node → For ONE: return immediately
                     → For QUORUM/ALL: read from multiple, compare
                     → Trigger read repair if inconsistent
```

## Data Flow

### Write Operation

```
1. Client sends PUT command
2. Client hashes key, determines primary node
3. Request sent to primary via TCP
4. Primary decodes binary protocol
5. Primary writes to local store
6. If replication enabled:
   a. Primary sends to RF-1 replicas in parallel
   b. Wait for consistency level requirement
7. Response sent to client
```

### Read Operation

```
1. Client sends GET command
2. Client hashes key, determines primary node
3. Request sent to primary via TCP
4. Primary reads from local store
5. If consistency > ONE:
   a. Read from additional replicas
   b. Compare values
   c. Trigger read repair if needed
6. Response sent to client
```

## Thread Model

```
┌────────────────────────────────────────────────────────────┐
│                      Thread Pools                           │
├────────────────────────────────────────────────────────────┤
│                                                             │
│  TCP Server:                                                │
│  ┌─────────────┐  ┌─────────────────────────────────────┐  │
│  │ Selector    │  │     Worker Pool (4 threads)          │  │
│  │ (1 thread)  │  │     - Command processing             │  │
│  └─────────────┘  └─────────────────────────────────────┘  │
│                                                             │
│  Cluster:                                                   │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐    │
│  │ Gossip Recv │  │ Gossip Send │  │  Health Check   │    │
│  │ (1 thread)  │  │ (1 thread)  │  │   (1 thread)    │    │
│  └─────────────┘  └─────────────┘  └─────────────────┘    │
│                                                             │
│  Replication:                                               │
│  ┌─────────────────────────────────────────────────────┐  │
│  │        Replicator Pool (16 threads)                  │  │
│  │        - Parallel replica writes/reads               │  │
│  └─────────────────────────────────────────────────────┘  │
│                                                             │
└────────────────────────────────────────────────────────────┘
```

## Performance Considerations

### Memory Management

- **ByteBufferPool**: Pre-allocated direct buffers reduce GC pressure
- **ThreadLocal buffers**: Each thread has its own buffer
- **Object pooling**: Connection objects reused

### Network Efficiency

- **Connection pooling**: Persistent connections to each node
- **Binary protocol**: Minimal serialization overhead
- **TCP_NODELAY**: Disabled Nagle's algorithm for low latency

### Concurrency

- **Lock-free structures**: ConcurrentHashMap, ConcurrentSkipListMap
- **Non-blocking I/O**: Single thread handles many connections
- **Async replication**: Parallel writes to replicas

## Future Improvements

1. **Persistent Storage**
   - Log-structured merge tree (LSM)
   - Write-ahead log for durability

2. **Enhanced Failure Detection**
   - Phi-accrual failure detector
   - More accurate suspicion levels

3. **Conflict Resolution**
   - Vector clocks for causality tracking
   - Last-writer-wins with timestamps

4. **Anti-Entropy**
   - Merkle trees for efficient sync
   - Scheduled background repair
