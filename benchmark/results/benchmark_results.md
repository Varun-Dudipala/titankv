# TitanKV Benchmark Results

## Test Environment

- **Hardware**: Apple MacBook Air (M-series)
- **OS**: macOS Darwin 24.6.0
- **Java**: 24
- **Date**: 2026-01-10

## Single Node Benchmarks

### Test Configuration 1: Moderate Load (20 threads)

| Parameter | Value |
|-----------|-------|
| Threads | 20 |
| Operations/Thread | 2,000 |
| Total Operations | 40,000 |
| Read Ratio | 80% |
| Value Size | 100 bytes |
| Server Config | Single node on localhost:9001 |

**Results:**

| Metric | Value |
|--------|-------|
| **Total Throughput** | **113,731 ops/sec** |
| Read Throughput | 91,204 ops/sec |
| Write Throughput | 22,527 ops/sec |
| Average Latency | 0.169 ms |
| Errors | 0 |
| Duration | 0.35 seconds |

### Test Configuration 2: High Load (50 threads)

| Parameter | Value |
|-----------|-------|
| Threads | 50 |
| Operations/Thread | 5,000 |
| Total Operations | 250,000 |
| Read Ratio | 80% |
| Value Size | 100 bytes |
| Server Config | Single node on localhost:9001 |

**Results:**

| Metric | Value |
|--------|-------|
| **Total Throughput** | **219,014 ops/sec** |
| Read Throughput | 175,089 ops/sec |
| Write Throughput | 43,925 ops/sec |
| Average Latency | 0.220 ms |
| Errors | 0 |
| Duration | 1.14 seconds |

## Performance Summary

```
                TitanKV Performance Summary
    ╔══════════════════════════════════════════════════╗
    ║  Peak Throughput:     219,014 ops/sec            ║
    ║  Peak Write Rate:      43,925 ops/sec            ║
    ║  Peak Read Rate:      175,089 ops/sec            ║
    ║  Average Latency:       0.220 ms                 ║
    ║  Error Rate:              0%                     ║
    ╚══════════════════════════════════════════════════╝
```

## Comparison with Design Targets

| Metric | Target | Achieved | Status |
|--------|--------|----------|--------|
| Write ops/sec | 2,000+ | 43,925 | **22x target** |
| Read ops/sec | 5,000+ | 175,089 | **35x target** |
| P99 Latency | < 10ms | ~0.22ms avg | **Well under** |

## Key Performance Factors

1. **NIO-based TCP Server**: Non-blocking I/O handles many connections efficiently
2. **Binary Protocol**: Minimal serialization overhead (13-byte request header)
3. **Connection Pooling**: Reuses connections to reduce handshake overhead
4. **ConcurrentHashMap**: Lock-free reads for high read throughput
5. **Direct ByteBuffers**: Reduced GC pressure during high load

## Notes

- Benchmarks run on localhost (minimal network latency)
- In-memory storage (no disk I/O bottleneck)
- Results may vary based on hardware and network conditions
- Production deployments should account for network latency and persistence overhead
