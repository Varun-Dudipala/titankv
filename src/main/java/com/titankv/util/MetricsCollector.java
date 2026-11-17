package com.titankv.util;

import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Request metrics for one node: client operation counts and latencies, read hits and misses,
 * errors, open connections, and replica traffic from other nodes. Exported in Prometheus format
 * by {@link MetricsHttpServer}.
 */
public class MetricsCollector {

    private final MeterRegistry registry;

    private final Counter getOps;
    private final Counter putOps;
    private final Counter deleteOps;
    private final Counter readHits;
    private final Counter readMisses;
    private final Counter errors;
    private final Counter internalGets;
    private final Counter internalWrites;

    private final Timer getLatency;
    private final Timer putLatency;
    private final Timer deleteLatency;

    private final LongAdder activeConnections = new LongAdder();

    public MetricsCollector() {
        this(new SimpleMeterRegistry());
    }

    public MetricsCollector(MeterRegistry registry) {
        this.registry = registry;
        this.getOps = operations("get");
        this.putOps = operations("put");
        this.deleteOps = operations("delete");
        this.readHits = Counter.builder("titankv.reads").tag("result", "hit")
                .description("Client reads that found the key").register(registry);
        this.readMisses = Counter.builder("titankv.reads").tag("result", "miss")
                .description("Client reads that did not find the key").register(registry);
        this.errors = Counter.builder("titankv.errors")
                .description("Requests answered with an error").register(registry);
        this.internalGets = Counter.builder("titankv.replica.ops").tag("operation", "read")
                .description("Replica reads served for other coordinators").register(registry);
        this.internalWrites = Counter.builder("titankv.replica.ops").tag("operation", "write")
                .description("Replica writes applied for other coordinators").register(registry);
        this.getLatency = latency("get");
        this.putLatency = latency("put");
        this.deleteLatency = latency("delete");
        Gauge.builder("titankv.connections", activeConnections, LongAdder::sum)
                .description("Open client and node connections").register(registry);
    }

    private Counter operations(String operation) {
        return Counter.builder("titankv.ops").tag("operation", operation)
                .description("Client operations coordinated by this node").register(registry);
    }

    private Timer latency(String operation) {
        return Timer.builder("titankv.latency").tag("operation", operation)
                .description("Client operation latency, including replication")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);
    }

    public void recordGet(long durationNanos, boolean hit) {
        getOps.increment();
        getLatency.record(durationNanos, TimeUnit.NANOSECONDS);
        (hit ? readHits : readMisses).increment();
    }

    public void recordPut(long durationNanos) {
        putOps.increment();
        putLatency.record(durationNanos, TimeUnit.NANOSECONDS);
    }

    public void recordDelete(long durationNanos) {
        deleteOps.increment();
        deleteLatency.record(durationNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Count a replica read or write sent by another node's coordinator. Kept apart from client
     * operations so they are not counted twice across the cluster.
     */
    public void recordReplicaRead() {
        internalGets.increment();
    }

    public void recordReplicaWrite() {
        internalWrites.increment();
    }

    public void recordError() {
        errors.increment();
    }

    public void connectionOpened() {
        activeConnections.increment();
    }

    public void connectionClosed() {
        activeConnections.decrement();
    }

    public long getTotalGetOps() {
        return (long) getOps.count();
    }

    public long getTotalPutOps() {
        return (long) putOps.count();
    }

    public long getTotalDeleteOps() {
        return (long) deleteOps.count();
    }

    public long getTotalErrors() {
        return (long) errors.count();
    }

    public long getActiveConnections() {
        return activeConnections.sum();
    }

    /**
     * @return the share of client reads that found their key
     */
    public double getHitRate() {
        double hits = readHits.count();
        double total = hits + readMisses.count();
        return total > 0 ? hits / total : 0.0;
    }

    public double getGetMeanLatencyMs() {
        return getLatency.mean(TimeUnit.MILLISECONDS);
    }

    public double getPutMeanLatencyMs() {
        return putLatency.mean(TimeUnit.MILLISECONDS);
    }

    /**
     * @return the 99th percentile of recent client reads in milliseconds (the timers keep a
     *         rolling window of about two minutes), 0 before the first read
     */
    public double getGetP99LatencyMs() {
        return p99Ms(getLatency);
    }

    public double getPutP99LatencyMs() {
        return p99Ms(putLatency);
    }

    private static double p99Ms(Timer timer) {
        for (io.micrometer.core.instrument.distribution.ValueAtPercentile value
                : timer.takeSnapshot().percentileValues()) {
            if (value.percentile() == 0.99) {
                double ms = value.value(TimeUnit.MILLISECONDS);
                return Double.isFinite(ms) ? ms : 0.0;
            }
        }
        return 0.0;
    }

    public long getTotalReplicaReads() {
        return (long) internalGets.count();
    }

    public long getTotalReplicaWrites() {
        return (long) internalWrites.count();
    }

    public MeterRegistry getRegistry() {
        return registry;
    }
}
