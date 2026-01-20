package com.titankv.util;

import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Metrics collector for TitanKV performance monitoring.
 * Tracks throughput, latency, and operational statistics.
 */
public class MetricsCollector {

    private final MeterRegistry registry;

    // Counters
    private final Counter getOps;
    private final Counter putOps;
    private final Counter deleteOps;
    private final Counter getHits;
    private final Counter getMisses;
    private final Counter errors;

    // Timers
    private final Timer getLatency;
    private final Timer putLatency;
    private final Timer deleteLatency;

    // Gauges
    private final LongAdder activeConnections;
    private final LongAdder storeSize;

    /**
     * Create a metrics collector with a simple registry.
     */
    public MetricsCollector() {
        this(new SimpleMeterRegistry());
    }

    /**
     * Create a metrics collector with a custom registry.
     *
     * @param registry the Micrometer registry to use
     */
    public MetricsCollector(MeterRegistry registry) {
        this.registry = registry;

        // Initialize counters
        this.getOps = Counter.builder("titankv.ops")
            .tag("operation", "get")
            .description("Total GET operations")
            .register(registry);

        this.putOps = Counter.builder("titankv.ops")
            .tag("operation", "put")
            .description("Total PUT operations")
            .register(registry);

        this.deleteOps = Counter.builder("titankv.ops")
            .tag("operation", "delete")
            .description("Total DELETE operations")
            .register(registry);

        this.getHits = Counter.builder("titankv.cache")
            .tag("result", "hit")
            .description("Cache hits")
            .register(registry);

        this.getMisses = Counter.builder("titankv.cache")
            .tag("result", "miss")
            .description("Cache misses")
            .register(registry);

        this.errors = Counter.builder("titankv.errors")
            .description("Total errors")
            .register(registry);

        // Initialize timers
        this.getLatency = Timer.builder("titankv.latency")
            .tag("operation", "get")
            .description("GET operation latency")
            .publishPercentiles(0.5, 0.95, 0.99)
            .register(registry);

        this.putLatency = Timer.builder("titankv.latency")
            .tag("operation", "put")
            .description("PUT operation latency")
            .publishPercentiles(0.5, 0.95, 0.99)
            .register(registry);

        this.deleteLatency = Timer.builder("titankv.latency")
            .tag("operation", "delete")
            .description("DELETE operation latency")
            .publishPercentiles(0.5, 0.95, 0.99)
            .register(registry);

        // Initialize gauge backing values
        this.activeConnections = new LongAdder();
        this.storeSize = new LongAdder();

        // Register gauges
        Gauge.builder("titankv.connections", activeConnections, LongAdder::sum)
            .description("Active connections")
            .register(registry);

        Gauge.builder("titankv.store.size", storeSize, LongAdder::sum)
            .description("Number of entries in store")
            .register(registry);
    }

    // Operation recording methods

    public void recordGet(long durationNanos, boolean hit) {
        getOps.increment();
        getLatency.record(durationNanos, TimeUnit.NANOSECONDS);
        if (hit) {
            getHits.increment();
        } else {
            getMisses.increment();
        }
    }

    public void recordPut(long durationNanos) {
        putOps.increment();
        putLatency.record(durationNanos, TimeUnit.NANOSECONDS);
    }

    public void recordDelete(long durationNanos) {
        deleteOps.increment();
        deleteLatency.record(durationNanos, TimeUnit.NANOSECONDS);
    }

    public void recordError() {
        errors.increment();
    }

    // Connection tracking

    public void connectionOpened() {
        activeConnections.increment();
    }

    public void connectionClosed() {
        activeConnections.decrement();
    }

    // Store size tracking

    public void setStoreSize(int size) {
        // Reset and set to new value
        long current = storeSize.sum();
        storeSize.add(size - current);
    }

    // Getters for metrics values

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

    public double getHitRate() {
        double hits = getHits.count();
        double misses = getMisses.count();
        double total = hits + misses;
        return total > 0 ? hits / total : 0.0;
    }

    @SuppressWarnings("deprecation") // Using deprecated percentile API for simplicity
    public double getGetP99LatencyMs() {
        return getLatency.percentile(0.99, TimeUnit.MILLISECONDS);
    }

    @SuppressWarnings("deprecation") // Using deprecated percentile API for simplicity
    public double getPutP99LatencyMs() {
        return putLatency.percentile(0.99, TimeUnit.MILLISECONDS);
    }

    public double getGetMeanLatencyMs() {
        return getLatency.mean(TimeUnit.MILLISECONDS);
    }

    public double getPutMeanLatencyMs() {
        return putLatency.mean(TimeUnit.MILLISECONDS);
    }

    /**
     * Get the underlying registry.
     *
     * @return the MeterRegistry
     */
    public MeterRegistry getRegistry() {
        return registry;
    }

    /**
     * Print a summary of current metrics.
     *
     * @return formatted metrics string
     */
    public String summary() {
        return String.format(
            "TitanKV Metrics Summary%n" +
            "=======================-%n" +
            "Operations: GET=%d, PUT=%d, DELETE=%d%n" +
            "Cache: hits=%d, misses=%d, hitRate=%.2f%%%n" +
            "Errors: %d%n" +
            "Connections: %d active%n" +
            "Latency (mean): GET=%.3fms, PUT=%.3fms%n" +
            "Latency (p99):  GET=%.3fms, PUT=%.3fms",
            getTotalGetOps(), getTotalPutOps(), getTotalDeleteOps(),
            (long) getHits.count(), (long) getMisses.count(), getHitRate() * 100,
            getTotalErrors(),
            getActiveConnections(),
            getGetMeanLatencyMs(), getPutMeanLatencyMs(),
            getGetP99LatencyMs(), getPutP99LatencyMs()
        );
    }

    /**
     * Shared instance for common use.
     */
    private static final MetricsCollector SHARED = new MetricsCollector();

    /**
     * Get the shared metrics instance.
     *
     * @return the shared MetricsCollector
     */
    public static MetricsCollector shared() {
        return SHARED;
    }
}
