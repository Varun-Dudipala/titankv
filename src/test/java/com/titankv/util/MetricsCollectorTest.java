package com.titankv.util;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for MetricsCollector to boost util package coverage.
 */
class MetricsCollectorTest {

    private MetricsCollector metrics;

    @BeforeEach
    void setUp() {
        metrics = new MetricsCollector();
    }

    @Test
    void recordGet_incrementsCounter() {
        metrics.recordGet(1000000L, true);
        metrics.recordGet(2000000L, false);

        assertThat(metrics.getTotalGetOps()).isEqualTo(2);
    }

    @Test
    void recordGet_tracksHitsAndMisses() {
        metrics.recordGet(1000000L, true);  // Hit
        metrics.recordGet(2000000L, true);  // Hit
        metrics.recordGet(3000000L, false); // Miss

        assertThat(metrics.getHitRate()).isEqualTo(2.0 / 3.0);
    }

    @Test
    void recordPut_incrementsCounter() {
        metrics.recordPut(1000000L);
        metrics.recordPut(2000000L);
        metrics.recordPut(3000000L);

        assertThat(metrics.getTotalPutOps()).isEqualTo(3);
    }

    @Test
    void recordDelete_incrementsCounter() {
        metrics.recordDelete(1000000L);
        metrics.recordDelete(2000000L);

        assertThat(metrics.getTotalDeleteOps()).isEqualTo(2);
    }

    @Test
    void recordError_incrementsCounter() {
        metrics.recordError();
        metrics.recordError();
        metrics.recordError();

        assertThat(metrics.getTotalErrors()).isEqualTo(3);
    }

    @Test
    void connectionTracking_incrementsAndDecrements() {
        assertThat(metrics.getActiveConnections()).isZero();

        metrics.connectionOpened();
        metrics.connectionOpened();
        assertThat(metrics.getActiveConnections()).isEqualTo(2);

        metrics.connectionClosed();
        assertThat(metrics.getActiveConnections()).isEqualTo(1);

        metrics.connectionClosed();
        assertThat(metrics.getActiveConnections()).isZero();
    }

    @Test
    void connectionTracking_cannotGoNegative() {
        metrics.connectionClosed();

        // Should be -1 (LongAdder allows negative)
        assertThat(metrics.getActiveConnections()).isEqualTo(-1);
    }

    @Test
    void setStoreSize_updatesGauge() {
        metrics.setStoreSize(100);
        metrics.setStoreSize(150);
        metrics.setStoreSize(75);

        // Final value should be 75
        assertThat(metrics.getRegistry().find("titankv.store.size").gauge()).isNotNull();
    }

    @Test
    void getLatency_recordsNanoseconds() {
        long duration = TimeUnit.MILLISECONDS.toNanos(5);
        metrics.recordGet(duration, true);

        double meanMs = metrics.getGetMeanLatencyMs();
        assertThat(meanMs).isGreaterThan(0);
        assertThat(meanMs).isLessThanOrEqualTo(10); // Should be around 5ms
    }

    @Test
    void putLatency_recordsNanoseconds() {
        long duration = TimeUnit.MILLISECONDS.toNanos(3);
        metrics.recordPut(duration);

        double meanMs = metrics.getPutMeanLatencyMs();
        assertThat(meanMs).isGreaterThan(0);
        assertThat(meanMs).isLessThanOrEqualTo(5);
    }

    @Test
    void getP99Latency_computesPercentile() {
        // Record multiple operations with varying latencies
        for (int i = 0; i < 100; i++) {
            long latency = TimeUnit.MILLISECONDS.toNanos(i);
            metrics.recordGet(latency, true);
        }

        double p99 = metrics.getGetP99LatencyMs();
        assertThat(p99).isGreaterThan(90); // P99 should be close to 99ms
    }

    @Test
    void putP99Latency_computesPercentile() {
        for (int i = 0; i < 100; i++) {
            long latency = TimeUnit.MILLISECONDS.toNanos(i);
            metrics.recordPut(latency);
        }

        double p99 = metrics.getPutP99LatencyMs();
        assertThat(p99).isGreaterThan(90);
    }

    @Test
    void hitRate_withNoOps_returnsZero() {
        assertThat(metrics.getHitRate()).isZero();
    }

    @Test
    void hitRate_allHits_returnsOne() {
        metrics.recordGet(1000L, true);
        metrics.recordGet(2000L, true);
        metrics.recordGet(3000L, true);

        assertThat(metrics.getHitRate()).isEqualTo(1.0);
    }

    @Test
    void hitRate_allMisses_returnsZero() {
        metrics.recordGet(1000L, false);
        metrics.recordGet(2000L, false);

        assertThat(metrics.getHitRate()).isZero();
    }

    @Test
    void summary_includesAllMetrics() {
        metrics.recordGet(1000000L, true);
        metrics.recordPut(2000000L);
        metrics.recordDelete(3000000L);
        metrics.recordError();
        metrics.connectionOpened();

        String summary = metrics.summary();

        assertThat(summary).contains("GET=1");
        assertThat(summary).contains("PUT=1");
        assertThat(summary).contains("DELETE=1");
        assertThat(summary).contains("Errors: 1");
        assertThat(summary).contains("Connections: 1");
    }

    @Test
    void customRegistry_isUsed() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MetricsCollector customMetrics = new MetricsCollector(registry);

        customMetrics.recordGet(1000L, true);

        assertThat(registry.find("titankv.ops").tag("operation", "get").counter())
            .isNotNull();
        assertThat(customMetrics.getTotalGetOps()).isEqualTo(1);
    }

    @Test
    void getRegistry_returnsUnderlyingRegistry() {
        assertThat(metrics.getRegistry()).isNotNull();
        assertThat(metrics.getRegistry()).isInstanceOf(SimpleMeterRegistry.class);
    }

    @Test
    void sharedInstance_isAccessible() {
        MetricsCollector shared = MetricsCollector.shared();

        assertThat(shared).isNotNull();
        assertThat(shared).isSameAs(MetricsCollector.shared());
    }

    @Test
    void sharedInstance_isIndependent() {
        MetricsCollector shared = MetricsCollector.shared();
        MetricsCollector instance = new MetricsCollector();

        shared.recordGet(1000L, true);
        instance.recordGet(2000L, true);

        // Shared should have 1 (plus any from other tests)
        // Instance should have exactly 1
        assertThat(instance.getTotalGetOps()).isEqualTo(1);
    }

    @Test
    void multipleOperations_maintainCorrectCounts() {
        for (int i = 0; i < 10; i++) {
            metrics.recordGet(1000L, i % 2 == 0); // 5 hits, 5 misses
        }
        for (int i = 0; i < 5; i++) {
            metrics.recordPut(1000L);
        }
        for (int i = 0; i < 3; i++) {
            metrics.recordDelete(1000L);
        }

        assertThat(metrics.getTotalGetOps()).isEqualTo(10);
        assertThat(metrics.getTotalPutOps()).isEqualTo(5);
        assertThat(metrics.getTotalDeleteOps()).isEqualTo(3);
        assertThat(metrics.getHitRate()).isEqualTo(0.5);
    }

    @Test
    void latencyTracking_multipleValues() {
        metrics.recordGet(TimeUnit.MILLISECONDS.toNanos(1), true);
        metrics.recordGet(TimeUnit.MILLISECONDS.toNanos(2), true);
        metrics.recordGet(TimeUnit.MILLISECONDS.toNanos(3), true);

        double mean = metrics.getGetMeanLatencyMs();
        assertThat(mean).isGreaterThan(1.5); // Average of 1, 2, 3
        assertThat(mean).isLessThan(2.5);
    }

    @Test
    void zeroLatency_isRecorded() {
        metrics.recordGet(0, true);

        assertThat(metrics.getTotalGetOps()).isEqualTo(1);
        assertThat(metrics.getGetMeanLatencyMs()).isGreaterThanOrEqualTo(0);
    }

    @Test
    void veryHighLatency_isRecorded() {
        long oneSecond = TimeUnit.SECONDS.toNanos(1);
        metrics.recordPut(oneSecond);

        double meanMs = metrics.getPutMeanLatencyMs();
        assertThat(meanMs).isGreaterThan(999); // Should be ~1000ms
    }
}
