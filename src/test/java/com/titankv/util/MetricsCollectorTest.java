package com.titankv.util;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;

/**
 * MetricsCollector: operation counts, read hit rate, latencies, connections and replica traffic.
 */
class MetricsCollectorTest {

    private SimpleMeterRegistry registry;
    private MetricsCollector metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new MetricsCollector(registry);
    }

    @Test
    void countsClientOperationsByType() {
        metrics.recordGet(1_000_000L, true);
        metrics.recordGet(2_000_000L, false);
        metrics.recordPut(1_000_000L);
        metrics.recordPut(1_000_000L);
        metrics.recordPut(1_000_000L);
        metrics.recordDelete(1_000_000L);

        assertThat(metrics.getTotalGetOps()).isEqualTo(2);
        assertThat(metrics.getTotalPutOps()).isEqualTo(3);
        assertThat(metrics.getTotalDeleteOps()).isEqualTo(1);
        assertThat(registry.get("titankv.ops").tag("operation", "put").counter().count()).isEqualTo(3);
    }

    @Test
    void hitRateIsTheShareOfReadsThatFoundTheirKey() {
        assertThat(metrics.getHitRate()).isZero();
        metrics.recordGet(1L, true);
        metrics.recordGet(1L, true);
        metrics.recordGet(1L, false);

        assertThat(metrics.getHitRate()).isEqualTo(2.0 / 3.0);
        assertThat(registry.get("titankv.reads").tag("result", "miss").counter().count()).isEqualTo(1);
    }

    @Test
    void replicaTrafficIsCountedApartFromClientOperations() {
        metrics.recordReplicaRead();
        metrics.recordReplicaWrite();
        metrics.recordReplicaWrite();

        assertThat(metrics.getTotalGetOps()).isZero();
        assertThat(metrics.getTotalPutOps()).isZero();
        assertThat(registry.get("titankv.replica.ops").tag("operation", "read").counter().count()).isEqualTo(1);
        assertThat(registry.get("titankv.replica.ops").tag("operation", "write").counter().count()).isEqualTo(2);
    }

    @Test
    void latenciesAreRecordedPerOperation() {
        metrics.recordGet(TimeUnit.MILLISECONDS.toNanos(2), true);
        metrics.recordGet(TimeUnit.MILLISECONDS.toNanos(4), true);
        metrics.recordPut(TimeUnit.MILLISECONDS.toNanos(10));

        assertThat(metrics.getGetMeanLatencyMs()).isCloseTo(3.0, within(0.01));
        assertThat(metrics.getPutMeanLatencyMs()).isCloseTo(10.0, within(0.01));
        assertThat(registry.get("titankv.latency").tag("operation", "get").timer().count()).isEqualTo(2);
    }

    @Test
    void tracksErrorsAndOpenConnections() {
        metrics.recordError();
        metrics.connectionOpened();
        metrics.connectionOpened();
        metrics.connectionClosed();

        assertThat(metrics.getTotalErrors()).isEqualTo(1);
        assertThat(metrics.getActiveConnections()).isEqualTo(1);
        assertThat(registry.get("titankv.connections").gauge().value()).isEqualTo(1);
    }
}
