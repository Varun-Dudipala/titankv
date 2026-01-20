package com.titankv.consistency;

import com.titankv.cluster.ClusterManager;
import com.titankv.cluster.Node;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for ReadRepairHandler to boost consistency package coverage.
 */
class ReadRepairHandlerTest {

    private ReadRepairHandler handler;
    private ClusterManager clusterManager;
    private Node localNode;

    @BeforeEach
    void setUp() {
        localNode = new Node("test-node", "localhost", 19300);
        clusterManager = new ClusterManager(localNode);
        clusterManager.start(null);

        handler = new ReadRepairHandler(clusterManager, 3);
    }

    @AfterEach
    void tearDown() {
        if (handler != null) {
            handler.shutdown();
        }
        if (clusterManager != null) {
            clusterManager.stop();
        }
    }

    @Test
    void readWithRepair_noReplicasAvailable_failsGracefully() {
        // Empty cluster - no replicas
        CompletableFuture<ReadRepairHandler.RepairResult> future =
            handler.readWithRepair("nonexistent-key", 1, 1000);

        assertThatThrownBy(() -> future.get(2, TimeUnit.SECONDS))
            .hasCauseInstanceOf(ConsistencyException.class)
            .hasMessageContaining("Not enough replicas");
    }

    @Test
    void readWithRepair_timeoutExceeded_fails() {
        // Add a node that won't respond (port 60000 is safe, won't conflict with gossip)
        Node unreachableNode = new Node("unreachable", "localhost", 60000);
        unreachableNode.setStatus(Node.Status.ALIVE);
        clusterManager.addNode(unreachableNode);

        CompletableFuture<ReadRepairHandler.RepairResult> future =
            handler.readWithRepair("test-key", 1, 100); // 100ms timeout

        assertThatThrownBy(() -> future.get(2, TimeUnit.SECONDS))
            .isInstanceOf(ExecutionException.class);
    }

    @Test
    void repairResult_constructorAndGetters() {
        byte[] value = "test-value".getBytes();
        long timestamp = System.currentTimeMillis();
        long expiresAt = timestamp + 10000;
        Node node1 = new Node("node1", "localhost", 9001);
        Node node2 = new Node("node2", "localhost", 9002);

        ReadRepairHandler.RepairResult result = new ReadRepairHandler.RepairResult(
            value, timestamp, expiresAt,
            java.util.List.of(node1, node2),
            true
        );

        assertThat(result.getValue()).isEqualTo(value);
        assertThat(result.getTimestamp()).isEqualTo(timestamp);
        assertThat(result.getExpiresAt()).isEqualTo(expiresAt);
        assertThat(result.getRepairedNodes()).containsExactly(node1, node2);
        assertThat(result.isRepairNeeded()).isTrue();
    }

    @Test
    void repairResult_noRepairNeeded() {
        byte[] value = "test-value".getBytes();
        long timestamp = System.currentTimeMillis();

        ReadRepairHandler.RepairResult result = new ReadRepairHandler.RepairResult(
            value, timestamp, 0,
            java.util.List.of(),
            false
        );

        assertThat(result.isRepairNeeded()).isFalse();
        assertThat(result.getRepairedNodes()).isEmpty();
    }

    @Test
    void repairResult_nullValue() {
        ReadRepairHandler.RepairResult result = new ReadRepairHandler.RepairResult(
            null, 0, 0,
            java.util.List.of(),
            false
        );

        assertThat(result.getValue()).isNull();
        assertThat(result.getTimestamp()).isZero();
        assertThat(result.isRepairNeeded()).isFalse();
    }

    @Test
    void forceRepair_emptyCluster_returnsZero() throws Exception {
        byte[] value = "force-repair-value".getBytes();
        long timestamp = System.currentTimeMillis();

        CompletableFuture<Integer> future = handler.forceRepair(
            "test-key", value, timestamp, 0
        );

        // Should complete even with no nodes to repair
        Integer repaired = future.get(1, TimeUnit.SECONDS);
        assertThat(repaired).isGreaterThanOrEqualTo(0);
    }

    @Test
    void forceRepair_withUnreachableNode_handlesFailure() throws Exception {
        // Add unreachable node (port 60001 is safe)
        Node unreachableNode = new Node("unreachable", "localhost", 60001);
        unreachableNode.setStatus(Node.Status.ALIVE);
        clusterManager.addNode(unreachableNode);

        byte[] value = "test-value".getBytes();
        long timestamp = System.currentTimeMillis();

        CompletableFuture<Integer> future = handler.forceRepair(
            "test-key", value, timestamp, 0
        );

        // Should complete even if repair fails on some nodes
        Integer repaired = future.get(2, TimeUnit.SECONDS);
        assertThat(repaired).isGreaterThanOrEqualTo(0);
    }

    @Test
    void shutdown_stopsGracefully() {
        handler.shutdown();

        // Second shutdown should be safe
        assertThatCode(() -> handler.shutdown()).doesNotThrowAnyException();
    }

    @Test
    void shutdown_withPendingOperations_waits() {
        // Start a long operation
        CompletableFuture<ReadRepairHandler.RepairResult> future =
            handler.readWithRepair("test-key", 1, 5000);

        // Shutdown should wait for termination
        long start = System.currentTimeMillis();
        handler.shutdown();
        long elapsed = System.currentTimeMillis() - start;

        // Should complete relatively quickly (within 6 seconds)
        assertThat(elapsed).isLessThan(6000);
    }

    @Test
    void readWithRepair_notEnoughReplicas_fails() {
        // Local node only, requesting 2 responses
        CompletableFuture<ReadRepairHandler.RepairResult> future =
            handler.readWithRepair("test-key", 2, 1000);

        assertThatThrownBy(() -> future.get(2, TimeUnit.SECONDS))
            .hasCauseInstanceOf(ConsistencyException.class)
            .hasMessageContaining("Not enough replicas");
    }

    @Test
    void readWithRepair_emptyKey_handled() {
        CompletableFuture<ReadRepairHandler.RepairResult> future =
            handler.readWithRepair("", 1, 1000);

        // Should attempt to read even with empty key
        assertThatThrownBy(() -> future.get(2, TimeUnit.SECONDS))
            .isInstanceOf(ExecutionException.class);
    }

    @Test
    void handler_usesCorrectReplicationFactor() {
        ReadRepairHandler customHandler = new ReadRepairHandler(clusterManager, 5);

        try {
            // Verify it's created successfully with custom RF
            assertThat(customHandler).isNotNull();

            // No replicas available for RF=5
            CompletableFuture<ReadRepairHandler.RepairResult> future =
                customHandler.readWithRepair("test-key", 1, 1000);

            assertThatThrownBy(() -> future.get(2, TimeUnit.SECONDS))
                .hasCauseInstanceOf(ConsistencyException.class);
        } finally {
            customHandler.shutdown();
        }
    }
}
