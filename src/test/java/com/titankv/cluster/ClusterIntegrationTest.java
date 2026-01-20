package com.titankv.cluster;

import com.titankv.TitanKVClient;
import com.titankv.TitanKVServer;
import com.titankv.client.ClientConfig;
import org.junit.jupiter.api.*;

import java.util.Optional;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Integration tests for a multi-node TitanKV cluster.
 * Tests cluster formation, data distribution, and failure handling.
 * 
 * Note: These tests are timing-sensitive due to gossip protocol propagation.
 * Run with -Dtest=ClusterIntegrationTest for dedicated execution.
 */
@org.junit.jupiter.api.Tag("integration")
class ClusterIntegrationTest {

    private static final int BASE_PORT = 19300;
    private static final long DEFAULT_TIMEOUT_MS = 15000;
    private static final long POLL_INTERVAL_MS = 100;

    private TitanKVServer node1;
    private TitanKVServer node2;
    private TitanKVServer node3;

    @BeforeAll
    static void setupDevMode() {
        // Enable dev mode to disable authentication requirements
        System.setProperty("titankv.dev.mode", "true");
    }

    /**
     * Wait for a condition to become true, polling at regular intervals.
     */
    private void waitForCondition(Supplier<Boolean> condition, long timeoutMs, String description) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.get()) {
                return;
            }
            try {
                Thread.sleep(POLL_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("Interrupted waiting for: " + description);
            }
        }
        fail("Timeout waiting for: " + description);
    }

    @BeforeEach
    void setUp() throws Exception {
        // Start a 3-node cluster
        // Node 1 is the seed node
        node1 = new TitanKVServer(BASE_PORT, "node1", null);
        node1.start();
        waitForCondition(() -> node1.isRunning(), DEFAULT_TIMEOUT_MS, "node1 to start");

        // Node 2 joins via node1
        node2 = new TitanKVServer(BASE_PORT + 1, "node2", "localhost:" + BASE_PORT);
        node2.start();
        waitForCondition(() -> node2.isRunning(), DEFAULT_TIMEOUT_MS, "node2 to start");

        // Node 3 joins via node1
        node3 = new TitanKVServer(BASE_PORT + 2, "node3", "localhost:" + BASE_PORT);
        node3.start();
        waitForCondition(() -> node3.isRunning(), DEFAULT_TIMEOUT_MS, "node3 to start");

        // Wait for gossip to propagate - all nodes should see all 3 nodes
        waitForCondition(
            () -> node1.getClusterSize() >= 3 && node2.getClusterSize() >= 3 && node3.getClusterSize() >= 3,
            DEFAULT_TIMEOUT_MS,
            "full cluster discovery (3 nodes)"
        );
        
        // Additional wait for stability
        Thread.sleep(1000);
    }

    @AfterEach
    void tearDown() {
        if (node3 != null) node3.stop();
        if (node2 != null) node2.stop();
        if (node1 != null) node1.stop();
    }

    @Test
    void clusterForms_withThreeNodes() {
        // All nodes should be running
        assertThat(node1.isRunning()).isTrue();
        assertThat(node2.isRunning()).isTrue();
        assertThat(node3.isRunning()).isTrue();

        // Each node should know about itself at minimum
        assertThat(node1.getClusterSize()).isGreaterThanOrEqualTo(1);
        assertThat(node2.getClusterSize()).isGreaterThanOrEqualTo(1);
        assertThat(node3.getClusterSize()).isGreaterThanOrEqualTo(1);
    }

    @Test
    void dataCanBeWrittenToAnyNode() throws Exception {
        try (TitanKVClient client1 = new TitanKVClient("localhost:" + BASE_PORT);
             TitanKVClient client2 = new TitanKVClient("localhost:" + (BASE_PORT + 1));
             TitanKVClient client3 = new TitanKVClient("localhost:" + (BASE_PORT + 2))) {

            // Write to each node
            client1.put("key1", "value1");
            client2.put("key2", "value2");
            client3.put("key3", "value3");

            // Read back from the same node
            assertThat(client1.getString("key1")).hasValue("value1");
            assertThat(client2.getString("key2")).hasValue("value2");
            assertThat(client3.getString("key3")).hasValue("value3");
        }
    }

    @Test
    void clientWithMultipleHosts_distributesRequests() throws Exception {
        String[] hosts = {
            "localhost:" + BASE_PORT,
            "localhost:" + (BASE_PORT + 1),
            "localhost:" + (BASE_PORT + 2)
        };

        ClientConfig config = ClientConfig.builder()
            .connectTimeoutMs(5000)
            .readTimeoutMs(5000)
            .maxRetries(3)
            .retryOnFailure(true)
            .build();

        try (TitanKVClient client = new TitanKVClient(config, hosts)) {
            // Write multiple keys - they should be distributed by consistent hashing
            for (int i = 0; i < 50; i++) {
                client.put("distributed-key-" + i, "value-" + i);
            }

            // All keys should be readable
            for (int i = 0; i < 50; i++) {
                Optional<String> result = client.getString("distributed-key-" + i);
                assertThat(result).isPresent();
                assertThat(result.get()).isEqualTo("value-" + i);
            }
        }
    }

    @Test
    void consistentHashing_routesToSameNode() throws Exception {
        // Same key should always route to the same node
        String testKey = "consistent-test-key";
        String testValue = "consistent-value";

        try (TitanKVClient client = new TitanKVClient("localhost:" + BASE_PORT)) {
            client.put(testKey, testValue);

            // Read multiple times - should always succeed from same node
            for (int i = 0; i < 10; i++) {
                Optional<String> result = client.getString(testKey);
                assertThat(result).hasValue(testValue);
            }
        }
    }

    @Test
    void nodeFailure_otherNodesStillWork() throws Exception {
        String[] allHosts = {
            "localhost:" + BASE_PORT,
            "localhost:" + (BASE_PORT + 1),
            "localhost:" + (BASE_PORT + 2)
        };

        ClientConfig config = ClientConfig.builder()
            .connectTimeoutMs(5000)
            .readTimeoutMs(5000)
            .maxRetries(3)
            .retryOnFailure(true)
            .build();

        // Write some data through all nodes
        try (TitanKVClient client = new TitanKVClient(config, allHosts)) {
            for (int i = 0; i < 10; i++) {
                client.put("failover-key-" + i, "value-" + i);
            }
        }

        // Stop node 3
        node3.stop();
        waitForCondition(() -> !node3.isRunning(), DEFAULT_TIMEOUT_MS, "node3 to stop");

        // Should still be able to access nodes 1 and 2
        String[] remainingHosts = {
            "localhost:" + BASE_PORT,
            "localhost:" + (BASE_PORT + 1)
        };

        try (TitanKVClient client = new TitanKVClient(config, remainingHosts)) {
            assertThat(client.ping()).isTrue();

            // Can still write new data
            client.put("after-failover", "still-works");
            assertThat(client.getString("after-failover")).hasValue("still-works");
        }
    }

    @Test
    void clusterManager_tracksNodes() throws Exception {
        ClusterManager cm1 = node1.getClusterManager();

        // The local node should always be in the cluster
        assertThat(cm1.getLocalNode()).isNotNull();
        assertThat(cm1.getLocalNode().getId()).isEqualTo("node1");

        // Cluster should have at least 1 node (local)
        assertThat(cm1.getNodeCount()).isGreaterThanOrEqualTo(1);

        // Hash ring should have virtual nodes
        assertThat(cm1.getHashRing().getVirtualNodeCount()).isGreaterThan(0);
    }

    @Test
    void multipleOperations_onCluster() throws Exception {
        String[] hosts = {
            "localhost:" + BASE_PORT,
            "localhost:" + (BASE_PORT + 1),
            "localhost:" + (BASE_PORT + 2)
        };

        ClientConfig config = ClientConfig.builder()
            .connectTimeoutMs(5000)
            .readTimeoutMs(5000)
            .maxRetries(3)
            .retryOnFailure(true)
            .build();

        try (TitanKVClient client = new TitanKVClient(config, hosts)) {
            // PUT
            client.put("multiop-key", "initial-value");
            assertThat(client.getString("multiop-key")).hasValue("initial-value");

            // UPDATE
            client.put("multiop-key", "updated-value");
            assertThat(client.getString("multiop-key")).hasValue("updated-value");

            // EXISTS
            assertThat(client.exists("multiop-key")).isTrue();
            assertThat(client.exists("nonexistent")).isFalse();

            // DELETE
            client.delete("multiop-key");
            assertThat(client.exists("multiop-key")).isFalse();
            assertThat(client.get("multiop-key")).isEmpty();
        }
    }

    @Test
    void highThroughput_onCluster() throws Exception {
        String[] hosts = {
            "localhost:" + BASE_PORT,
            "localhost:" + (BASE_PORT + 1),
            "localhost:" + (BASE_PORT + 2)
        };

        int numOperations = 500;

        ClientConfig config = ClientConfig.builder()
            .connectTimeoutMs(5000)
            .readTimeoutMs(5000)
            .maxRetries(3)
            .retryOnFailure(true)
            .build();

        try (TitanKVClient client = new TitanKVClient(config, hosts)) {
            long startTime = System.currentTimeMillis();

            // Write
            for (int i = 0; i < numOperations; i++) {
                client.put("throughput-key-" + i, "value-" + i);
            }

            // Read
            for (int i = 0; i < numOperations; i++) {
                client.get("throughput-key-" + i);
            }

            long duration = System.currentTimeMillis() - startTime;
            double opsPerSecond = (numOperations * 2.0) / (duration / 1000.0);

            System.out.println("Cluster throughput: " + String.format("%.0f", opsPerSecond) + " ops/sec");

            // Should be able to handle at least some throughput
            assertThat(opsPerSecond).isGreaterThan(50);
        }
    }
}
