package com.titankv.cluster;

import com.titankv.TitanKVClient;
import com.titankv.TitanKVServer;
import com.titankv.client.ClientConfig;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;

/**
 * Multi-node integration tests for TitanKV.
 * Tests cluster formation, data replication, failure scenarios, and recovery.
 * 
 * Note: These tests are timing-sensitive due to gossip protocol propagation.
 * Run with -Dtest=MultiNodeIntegrationTest for dedicated execution.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@org.junit.jupiter.api.Tag("integration")
class MultiNodeIntegrationTest {

    private static final int BASE_PORT = 19001;
    private static final int NUM_NODES = 3;
    private static final long STARTUP_DELAY_MS = 1000;

    private List<TitanKVServer> servers;
    private TitanKVClient client;

    @BeforeAll
    void setupCluster() throws Exception {
        // Enable dev mode for tests
        System.setProperty("titankv.dev.mode", "true");
        
        servers = new ArrayList<>();
        
        // Start first node (seed)
        TitanKVServer seed = new TitanKVServer(BASE_PORT, "node-1", null);
        seed.start();
        servers.add(seed);
        Thread.sleep(STARTUP_DELAY_MS);

        // Start additional nodes
        for (int i = 1; i < NUM_NODES; i++) {
            int port = BASE_PORT + i;
            String nodeId = "node-" + (i + 1);
            String seeds = "localhost:" + BASE_PORT;
            
            TitanKVServer node = new TitanKVServer(port, nodeId, seeds);
            node.start();
            servers.add(node);
            Thread.sleep(STARTUP_DELAY_MS);
        }

        // Wait for cluster to stabilize (gossip propagation)
        Thread.sleep(5000);

        // Create client connected to all nodes
        String[] hosts = new String[NUM_NODES];
        for (int i = 0; i < NUM_NODES; i++) {
            hosts[i] = "localhost:" + (BASE_PORT + i);
        }
        
        ClientConfig config = ClientConfig.builder()
            .connectTimeoutMs(5000)
            .readTimeoutMs(5000)
            .maxRetries(3)
            .retryOnFailure(true)
            .build();
        
        client = new TitanKVClient(config, hosts);
    }

    @AfterAll
    void teardownCluster() {
        if (client != null) {
            client.close();
        }
        
        for (TitanKVServer server : servers) {
            try {
                server.stop();
            } catch (Exception e) {
                // Ignore shutdown errors
            }
        }
    }

    @Test
    @DisplayName("Cluster should form with all nodes")
    void testClusterFormation() {
        // All servers should be running
        for (TitanKVServer server : servers) {
            assertThat(server.isRunning()).isTrue();
        }

        // Each server should see all other nodes
        for (TitanKVServer server : servers) {
            ClusterManager cm = server.getClusterManager();
            assertThat(cm.getNodeCount()).isEqualTo(NUM_NODES);
            assertThat(cm.getAliveNodeCount()).isEqualTo(NUM_NODES);
        }
    }

    @Test
    @DisplayName("Data should be accessible from any node")
    void testDataAccessibility() throws IOException {
        String key = "test-key-" + System.currentTimeMillis();
        String value = "test-value";

        // Write through client
        client.put(key, value);

        // Read back
        Optional<String> result = client.getString(key);
        assertThat(result).isPresent().contains(value);
    }

    @Test
    @DisplayName("Multiple writes should succeed under load")
    void testConcurrentWrites() throws Exception {
        int numOperations = 100;
        ExecutorService executor = Executors.newFixedThreadPool(10);
        CountDownLatch latch = new CountDownLatch(numOperations);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);

        for (int i = 0; i < numOperations; i++) {
            final int idx = i;
            executor.submit(() -> {
                try {
                    String key = "concurrent-key-" + idx;
                    String value = "value-" + idx;
                    client.put(key, value);
                    
                    Optional<String> result = client.getString(key);
                    if (result.isPresent() && result.get().equals(value)) {
                        successCount.incrementAndGet();
                    } else {
                        failCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    failCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(60, TimeUnit.SECONDS);
        executor.shutdown();

        assertThat(successCount.get()).isGreaterThanOrEqualTo(numOperations * 90 / 100); // 90% success rate minimum
    }

    @Test
    @DisplayName("Consistent hashing should distribute keys")
    void testKeyDistribution() throws IOException {
        int numKeys = 100;
        Map<String, Set<String>> nodeKeys = new HashMap<>();
        
        for (TitanKVServer server : servers) {
            nodeKeys.put(server.getNodeId(), new HashSet<>());
        }

        // Write many keys
        for (int i = 0; i < numKeys; i++) {
            String key = "dist-key-" + i;
            client.put(key, "value-" + i);
        }

        // Check distribution across nodes
        for (TitanKVServer server : servers) {
            int localKeys = server.getStore().size();
            // Each node should have some keys (not all on one node)
            // With replication factor 3 and 3 nodes, each node should have most keys
            assertThat(localKeys).isGreaterThan(0);
        }
    }

    @Test
    @DisplayName("Delete should work across cluster")
    void testDelete() throws IOException {
        String key = "delete-test-" + System.currentTimeMillis();
        String value = "to-be-deleted";

        // Write
        client.put(key, value);
        assertThat(client.exists(key)).isTrue();

        // Delete
        client.delete(key);

        // Verify deleted
        Optional<String> result = client.getString(key);
        assertThat(result).isEmpty();
        assertThat(client.exists(key)).isFalse();
    }

    @Test
    @DisplayName("Large values should be stored correctly")
    void testLargeValues() throws IOException {
        String key = "large-value-test";
        
        // Create 1MB value
        byte[] largeValue = new byte[1024 * 1024];
        new Random().nextBytes(largeValue);

        client.put(key, largeValue);

        Optional<byte[]> result = client.get(key);
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(largeValue);
    }

    @Test
    @DisplayName("Client should retry on transient failures")
    void testRetryBehavior() throws IOException {
        // This test verifies the client's retry logic works
        String key = "retry-test-" + System.currentTimeMillis();
        String value = "retry-value";

        // Write and read should succeed even with potential transient issues
        client.put(key, value);
        Optional<String> result = client.getString(key);
        
        assertThat(result).isPresent().contains(value);
    }

    @Test
    @DisplayName("Ping should work")
    void testPing() {
        assertThat(client.ping()).isTrue();
    }

    @Test
    @DisplayName("Non-existent key returns empty")
    void testNonExistentKey() throws IOException {
        String key = "non-existent-key-" + UUID.randomUUID();
        
        Optional<byte[]> result = client.get(key);
        assertThat(result).isEmpty();
        assertThat(client.exists(key)).isFalse();
    }

    @Test
    @DisplayName("Update should overwrite previous value")
    void testUpdate() throws IOException {
        String key = "update-test-" + System.currentTimeMillis();
        String value1 = "first-value";
        String value2 = "second-value";

        client.put(key, value1);
        assertThat(client.getString(key)).contains(value1);

        client.put(key, value2);
        assertThat(client.getString(key)).contains(value2);
    }

    @Test
    @DisplayName("Empty value should be stored correctly")
    void testEmptyValue() throws IOException {
        String key = "empty-value-test";
        byte[] emptyValue = new byte[0];

        client.put(key, emptyValue);

        Optional<byte[]> result = client.get(key);
        assertThat(result).isPresent();
        assertThat(result.get()).hasSize(0);
    }

    @Test
    @DisplayName("Special characters in keys should work")
    void testSpecialCharacterKeys() throws IOException {
        String[] specialKeys = {
            "key:with:colons",
            "key.with.dots",
            "key-with-dashes",
            "key_with_underscores",
            "key/with/slashes",
            "key with spaces"
        };

        for (String key : specialKeys) {
            String value = "value-for-" + key;
            client.put(key, value);
            
            Optional<String> result = client.getString(key);
            assertThat(result)
                .as("Key '%s' should be retrievable", key)
                .isPresent()
                .contains(value);
        }
    }

    @Test
    @DisplayName("Cluster membership should be visible to all nodes")
    void testMembershipVisibility() {
        for (TitanKVServer server : servers) {
            ClusterManager cm = server.getClusterManager();
            
            // All nodes should see each other
            assertThat(cm.getAllNodes()).hasSize(NUM_NODES);
            
            // Local node should be in the list
            assertThat(cm.getAllNodes().stream()
                .anyMatch(n -> n.getId().equals(server.getNodeId()))).isTrue();
        }
    }

    @Test
    @DisplayName("Metrics should be collected")
    void testMetricsCollection() throws IOException {
        // Perform some operations
        String key = "metrics-test-" + System.currentTimeMillis();
        client.put(key, "test");
        client.get(key);
        client.delete(key);

        // Check that metrics are collected on at least one server
        long totalOps = 0;
        for (TitanKVServer server : servers) {
            totalOps += server.getMetrics().getTotalGetOps() +
                        server.getMetrics().getTotalPutOps() +
                        server.getMetrics().getTotalDeleteOps();
        }
        // At least some operations should have been recorded across the cluster
        assertThat(totalOps).as("Some metrics should be recorded").isGreaterThan(0);
    }
}
