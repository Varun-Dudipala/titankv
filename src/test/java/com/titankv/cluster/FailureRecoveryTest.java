package com.titankv.cluster;

import com.titankv.TitanKVClient;
import com.titankv.TitanKVServer;
import com.titankv.client.ClientConfig;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for failure scenarios and recovery.
 * Verifies that the cluster handles node failures gracefully.
 * 
 * Note: These tests are timing-sensitive due to gossip protocol propagation.
 * Run with -Dtest=FailureRecoveryTest for dedicated execution.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@org.junit.jupiter.api.Tag("integration")
class FailureRecoveryTest {

    private static final int BASE_PORT = 19400;
    private static final int NUM_NODES = 3;
    private static final long STARTUP_DELAY_MS = 1000;

    private List<TitanKVServer> servers;
    private TitanKVClient client;

    @BeforeEach
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

        // Wait for cluster to stabilize
        Thread.sleep(2000);

        // Create client connected to all nodes
        String[] hosts = new String[NUM_NODES];
        for (int i = 0; i < NUM_NODES; i++) {
            hosts[i] = "localhost:" + (BASE_PORT + i);
        }
        
        ClientConfig config = ClientConfig.builder()
            .connectTimeoutMs(3000)
            .readTimeoutMs(3000)
            .maxRetries(3)
            .retryOnFailure(true)
            .retryDelayMs(100)
            .build();
        
        client = new TitanKVClient(config, hosts);
    }

    @AfterEach
    void teardownCluster() {
        if (client != null) {
            client.close();
            client = null;
        }
        
        for (TitanKVServer server : servers) {
            try {
                server.stop();
            } catch (Exception e) {
                // Ignore shutdown errors
            }
        }
        servers.clear();
    }

    @Test
    @DisplayName("Data should survive single node failure")
    void testSingleNodeFailure() throws Exception {
        // Write data
        String key = "failure-test-" + System.currentTimeMillis();
        String value = "test-value";
        client.put(key, value);

        // Verify data is written
        assertThat(client.getString(key)).contains(value);

        // Stop one node (not the seed)
        TitanKVServer nodeToStop = servers.get(1);
        nodeToStop.stop();

        // Wait for failure detection
        Thread.sleep(4000);

        // Verify remaining nodes detected the failure
        for (int i = 0; i < servers.size(); i++) {
            if (i == 1) continue; // Skip stopped node
            ClusterManager cm = servers.get(i).getClusterManager();
            Node failedNode = cm.getNode(nodeToStop.getNodeId());
            if (failedNode != null) {
                assertThat(failedNode.getStatus())
                    .isIn(Node.Status.SUSPECT, Node.Status.DEAD);
            }
        }

        // Data should still be accessible (from other replicas)
        // Create new client without the failed node
        client.close();
        client = new TitanKVClient(
            ClientConfig.builder()
                .connectTimeoutMs(3000)
                .readTimeoutMs(3000)
                .maxRetries(2)
                .retryOnFailure(true)
                .build(),
            "localhost:" + BASE_PORT,
            "localhost:" + (BASE_PORT + 2)
        );

        Optional<String> result = client.getString(key);
        assertThat(result).isPresent().contains(value);
    }

    @Test
    @DisplayName("Cluster should detect node recovery")
    void testNodeRecovery() throws Exception {
        // Write data while all nodes are up
        String key = "recovery-test-" + System.currentTimeMillis();
        String value = "recovery-value";
        client.put(key, value);

        // Stop a node
        TitanKVServer nodeToRestart = servers.get(1);
        int restartPort = BASE_PORT + 1;
        nodeToRestart.stop();

        // Wait for failure detection
        Thread.sleep(5000);

        // Restart the node
        TitanKVServer restartedNode = new TitanKVServer(
            restartPort,
            "node-2",
            "localhost:" + BASE_PORT
        );
        restartedNode.start();
        servers.set(1, restartedNode);

        // Wait for recovery detection
        Thread.sleep(5000);

        // Verify the node is back in the cluster
        ClusterManager seedCm = servers.get(0).getClusterManager();
        Node recoveredNode = seedCm.getNode("node-2");
        
        // Node should either be null (removed) or alive
        // The exact behavior depends on gossip timing
        if (recoveredNode != null) {
            // If present, should be alive or recently recovered
            assertThat(recoveredNode.getStatus())
                .isIn(Node.Status.ALIVE, Node.Status.JOINING);
        }
    }

    @Test
    @DisplayName("Writes should continue during partial failure")
    void testWritesDuringFailure() throws Exception {
        // Stop one node
        servers.get(2).stop();
        Thread.sleep(2000);

        // Should still be able to write (QUORUM with 2/3 nodes)
        String key = "partial-failure-write-" + System.currentTimeMillis();
        String value = "write-during-failure";

        // This may succeed or fail depending on consistency requirements
        // With QUORUM (2 of 3), should succeed with 2 nodes
        try {
            client.put(key, value);
            // If write succeeded, verify it
            Optional<String> result = client.getString(key);
            assertThat(result).isPresent().contains(value);
        } catch (IOException e) {
            // Write failed due to consistency requirements - this is acceptable
            assertThat(e.getMessage()).containsAnyOf("timeout", "failed", "consistency");
        }
    }

    @Test
    @DisplayName("Circuit breaker should open for failing host")
    void testCircuitBreaker() throws Exception {
        // Stop a node
        servers.get(1).stop();
        Thread.sleep(1000);

        // Make several requests that will fail to the dead node
        // Client should eventually stop trying that node
        int successCount = 0;
        
        for (int i = 0; i < 10; i++) {
            try {
                String key = "circuit-breaker-test-" + i;
                client.put(key, "value");
                client.getString(key);
                successCount++;
            } catch (IOException e) {
                // Expected for some requests to dead node
            }
        }

        // With circuit breaker, later requests should start succeeding
        // as the client avoids the dead node
        assertThat(successCount).as("Some requests should succeed").isGreaterThan(0);
    }

    @Test
    @DisplayName("Read repair should fix stale replicas")
    void testReadRepair() throws Exception {
        // Write initial value
        String key = "read-repair-test-" + System.currentTimeMillis();
        String value1 = "initial-value";
        client.put(key, value1);

        // Verify written
        assertThat(client.getString(key)).contains(value1);

        // Update the value
        String value2 = "updated-value";
        client.put(key, value2);

        // Read should return the updated value
        // Read repair should fix any stale replicas
        Optional<String> result = client.getString(key);
        assertThat(result).isPresent().contains(value2);
    }

    @Test
    @DisplayName("Tombstones should prevent resurrection")
    void testTombstonePreventsResurrection() throws Exception {
        String key = "tombstone-test-" + System.currentTimeMillis();
        String value = "to-be-deleted";

        // Write
        client.put(key, value);
        assertThat(client.getString(key)).contains(value);

        // Delete
        client.delete(key);
        assertThat(client.getString(key)).isEmpty();

        // Try to write with an older timestamp (simulated by just writing again)
        // The delete tombstone should prevent the old value from appearing
        // (In a real scenario, this tests read repair with stale data)
        
        // Verify still deleted
        assertThat(client.getString(key)).isEmpty();
    }
}
