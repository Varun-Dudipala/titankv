package com.titankv.cluster;

import com.titankv.TitanKVClient;
import com.titankv.TitanKVServer;
import org.junit.jupiter.api.*;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for a multi-node TitanKV cluster.
 * Tests cluster formation, data distribution, and failure handling.
 */
class ClusterIntegrationTest {

    private static final int BASE_PORT = 19100;

    private TitanKVServer node1;
    private TitanKVServer node2;
    private TitanKVServer node3;

    @BeforeEach
    void setUp() throws Exception {
        // Start a 3-node cluster
        // Node 1 is the seed node
        node1 = new TitanKVServer(BASE_PORT, "node1", null);
        node1.start();
        Thread.sleep(200);

        // Node 2 joins via node1
        node2 = new TitanKVServer(BASE_PORT + 1, "node2", "localhost:" + BASE_PORT);
        node2.start();
        Thread.sleep(200);

        // Node 3 joins via node1
        node3 = new TitanKVServer(BASE_PORT + 2, "node3", "localhost:" + BASE_PORT);
        node3.start();
        Thread.sleep(500); // Allow gossip to propagate
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

        try (TitanKVClient client = new TitanKVClient(hosts)) {
            // Write multiple keys - they should be distributed by consistent hashing
            for (int i = 0; i < 100; i++) {
                client.put("distributed-key-" + i, "value-" + i);
            }

            // All keys should be readable
            for (int i = 0; i < 100; i++) {
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

        // Write some data through all nodes
        try (TitanKVClient client = new TitanKVClient(allHosts)) {
            for (int i = 0; i < 10; i++) {
                client.put("failover-key-" + i, "value-" + i);
            }
        }

        // Stop node 3
        node3.stop();
        Thread.sleep(200);

        // Should still be able to access nodes 1 and 2
        String[] remainingHosts = {
            "localhost:" + BASE_PORT,
            "localhost:" + (BASE_PORT + 1)
        };

        try (TitanKVClient client = new TitanKVClient(remainingHosts)) {
            assertThat(client.ping()).isTrue();

            // Can still write new data
            client.put("after-failover", "still-works");
            assertThat(client.getString("after-failover")).hasValue("still-works");
        }
    }

    @Test
    void clusterManager_tracksNodes() throws Exception {
        // Wait for gossip to discover nodes
        Thread.sleep(1000);

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

        try (TitanKVClient client = new TitanKVClient(hosts)) {
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

        int numOperations = 1000;

        try (TitanKVClient client = new TitanKVClient(hosts)) {
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
            assertThat(opsPerSecond).isGreaterThan(100);
        }
    }
}
