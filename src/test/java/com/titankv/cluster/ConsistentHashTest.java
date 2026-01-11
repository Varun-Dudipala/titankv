package com.titankv.cluster;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.*;

class ConsistentHashTest {

    private ConsistentHash hashRing;

    @BeforeEach
    void setUp() {
        hashRing = new ConsistentHash(100); // Fewer virtual nodes for faster tests
    }

    @Test
    void addNode_createsVirtualNodes() {
        Node node = new Node("node1", "localhost", 9001);
        node.setStatus(Node.Status.ALIVE);

        hashRing.addNode(node);

        assertThat(hashRing.getNodeCount()).isEqualTo(1);
        assertThat(hashRing.getVirtualNodeCount()).isEqualTo(100);
    }

    @Test
    void addNode_duplicateIgnored() {
        Node node = new Node("node1", "localhost", 9001);
        node.setStatus(Node.Status.ALIVE);

        hashRing.addNode(node);
        hashRing.addNode(node);

        assertThat(hashRing.getNodeCount()).isEqualTo(1);
    }

    @Test
    void removeNode_removesVirtualNodes() {
        Node node = new Node("node1", "localhost", 9001);
        node.setStatus(Node.Status.ALIVE);

        hashRing.addNode(node);
        hashRing.removeNode(node);

        assertThat(hashRing.getNodeCount()).isEqualTo(0);
        assertThat(hashRing.getVirtualNodeCount()).isEqualTo(0);
    }

    @Test
    void getNode_emptyRing_throwsException() {
        assertThatThrownBy(() -> hashRing.getNode("key"))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("No nodes in cluster");
    }

    @Test
    void getNode_singleNode_alwaysReturnsThatNode() {
        Node node = new Node("node1", "localhost", 9001);
        node.setStatus(Node.Status.ALIVE);
        hashRing.addNode(node);

        for (int i = 0; i < 100; i++) {
            Node result = hashRing.getNode("key" + i);
            assertThat(result).isEqualTo(node);
        }
    }

    @Test
    void getNode_consistentForSameKey() {
        Node node1 = new Node("node1", "localhost", 9001);
        Node node2 = new Node("node2", "localhost", 9002);
        Node node3 = new Node("node3", "localhost", 9003);
        node1.setStatus(Node.Status.ALIVE);
        node2.setStatus(Node.Status.ALIVE);
        node3.setStatus(Node.Status.ALIVE);

        hashRing.addNode(node1);
        hashRing.addNode(node2);
        hashRing.addNode(node3);

        String key = "testKey123";
        Node firstResult = hashRing.getNode(key);

        // Same key should always return the same node
        for (int i = 0; i < 100; i++) {
            assertThat(hashRing.getNode(key)).isEqualTo(firstResult);
        }
    }

    @Test
    void getNode_distributesKeysAcrossNodes() {
        Node node1 = new Node("node1", "localhost", 9001);
        Node node2 = new Node("node2", "localhost", 9002);
        Node node3 = new Node("node3", "localhost", 9003);
        node1.setStatus(Node.Status.ALIVE);
        node2.setStatus(Node.Status.ALIVE);
        node3.setStatus(Node.Status.ALIVE);

        hashRing.addNode(node1);
        hashRing.addNode(node2);
        hashRing.addNode(node3);

        Map<Node, Integer> distribution = new HashMap<>();
        int numKeys = 10000;

        for (int i = 0; i < numKeys; i++) {
            Node node = hashRing.getNode("key" + i);
            distribution.merge(node, 1, Integer::sum);
        }

        // Each node should have approximately 33% of keys (within 15% variance)
        double expectedPerNode = numKeys / 3.0;
        for (int count : distribution.values()) {
            assertThat((double) count)
                .isCloseTo(expectedPerNode, withinPercentage(15));
        }
    }

    @Test
    void getNodes_returnsDistinctNodes() {
        Node node1 = new Node("node1", "localhost", 9001);
        Node node2 = new Node("node2", "localhost", 9002);
        Node node3 = new Node("node3", "localhost", 9003);
        node1.setStatus(Node.Status.ALIVE);
        node2.setStatus(Node.Status.ALIVE);
        node3.setStatus(Node.Status.ALIVE);

        hashRing.addNode(node1);
        hashRing.addNode(node2);
        hashRing.addNode(node3);

        List<Node> nodes = hashRing.getNodes("testKey", 3);

        assertThat(nodes).hasSize(3);
        assertThat(new HashSet<>(nodes)).hasSize(3); // All distinct
    }

    @Test
    void getNodes_returnsAvailableNodesOnly() {
        Node node1 = new Node("node1", "localhost", 9001);
        Node node2 = new Node("node2", "localhost", 9002);
        Node node3 = new Node("node3", "localhost", 9003);
        node1.setStatus(Node.Status.ALIVE);
        node2.setStatus(Node.Status.DEAD); // Dead node
        node3.setStatus(Node.Status.ALIVE);

        hashRing.addNode(node1);
        hashRing.addNode(node2);
        hashRing.addNode(node3);

        List<Node> nodes = hashRing.getNodes("testKey", 3);

        // Should only return alive nodes
        assertThat(nodes).doesNotContain(node2);
        assertThat(nodes).hasSize(2);
    }

    @Test
    void getNodes_returnsFewerIfNotEnoughAvailable() {
        Node node1 = new Node("node1", "localhost", 9001);
        node1.setStatus(Node.Status.ALIVE);

        hashRing.addNode(node1);

        List<Node> nodes = hashRing.getNodes("testKey", 3);

        assertThat(nodes).hasSize(1);
        assertThat(nodes).contains(node1);
    }

    @Test
    void addNode_minimalKeyRedistribution() {
        Node node1 = new Node("node1", "localhost", 9001);
        Node node2 = new Node("node2", "localhost", 9002);
        node1.setStatus(Node.Status.ALIVE);
        node2.setStatus(Node.Status.ALIVE);

        hashRing.addNode(node1);
        hashRing.addNode(node2);

        // Record key assignments
        Map<String, Node> beforeAssignments = new HashMap<>();
        int numKeys = 1000;
        for (int i = 0; i < numKeys; i++) {
            String key = "key" + i;
            beforeAssignments.put(key, hashRing.getNode(key));
        }

        // Add a third node
        Node node3 = new Node("node3", "localhost", 9003);
        node3.setStatus(Node.Status.ALIVE);
        hashRing.addNode(node3);

        // Count keys that moved
        int movedKeys = 0;
        for (int i = 0; i < numKeys; i++) {
            String key = "key" + i;
            if (!hashRing.getNode(key).equals(beforeAssignments.get(key))) {
                movedKeys++;
            }
        }

        // With consistent hashing, approximately K/N keys should move
        // K = total keys, N = new number of nodes
        // Expected: ~1000/3 = ~333 keys (with some variance)
        double expectedMoved = numKeys / 3.0;
        assertThat((double) movedKeys)
            .isCloseTo(expectedMoved, withinPercentage(30));
    }

    @Test
    void removeNode_keysRedistributed() {
        Node node1 = new Node("node1", "localhost", 9001);
        Node node2 = new Node("node2", "localhost", 9002);
        Node node3 = new Node("node3", "localhost", 9003);
        node1.setStatus(Node.Status.ALIVE);
        node2.setStatus(Node.Status.ALIVE);
        node3.setStatus(Node.Status.ALIVE);

        hashRing.addNode(node1);
        hashRing.addNode(node2);
        hashRing.addNode(node3);

        // Get all keys assigned to node3
        Set<String> node3Keys = new HashSet<>();
        for (int i = 0; i < 1000; i++) {
            String key = "key" + i;
            if (hashRing.getNode(key).equals(node3)) {
                node3Keys.add(key);
            }
        }

        // Remove node3
        hashRing.removeNode(node3);

        // All keys that were on node3 should now be on other nodes
        for (String key : node3Keys) {
            Node newNode = hashRing.getNode(key);
            assertThat(newNode).isIn(node1, node2);
        }
    }

    @Test
    void containsNode_returnsCorrectly() {
        Node node = new Node("node1", "localhost", 9001);
        node.setStatus(Node.Status.ALIVE);

        assertThat(hashRing.containsNode(node)).isFalse();

        hashRing.addNode(node);
        assertThat(hashRing.containsNode(node)).isTrue();

        hashRing.removeNode(node);
        assertThat(hashRing.containsNode(node)).isFalse();
    }

    @Test
    void getAllNodes_returnsAllPhysicalNodes() {
        Node node1 = new Node("node1", "localhost", 9001);
        Node node2 = new Node("node2", "localhost", 9002);
        node1.setStatus(Node.Status.ALIVE);
        node2.setStatus(Node.Status.ALIVE);

        hashRing.addNode(node1);
        hashRing.addNode(node2);

        Set<Node> nodes = hashRing.getAllNodes();

        assertThat(nodes).containsExactlyInAnyOrder(node1, node2);
    }

    @Test
    void clear_removesAllNodes() {
        Node node1 = new Node("node1", "localhost", 9001);
        Node node2 = new Node("node2", "localhost", 9002);
        node1.setStatus(Node.Status.ALIVE);
        node2.setStatus(Node.Status.ALIVE);

        hashRing.addNode(node1);
        hashRing.addNode(node2);

        hashRing.clear();

        assertThat(hashRing.isEmpty()).isTrue();
        assertThat(hashRing.getNodeCount()).isEqualTo(0);
    }

    @Test
    void getDistribution_showsEvenDistribution() {
        Node node1 = new Node("node1", "localhost", 9001);
        Node node2 = new Node("node2", "localhost", 9002);
        node1.setStatus(Node.Status.ALIVE);
        node2.setStatus(Node.Status.ALIVE);

        hashRing.addNode(node1);
        hashRing.addNode(node2);

        Map<String, Double> distribution = hashRing.getDistribution();

        // Each node should own approximately 50% of virtual nodes
        assertThat(distribution.get("node1")).isCloseTo(50.0, within(5.0));
        assertThat(distribution.get("node2")).isCloseTo(50.0, within(5.0));
    }

    @Test
    void concurrentAccess_isThreadSafe() throws InterruptedException {
        // Add initial nodes
        for (int i = 1; i <= 3; i++) {
            Node node = new Node("node" + i, "localhost", 9000 + i);
            node.setStatus(Node.Status.ALIVE);
            hashRing.addNode(node);
        }

        // Run concurrent reads and writes
        int numThreads = 10;
        int opsPerThread = 1000;
        Thread[] threads = new Thread[numThreads];

        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            threads[t] = new Thread(() -> {
                Random random = new Random(threadId);
                for (int i = 0; i < opsPerThread; i++) {
                    // Mix of reads and writes
                    if (random.nextDouble() < 0.9) {
                        // Read operation
                        try {
                            hashRing.getNode("key" + i);
                        } catch (IllegalStateException e) {
                            // May happen if all nodes removed
                        }
                    } else {
                        // Write operation
                        Node node = new Node("temp" + threadId + "_" + i, "localhost", 10000 + i);
                        node.setStatus(Node.Status.ALIVE);
                        hashRing.addNode(node);
                        hashRing.removeNode(node);
                    }
                }
            });
            threads[t].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        // Should not have thrown any exceptions
        assertThat(hashRing.getNodeCount()).isGreaterThanOrEqualTo(3);
    }
}
