package com.titankv.consistency;

import com.titankv.TitanKVServer;
import com.titankv.cluster.ClusterManager;
import com.titankv.cluster.Node;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for the ReplicationManager with different consistency levels.
 */
class ReplicationManagerTest {

    private static final int BASE_PORT = 19200;

    private TitanKVServer node1;
    private TitanKVServer node2;
    private TitanKVServer node3;
    private ReplicationManager replicationManager;

    @BeforeEach
    void setUp() throws Exception {
        // Start a 3-node cluster for replication testing
        node1 = new TitanKVServer(BASE_PORT, "replica-node1", null);
        node1.start();
        Thread.sleep(200);

        node2 = new TitanKVServer(BASE_PORT + 1, "replica-node2", "localhost:" + BASE_PORT);
        node2.start();
        Thread.sleep(200);

        node3 = new TitanKVServer(BASE_PORT + 2, "replica-node3", "localhost:" + BASE_PORT);
        node3.start();
        Thread.sleep(500);

        // Create replication manager using node1's cluster manager
        replicationManager = new ReplicationManager(node1.getClusterManager(), 3, 5000);
    }

    @AfterEach
    void tearDown() {
        if (replicationManager != null) {
            replicationManager.shutdown();
        }
        if (node3 != null) node3.stop();
        if (node2 != null) node2.stop();
        if (node1 != null) node1.stop();
    }

    @Test
    void consistencyLevel_ONE_requiresOneResponse() {
        ConsistencyLevel level = ConsistencyLevel.ONE;

        assertThat(level.getRequired(1)).isEqualTo(1);
        assertThat(level.getRequired(3)).isEqualTo(1);
        assertThat(level.getRequired(5)).isEqualTo(1);
    }

    @Test
    void consistencyLevel_QUORUM_requiresMajority() {
        ConsistencyLevel level = ConsistencyLevel.QUORUM;

        assertThat(level.getRequired(1)).isEqualTo(1); // (1/2)+1 = 1
        assertThat(level.getRequired(3)).isEqualTo(2); // (3/2)+1 = 2
        assertThat(level.getRequired(5)).isEqualTo(3); // (5/2)+1 = 3
    }

    @Test
    void consistencyLevel_ALL_requiresAllReplicas() {
        ConsistencyLevel level = ConsistencyLevel.ALL;

        assertThat(level.getRequired(1)).isEqualTo(1);
        assertThat(level.getRequired(3)).isEqualTo(3);
        assertThat(level.getRequired(5)).isEqualTo(5);
    }

    @Test
    void consistencyLevel_canTolerate_calculatesCorrectly() {
        ConsistencyLevel one = ConsistencyLevel.ONE;
        ConsistencyLevel quorum = ConsistencyLevel.QUORUM;
        ConsistencyLevel all = ConsistencyLevel.ALL;

        // With RF=3
        assertThat(one.canTolerate(3, 2)).isTrue();     // Need 1, have 1
        assertThat(one.canTolerate(3, 3)).isFalse();    // Need 1, have 0

        assertThat(quorum.canTolerate(3, 1)).isTrue();  // Need 2, have 2
        assertThat(quorum.canTolerate(3, 2)).isFalse(); // Need 2, have 1

        assertThat(all.canTolerate(3, 0)).isTrue();     // Need 3, have 3
        assertThat(all.canTolerate(3, 1)).isFalse();    // Need 3, have 2
    }

    @Test
    void consistencyLevel_maxTolerableFailures() {
        assertThat(ConsistencyLevel.ONE.maxTolerableFailures(3)).isEqualTo(2);
        assertThat(ConsistencyLevel.QUORUM.maxTolerableFailures(3)).isEqualTo(1);
        assertThat(ConsistencyLevel.ALL.maxTolerableFailures(3)).isEqualTo(0);
    }

    @Test
    void replicationManager_getReplicationFactor() {
        assertThat(replicationManager.getReplicationFactor()).isEqualTo(3);
    }

    @Test
    void consistencyException_containsDetails() {
        ConsistencyException ex = new ConsistencyException(
            "Cannot meet consistency", ConsistencyLevel.QUORUM, 2, 1);

        assertThat(ex.getRequestedLevel()).isEqualTo(ConsistencyLevel.QUORUM);
        assertThat(ex.getRequiredResponses()).isEqualTo(2);
        assertThat(ex.getActualResponses()).isEqualTo(1);
        assertThat(ex.getMessage()).contains("QUORUM");
    }

    @Test
    void clusterManager_providesNodesForReplication() {
        ClusterManager cm = node1.getClusterManager();

        // Should get at least the local node for any key
        var nodes = cm.getNodesForKey("test-key", 3);
        assertThat(nodes).isNotEmpty();

        // Local node should be available
        assertThat(cm.getLocalNode()).isNotNull();
        assertThat(cm.getLocalNode().isAvailable()).isTrue();
    }

    @Test
    void node_statusTransitions() {
        Node node = new Node("test-node", "localhost", 9999);

        // Initial status is JOINING
        assertThat(node.getStatus()).isEqualTo(Node.Status.JOINING);

        // Set to ALIVE
        node.setStatus(Node.Status.ALIVE);
        assertThat(node.isAvailable()).isTrue();

        // Set to SUSPECT
        node.setStatus(Node.Status.SUSPECT);
        assertThat(node.isAvailable()).isFalse();

        // Update heartbeat recovers from SUSPECT
        node.updateHeartbeat();
        assertThat(node.getStatus()).isEqualTo(Node.Status.ALIVE);

        // Set to DEAD
        node.setStatus(Node.Status.DEAD);
        assertThat(node.isAvailable()).isFalse();
    }

    @Test
    void node_heartbeatTracking() throws InterruptedException {
        Node node = new Node("test-node", "localhost", 9999);
        node.updateHeartbeat();

        long initial = node.getLastHeartbeat();
        Thread.sleep(10);

        node.updateHeartbeat();
        assertThat(node.getLastHeartbeat()).isGreaterThan(initial);
        assertThat(node.getMillisSinceLastHeartbeat()).isLessThan(100);
    }

    @Test
    void node_equality() {
        Node node1 = new Node("node1", "localhost", 9001);
        Node node2 = new Node("node1", "localhost", 9001);
        Node node3 = new Node("node2", "localhost", 9001);

        assertThat(node1).isEqualTo(node2);
        assertThat(node1).isNotEqualTo(node3);
        assertThat(node1.hashCode()).isEqualTo(node2.hashCode());
    }

    @Test
    void node_fromAddress() {
        Node node = Node.fromAddress("myhost:8080");

        assertThat(node.getHost()).isEqualTo("myhost");
        assertThat(node.getPort()).isEqualTo(8080);
        assertThat(node.getAddress()).isEqualTo("myhost:8080");
    }

    @Test
    void node_fromAddress_defaultPort() {
        Node node = Node.fromAddress("myhost");

        assertThat(node.getHost()).isEqualTo("myhost");
        assertThat(node.getPort()).isEqualTo(9001); // Default port
    }
}
