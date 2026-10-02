package com.titankv.cluster;

import com.titankv.TitanKVClient;
import com.titankv.consistency.ConsistencyLevel;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * With replication factor 3, QUORUM needs 2 acknowledgements no matter how many nodes are down.
 */
@Tag("integration")
class QuorumTest {

    private static final int BASE_PORT = 19800;

    @Test
    void quorumToleratesOneFailureButNotTwo() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT, 3)) {
            String survivor = cluster.address(0);

            cluster.stopNode(2);
            awaitAlive(cluster, 2);
            try (TitanKVClient client = cluster.client(survivor)) {
                client.put("quorum-key", "written-with-one-node-down");
                assertThat(client.getString("quorum-key")).contains("written-with-one-node-down");
            }

            cluster.crashNode(1);
            awaitAlive(cluster, 1);
            try (TitanKVClient client = cluster.client(survivor)) {
                assertThatThrownBy(() -> client.put("quorum-key", "must-not-be-acknowledged"))
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining("Not enough replicas");
                assertThatThrownBy(() -> client.getString("quorum-key"))
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining("Not enough replicas");
            }
        }
    }


    /**
     * A graceful shutdown is not a decommission. Nodes that shut down stay replicas, so QUORUM still
     * needs 2 of the key's 3 replicas: with two of three nodes stopped it must fail, rather than
     * the ring shrinking to one node and QUORUM accepting a single copy.
     */
    @Test
    void gracefulShutdownsDoNotShrinkTheQuorum() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT + 10, 3)) {
            String survivor = cluster.address(0);
            cluster.stopNode(1);
            cluster.stopNode(2);
            awaitAlive(cluster, 1);
            assertThat(cluster.node(0).getClusterManager().getHashRing().getNodeCount()).isEqualTo(3);

            try (TitanKVClient client = cluster.client(survivor)) {
                assertThatThrownBy(() -> client.put("shutdown-key", "single-copy"))
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining("Not enough replicas");
                // A client that accepts one copy can still choose ONE for this request
                client.put("shutdown-key", "single-copy", ConsistencyLevel.ONE);
                assertThat(client.getString("shutdown-key", ConsistencyLevel.ONE)).contains("single-copy");
                assertThatThrownBy(() -> client.getString("shutdown-key", ConsistencyLevel.QUORUM))
                        .hasMessageContaining("Not enough replicas");
            }
        }
    }

    private static void awaitAlive(TestCluster cluster, int expectedAlive) {
        TestCluster.awaitCondition(
                () -> cluster.node(0).getClusterManager().getAliveNodeCount() == expectedAlive,
                20_000, "node 0 to see " + expectedAlive + " alive nodes");
    }
}
