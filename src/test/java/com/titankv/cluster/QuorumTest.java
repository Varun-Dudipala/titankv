package com.titankv.cluster;

import com.titankv.TitanKVClient;
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

            crash(cluster, 1);
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
     * Stop a node without the graceful LEAVE, so the survivor still counts it as a member.
     */
    private static void crash(TestCluster cluster, int index) {
        ClusterManager crashed = cluster.node(index).getClusterManager();
        for (Node peer : crashed.getAllNodes()) {
            if (!peer.equals(crashed.getLocalNode())) {
                crashed.removeNode(peer);
            }
        }
        cluster.stopNode(index);
    }

    private static void awaitAlive(TestCluster cluster, int expectedAlive) {
        TestCluster.awaitCondition(
                () -> cluster.node(0).getClusterManager().getAliveNodeCount() == expectedAlive,
                20_000, "node 0 to see " + expectedAlive + " alive nodes");
    }
}
