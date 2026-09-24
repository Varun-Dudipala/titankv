package com.titankv.cluster;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Gossip membership: convergence, failure detection, recovery and graceful leave.
 */
@Tag("integration")
class MembershipTest {

    private static final int BASE_PORT = 19700;

    @Test
    void fiveNodesStartedBackToBackConvergeViaGossip() throws Exception {
        // Nodes 3-5 only know the seed; they must learn about each other through digests.
        try (TestCluster cluster = TestCluster.start(BASE_PORT, 5)) {
            for (int i = 0; i < 5; i++) {
                assertThat(cluster.node(i).getClusterManager().getHashRing().getNodeCount()).isEqualTo(5);
            }
        }
    }

    @Test
    void crashedNodeIsDetectedDeadAndRecoversAfterRestart() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT + 10, 4)) {
            String crashedId = cluster.node(3).getNodeId();
            simulateCrash(cluster, 3);

            TestCluster.awaitCondition(() -> allOthersSee(cluster, 3, crashedId, Node.Status.DEAD),
                    20_000, "surviving nodes to mark " + crashedId + " DEAD");
            for (int i = 0; i < 3; i++) {
                assertThat(cluster.node(i).getClusterManager().getAliveNodeCount()).isEqualTo(3);
            }

            cluster.startNode(3);
            cluster.awaitConverged(20_000);
        }
    }

    @Test
    void gracefulLeaveRemovesNodeWithoutItComingBack() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT + 20, 3)) {
            String leftId = cluster.node(2).getNodeId();
            cluster.stopNode(2);

            TestCluster.awaitCondition(() -> cluster.node(0).getClusterManager().getNode(leftId) == null
                            && cluster.node(1).getClusterManager().getNode(leftId) == null,
                    5_000, "remaining nodes to drop " + leftId);
            // Several gossip rounds later, stale digests must not have re-added it
            Thread.sleep(3_000);
            assertThat(cluster.node(0).getClusterManager().getNode(leftId)).isNull();
            assertThat(cluster.node(1).getClusterManager().getNode(leftId)).isNull();
        }
    }

    /**
     * Stop the node's gossip without the LEAVE broadcast a graceful shutdown sends, so the
     * others have to detect the failure from missing heartbeats.
     */
    private static void simulateCrash(TestCluster cluster, int index) {
        ClusterManager crashed = cluster.node(index).getClusterManager();
        for (Node peer : crashed.getAllNodes()) {
            if (!peer.equals(crashed.getLocalNode())) {
                crashed.removeNode(peer);
            }
        }
        cluster.stopNode(index);
    }

    private static boolean allOthersSee(TestCluster cluster, int excluded, String nodeId, Node.Status status) {
        for (int i = 0; i < cluster.size(); i++) {
            if (i == excluded) {
                continue;
            }
            Node node = cluster.node(i).getClusterManager().getNode(nodeId);
            if (node == null || node.getStatus() != status) {
                return false;
            }
        }
        return true;
    }
}
