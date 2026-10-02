package com.titankv.cluster;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Gossip membership: convergence, failure detection, recovery and graceful shutdown.
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
            cluster.crashNode(3);

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
    void gracefullyStoppedNodeIsDownAtOnceStaysOnTheRingAndRejoins() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT + 20, 3)) {
            String stoppedId = cluster.node(2).getNodeId();
            cluster.stopNode(2);

            // The shutdown announcement marks it DEAD well before the 10s failure detector would
            TestCluster.awaitCondition(() -> allOthersSee(cluster, 2, stoppedId, Node.Status.DEAD),
                    2_000, "remaining nodes to mark " + stoppedId + " DEAD");
            // Several gossip rounds later, stale digests must not have revived it, and it is
            // still a replica of its keys
            Thread.sleep(3_000);
            assertThat(allOthersSee(cluster, 2, stoppedId, Node.Status.DEAD)).isTrue();
            for (int i = 0; i < 2; i++) {
                assertThat(cluster.node(i).getClusterManager().getHashRing().getNodeCount()).isEqualTo(3);
            }

            cluster.startNode(2);
            cluster.awaitConverged(20_000);
        }
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
