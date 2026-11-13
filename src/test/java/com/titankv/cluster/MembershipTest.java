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

    /**
     * A restarted node must learn every member, including ones that are down, before it serves
     * clients. Down members are still replicas, so leaving one out of its ring would send reads
     * and writes for its keys to the wrong nodes.
     */
    @Test
    void restartedNodeLearnsDownMembersBeforeServing() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT + 30, 4)) {
            String downId = cluster.node(3).getNodeId();
            cluster.crashNode(3);
            TestCluster.awaitCondition(() -> allOthersSee(cluster, 3, downId, Node.Status.DEAD),
                    20_000, "peers to mark " + downId + " DEAD");

            cluster.crashNode(0);
            cluster.startNode(0);
            ClusterManager restarted = cluster.node(0).getClusterManager();
            TestCluster.awaitCondition(restarted::isReady, 10_000, "the restarted node to become ready");

            assertThat(restarted.getHashRing().getNodeCount()).isEqualTo(4);
            assertThat(restarted.getNode(downId)).isNotNull();
            assertThat(restarted.getNode(downId).getStatus()).isEqualTo(Node.Status.DEAD);
        }
    }

    /**
     * Nodes that keep nothing on disk and restart at the same moment must not take each other's
     * word for the membership: neither knows it. They wait until every seed is back.
     */
    @Test
    void nodesRestartedTogetherWithNothingSavedWaitForTheWholeCluster() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT + 40, 5)) {
            for (int i = 0; i < 5; i++) {
                cluster.crashNode(i);
            }
            ClusterManager first = cluster.startNode(0).getClusterManager();
            ClusterManager second = cluster.startNode(1).getClusterManager();

            TestCluster.awaitCondition(() -> first.getNodeCount() == 2 && second.getNodeCount() == 2,
                    10_000, "the two restarted nodes to find each other");
            Thread.sleep(2_500); // several gossip rounds
            assertThat(first.isReady()).as("ready knowing 2 of 5 members").isFalse();
            assertThat(second.isReady()).as("ready knowing 2 of 5 members").isFalse();
            assertThat(first.hasCompleteMembership()).isFalse();
            try (com.titankv.TitanKVClient client = cluster.client(cluster.address(0), cluster.address(1))) {
                org.assertj.core.api.Assertions.assertThatThrownBy(() -> client.put("too-early", "value"))
                        .isInstanceOf(java.io.IOException.class).hasMessageContaining("joining the cluster");
            }

            for (int i = 2; i < 5; i++) {
                cluster.startNode(i);
            }
            cluster.awaitConverged(30_000);
            for (int i = 0; i < 5; i++) {
                assertThat(cluster.node(i).getClusterManager().getHashRing().getNodeCount()).isEqualTo(5);
            }
        }
    }

    /**
     * A node that was cut off while a dead node was removed must still drop it. The removal is
     * announced three times over UDP, which a partitioned node never receives; it used to keep
     * the removed node on its ring for good and place keys differently from everyone else.
     */
    @Test
    void removalReachesANodeThatWasCutOffWhenItHappened() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT + 50, 4)) {
            String removedId = cluster.node(3).getNodeId();
            cluster.crashNode(3);
            TestCluster.awaitCondition(() -> allOthersSee(cluster, 3, removedId, Node.Status.DEAD),
                    20_000, "peers to mark " + removedId + " DEAD");

            cluster.isolate(2);
            try (com.titankv.TitanKVClient admin = cluster.client(cluster.address(0))) {
                admin.removeClusterNode(removedId);
            }
            TestCluster.awaitCondition(() -> cluster.node(1).getClusterManager().getNode(removedId) == null,
                    10_000, "the removal to reach the connected node");
            Thread.sleep(3_500); // all three announcements have been sent, and dropped by the partition
            assertThat(cluster.node(2).getClusterManager().getNode(removedId)).isNotNull();

            cluster.heal();
            TestCluster.awaitCondition(() -> cluster.node(2).getClusterManager().getNode(removedId) == null,
                    15_000, "the node that was cut off to learn of the removal");
            for (int i = 0; i < 3; i++) {
                assertThat(cluster.node(i).getClusterManager().getHashRing().getNodeCount())
                        .as("ring size on node " + (i + 1)).isEqualTo(3);
            }
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
