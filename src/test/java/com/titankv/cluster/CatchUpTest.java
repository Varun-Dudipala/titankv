package com.titankv.cluster;

import com.titankv.TitanKVClient;
import com.titankv.TitanKVServer;
import com.titankv.consistency.ConsistencyLevel;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A node that starts with no data in an existing cluster (it has no WAL, its disk was replaced,
 * or it has just joined) must be repaired from its peers before it answers reads. Quorum overlap
 * assumes a replica still holds what it acknowledged: an empty replica answering "not found",
 * together with one replica that missed a write, would outvote the one replica that has it.
 */
@Tag("integration")
class CatchUpTest {

    private static final int BASE_PORT = 24700;

    /**
     * Forty writes reach nodes 1 and 2 but not node 3. Node 2 then restarts with nothing (these
     * nodes have no WAL). While node 2 cannot repair itself, a QUORUM read through node 3 has to
     * be answered by node 1: before nodes caught up first, node 3 and the empty node 2 formed a
     * quorum of two replicas that both said "not found" for a write that had been acknowledged.
     */
    @Test
    void emptyRestartedNodeDoesNotAnswerReadsUntilItHasCaughtUp() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT, 3)) {
            List<String> keys = new ArrayList<>();
            cluster.cutOneWay(0, 2);
            try (TitanKVClient viaNode1 = cluster.client(cluster.address(0))) {
                for (int i = 0; i < 40; i++) {
                    keys.add("catch-up-" + i);
                    viaNode1.put("catch-up-" + i, "value-" + i);
                }
            }
            assertThat(cluster.node(2).getStore().size()).as("keys on the node that missed the writes").isZero();

            cluster.crashNode(1);
            cluster.cutRequestsFrom(1); // it will be seen as up, but cannot ask its peers for anything
            TitanKVServer restarted = cluster.startNode(1);
            TestCluster.awaitCondition(() -> restarted.getClusterManager().isCatchingUp()
                    && restarted.getClusterManager().hasCompleteMembership()
                    && cluster.node(2).getClusterManager().getNode(restarted.getNodeId()).isAvailable(),
                    10_000, "the restarted node to be back in the cluster and catching up");
            assertThat(restarted.getClusterManager().isReady()).isFalse();

            try (TitanKVClient viaNode3 = cluster.client(cluster.address(2));
                 TitanKVClient viaRestarted = cluster.client(cluster.address(1))) {
                for (int i = 0; i < 40; i++) {
                    assertThat(viaNode3.getString(keys.get(i))).as(keys.get(i)).contains("value-" + i);
                }
                // Asked directly, as a coordinator or as a replica, it declines
                assertThatThrownBy(() -> viaRestarted.getString(keys.get(0)))
                        .isInstanceOf(IOException.class).hasMessageContaining("joining the cluster");
                assertThatThrownBy(() -> viaRestarted.getInternalWithMetadata(keys.get(0)))
                        .isInstanceOf(IOException.class).hasMessageContaining("catching up");

                cluster.heal();
                TestCluster.awaitCondition(() -> restarted.getClusterManager().isReady(), 15_000,
                        "the restarted node to catch up once it can reach its peers");
                assertThat(restarted.getStore().size()).isEqualTo(40);
                for (int i = 0; i < 40; i++) {
                    assertThat(viaRestarted.getString(keys.get(i), ConsistencyLevel.ONE)).contains("value-" + i);
                }
            }
        }
    }

    /**
     * A node that joins holds none of the ranges it has just taken over. It is a replica for
     * writes at once, but stays out of reads until it has been repaired from the others.
     */
    @Test
    void joiningNodeDoesNotAnswerReadsBeforeItHasItsData() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT + 10, 3);
             TitanKVClient client = cluster.client(cluster.address(0), cluster.address(1), cluster.address(2))) {
            for (int i = 0; i < 60; i++) {
                client.put("join-" + i, "value-" + i, ConsistencyLevel.ALL);
            }

            cluster.cutRequestsFrom(3);
            TitanKVServer joined = cluster.addNode();
            TestCluster.awaitCondition(() -> joined.getClusterManager().hasCompleteMembership()
                    && cluster.node(0).getClusterManager().getNodeCount() == 4, 10_000, "the new node to be known");
            assertThat(joined.getClusterManager().isCatchingUp()).isTrue();
            assertThat(joined.getClusterManager().isReady()).isFalse();

            // Reads keep working through the old nodes, for keys the new node now replicates too,
            // and writes reach the new node while it catches up
            int replicatedByNewNode = 0;
            for (int i = 0; i < 60; i++) {
                assertThat(client.getString("join-" + i)).as("join-" + i).contains("value-" + i);
                replicatedByNewNode += cluster.replicasOf("join-" + i, 0).contains(3) ? 1 : 0;
            }
            assertThat(replicatedByNewNode).isGreaterThan(5);
            try (TitanKVClient viaNewNode = cluster.client(cluster.address(3))) {
                assertThatThrownBy(() -> viaNewNode.getInternalWithMetadata("join-0"))
                        .isInstanceOf(IOException.class).hasMessageContaining("catching up");
            }
            client.put("written-during-the-join", "new", ConsistencyLevel.ALL);

            cluster.heal();
            cluster.awaitConverged(20_000);
            assertThat(joined.getClusterManager().isCatchingUp()).isFalse();
            for (int i = 0; i < 60; i++) {
                if (cluster.replicasOf("join-" + i, 0).contains(3)) {
                    assertThat(joined.getStore().get("join-" + i)).as("join-" + i + " on the new node").isPresent();
                }
            }
            assertThat(client.getString("written-during-the-join", ConsistencyLevel.ALL)).contains("new");
        }
    }

    /**
     * Nodes with nothing saved that start a new cluster have nothing to catch up on, and must
     * still become ready.
     */
    @Test
    void newClusterOfEmptyNodesBecomesReady() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT + 20, 5);
             TitanKVClient client = cluster.client()) {
            for (int i = 0; i < 5; i++) {
                assertThat(cluster.node(i).getClusterManager().isCatchingUp()).isFalse();
            }
            client.put("first", "write");
            assertThat(client.getString("first")).contains("write");
        }
    }
}
