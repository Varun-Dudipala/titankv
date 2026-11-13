package com.titankv.cluster;

import com.titankv.TitanKVClient;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Last-write-wins with a coordinator whose clock is 30 seconds behind.
 */
@Tag("integration")
class ClockSkewTest {

    private static final int BASE_PORT = 20200;
    private static final long SKEW_MS = -30_000;

    @Test
    void replicaClocksAdvancePastReplicatedWrites() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT, 3);
             TitanKVClient viaNode1 = cluster.client(cluster.address(0));
             TitanKVClient viaSlowNode = cluster.client(cluster.address(1))) {
            cluster.node(1).getClusterManager().getClock().setSkewMillis(SKEW_MS);

            viaNode1.put("skew-key", "first");
            cluster.awaitReplicated("skew-key"); // the slow node is a replica and observes the version
            viaSlowNode.put("skew-key", "second");

            assertThat(viaNode1.getString("skew-key")).contains("second");
        }
    }

    @Test
    void causalContextOrdersAWriteAfterWhatTheClientHasSeen() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT + 10, 4)) {
            cluster.node(1).getClusterManager().getClock().setSkewMillis(SKEW_MS);
            String key = keyNotReplicatedOn(cluster, 1);

            try (TitanKVClient writer = cluster.client(cluster.address(0));
                 TitanKVClient reader = cluster.client(cluster.address(0));
                 TitanKVClient slowWithoutContext = cluster.client(cluster.address(1));
                 TitanKVClient slowWithContext = cluster.client(cluster.address(1))) {
                writer.put(key, "v1");
                reader.getString(key);

                // Control: the slow node has never seen v1's version, so without context its write loses
                slowWithoutContext.put(key, "lost-to-skew");
                assertThat(reader.getString(key)).contains("v1");

                slowWithContext.observeCausalContext(reader.getCausalContext());
                slowWithContext.put(key, "v2");
                assertThat(reader.getString(key)).contains("v2");
            }
        }
    }

    /**
     * A client's write through a node whose clock runs fast fails for lack of a quorum, but that
     * node has applied it locally. The client fails over to another node and its next write is
     * acknowledged. When the first node's copy reaches the other replicas, the acknowledged write
     * must still be the one that wins: it came later, from the same client.
     *
     * Known gap, reproduced by this test: the failed write wins. The coordinator picks the
     * version, so the client never learns the version of a write whose outcome it did not see,
     * and its causal context cannot order the next write after it. With clocks 20 seconds apart
     * the second coordinator issues a lower version than the first did.
     */
    @Test
    @org.junit.jupiter.api.Disabled("Known gap: a failed write through a fast-clock node can override the "
            + "same client's later acknowledged write; needs versions the client can know (see README limitations)")
    void acknowledgedWriteIsNotOverriddenByTheClientsEarlierFailedWrite() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT + 20, 3);
             TitanKVClient viaFastNode = cluster.client(cluster.address(0));
             TitanKVClient viaOtherNode = cluster.client(cluster.address(1))) {
            cluster.node(0).getClusterManager().getClock().setSkewMillis(20_000);

            cluster.isolate(0);
            // Node 1 has not noticed yet: it writes its own copy, then cannot reach a quorum
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> viaFastNode.put("failover-key", "failed"))
                    .isInstanceOf(java.io.IOException.class);
            assertThat(cluster.node(0).getStore().get("failover-key")).isPresent();

            // The same client, now talking to node 2, with everything it has seen so far
            viaOtherNode.observeCausalContext(viaFastNode.getCausalContext());
            viaOtherNode.put("failover-key", "acknowledged");

            cluster.heal();
            cluster.awaitConverged(20_000);
            TestCluster.awaitCondition(() -> cluster.node(0).getReplicationManager().getHintedHandoff()
                    .totalPendingHints() == 0, 15_000, "node 1 to deliver the failed write's hints");
            assertThat(viaOtherNode.getString("failover-key", com.titankv.consistency.ConsistencyLevel.ALL))
                    .contains("acknowledged");
        }
    }

    private static String keyNotReplicatedOn(TestCluster cluster, int index) {
        String nodeId = cluster.node(index).getNodeId();
        for (int i = 0; ; i++) {
            String key = "skew-" + i;
            List<Node> replicas = cluster.node(0).getClusterManager().getReplicasForKey(key, 3);
            if (replicas.stream().noneMatch(n -> n.getId().equals(nodeId))) {
                return key;
            }
        }
    }
}
