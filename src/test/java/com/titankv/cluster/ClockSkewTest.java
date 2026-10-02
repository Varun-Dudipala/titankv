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
