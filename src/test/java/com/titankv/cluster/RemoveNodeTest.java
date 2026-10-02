package com.titankv.cluster;

import com.titankv.TitanKVClient;
import com.titankv.TitanKVServer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Permanently removing a dead node, after which every key is back to three replicas.
 */
@Tag("integration")
class RemoveNodeTest {

    private static final int BASE_PORT = 20300;
    private static final int KEYS = 100;

    @Test
    void removedNodeLeavesEveryMemberAndItsDataIsReReplicated() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT, 4);
             TitanKVClient client = cluster.client(cluster.address(0))) {
            for (int i = 0; i < KEYS; i++) {
                client.put("rm:" + i, "v" + i);
            }
            String deadId = cluster.node(3).getNodeId();
            cluster.crashNode(3);
            TestCluster.awaitCondition(() -> status(cluster, 0, deadId) == Node.Status.DEAD, 20_000,
                    deadId + " to be marked DEAD");

            assertThatThrownBy(() -> client.removeClusterNode(cluster.node(1).getNodeId()))
                    .isInstanceOf(IOException.class).hasMessageContaining("only DEAD nodes");
            assertThat(client.clusterStatus()).contains(deadId).contains("DEAD");

            client.removeClusterNode(deadId);
            TestCluster.awaitCondition(() -> cluster.node(0).getClusterManager().getNode(deadId) == null
                            && cluster.node(1).getClusterManager().getNode(deadId) == null
                            && cluster.node(2).getClusterManager().getNode(deadId) == null,
                    10_000, "every node to drop " + deadId);

            Map<String, TitanKVServer> byId = new HashMap<>();
            for (int i = 0; i < 3; i++) {
                byId.put(cluster.node(i).getNodeId(), cluster.node(i));
            }
            TestCluster.awaitCondition(() -> fullyReplicated(cluster, byId), 30_000,
                    "every key to be on all three of its new replicas");

            Thread.sleep(3_000); // stale gossip must not bring the removed node back
            assertThat(cluster.node(1).getClusterManager().getNode(deadId)).isNull();
        }
    }

    private static Node.Status status(TestCluster cluster, int observer, String nodeId) {
        Node node = cluster.node(observer).getClusterManager().getNode(nodeId);
        return node != null ? node.getStatus() : null;
    }

    private static boolean fullyReplicated(TestCluster cluster, Map<String, TitanKVServer> byId) {
        for (int i = 0; i < KEYS; i++) {
            String key = "rm:" + i;
            List<Node> replicas = cluster.node(0).getClusterManager().getReplicasForKey(key, 3);
            for (Node replica : replicas) {
                TitanKVServer server = byId.get(replica.getId());
                if (server == null || server.getStore().get(key).isEmpty()) {
                    return false;
                }
            }
        }
        return true;
    }
}
