package com.titankv.cluster;

import com.titankv.TitanKVClient;
import com.titankv.TitanKVServer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * After a node joins, existing nodes stream it its keys and can then drop the keys they gave up.
 */
@Tag("integration")
class CleanupTest {

    private static final int BASE_PORT = 20400;
    private static final int KEYS = 200;

    @Test
    void joinThenCleanupLeavesEveryKeyOnExactlyItsReplicas() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT, 3);
             TitanKVClient client = cluster.client(cluster.address(0))) {
            for (int i = 0; i < KEYS; i++) {
                client.put("cleanup:" + i, "v" + i);
            }
            for (int i = 0; i < KEYS; i++) {
                cluster.awaitReplicated("cleanup:" + i);
            }

            TitanKVServer joined = cluster.addNode();
            cluster.awaitConverged(20_000);
            TestCluster.awaitCondition(() -> keysOwnedBy(cluster, joined).stream()
                    .allMatch(key -> joined.getStore().get(key).isPresent()), 20_000,
                    "the new node to receive the keys it now replicates");

            int removed = 0;
            for (int i = 0; i < 3; i++) {
                try (TitanKVClient admin = cluster.client(cluster.address(i))) {
                    removed += admin.cleanup();
                }
            }
            assertThat(removed).isGreaterThan(0);

            for (int n = 0; n < cluster.size(); n++) {
                TitanKVServer server = cluster.node(n);
                for (int i = 0; i < KEYS; i++) {
                    String key = "cleanup:" + i;
                    boolean replica = replicasOf(cluster, key).stream().anyMatch(r -> r.getId().equals(server.getNodeId()));
                    assertThat(server.getStore().get(key).isPresent()).as(server.getNodeId() + " holds " + key).isEqualTo(replica);
                }
            }
            for (int i = 0; i < KEYS; i++) {
                assertThat(client.getString("cleanup:" + i)).contains("v" + i);
            }
        }
    }

    private static List<Node> replicasOf(TestCluster cluster, String key) {
        return cluster.node(0).getClusterManager().getReplicasForKey(key, 3);
    }

    private static List<String> keysOwnedBy(TestCluster cluster, TitanKVServer server) {
        List<String> owned = new java.util.ArrayList<>();
        for (int i = 0; i < KEYS; i++) {
            String key = "cleanup:" + i;
            if (replicasOf(cluster, key).stream().anyMatch(r -> r.getId().equals(server.getNodeId()))) {
                owned.add(key);
            }
        }
        return owned;
    }
}
