package com.titankv.cluster;

import com.titankv.TitanKVClient;
import com.titankv.TitanKVServer;
import com.titankv.consistency.AntiEntropy;
import com.titankv.core.KVStore;
import com.titankv.core.KeyValuePair;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Merkle-tree anti-entropy repairs replicas that diverged, without any client reads.
 */
@Tag("integration")
class AntiEntropyTest {

    private static final int BASE_PORT = 20010;

    @AfterEach
    void resetInterval() {
        System.clearProperty("titankv.anti.entropy.interval.ms");
    }

    @Test
    void repairFixesMissingStaleAndUnreplicatedKeysInBothDirections() throws Exception {
        System.setProperty("titankv.anti.entropy.interval.ms", "0"); // only explicit repairs
        try (TestCluster cluster = TestCluster.start(BASE_PORT, 3);
             TitanKVClient client = cluster.client()) {
            for (int i = 0; i < 200; i++) {
                client.put("ae:" + i, "v2-" + i);
            }
            client.delete("ae:deleted");
            for (int i = 0; i < 200; i++) {
                cluster.awaitReplicated("ae:" + i);
            }
            cluster.awaitReplicated("ae:deleted");
            KVStore diverged = cluster.node(2).getStore();

            for (int i = 0; i < 20; i++) {
                diverged.delete("ae:" + i); // lost writes
            }
            for (int i = 20; i < 30; i++) {
                rollBack(diverged, "ae:" + i, "v1-" + i); // stale versions
            }
            for (int i = 0; i < 5; i++) {
                diverged.putIfNewer("only-on-node-3:" + i, bytes("x"), System.currentTimeMillis(), 0);
            }

            AntiEntropy.RepairStats stats = antiEntropy(cluster.node(0)).repairWith(peer(cluster, 0, 2));

            assertThat(stats.pushed()).isEqualTo(30);
            assertThat(stats.pulled()).isEqualTo(5);
            assertThat(stats.differingLeaves()).isBetween(1, 35);
            for (int i = 0; i < 200; i++) {
                assertThat(value(diverged, "ae:" + i)).isEqualTo("v2-" + i);
            }
            for (int i = 0; i < 5; i++) {
                assertThat(cluster.node(0).getStore().get("only-on-node-3:" + i)).isPresent();
            }
            assertThat(antiEntropy(cluster.node(0)).repairWith(peer(cluster, 0, 2)).differingLeaves()).isZero();
        }
    }

    @Test
    void tombstonesPropagateSoDeletedKeysAreNotResurrected() throws Exception {
        System.setProperty("titankv.anti.entropy.interval.ms", "0");
        try (TestCluster cluster = TestCluster.start(BASE_PORT + 10, 3);
             TitanKVClient client = cluster.client()) {
            client.put("ae-deleted", "value");
            cluster.awaitReplicated("ae-deleted");
            KeyValuePair original = cluster.node(1).getStore().getRaw("ae-deleted").orElseThrow();
            client.delete("ae-deleted");
            TestCluster.awaitCondition(() -> cluster.node(1).getStore().getRaw("ae-deleted")
                    .map(KeyValuePair::isTombstone).orElse(false), 5_000, "tombstone to reach node 2");
            KVStore diverged = cluster.node(1).getStore();
            diverged.delete("ae-deleted");
            diverged.putIfNewer("ae-deleted", bytes("value"), original.getTimestamp(), 0); // missed the delete

            antiEntropy(cluster.node(0)).repairWith(peer(cluster, 0, 1));
            assertThat(diverged.getRaw("ae-deleted").orElseThrow().isTombstone()).isTrue();
        }
    }

    @Test
    void periodicRepairConvergesReplicasWithoutReads() throws Exception {
        System.setProperty("titankv.anti.entropy.interval.ms", "500");
        try (TestCluster cluster = TestCluster.start(BASE_PORT + 20, 3);
             TitanKVClient client = cluster.client()) {
            for (int i = 0; i < 50; i++) {
                client.put("periodic:" + i, "v" + i);
            }
            for (int i = 0; i < 50; i++) {
                cluster.awaitReplicated("periodic:" + i);
            }
            KVStore diverged = cluster.node(1).getStore();
            for (int i = 0; i < 50; i++) {
                diverged.delete("periodic:" + i);
            }
            TestCluster.awaitCondition(() -> diverged.size() == 50, 15_000, "background repair to restore 50 keys");
        }
    }

    private static void rollBack(KVStore store, String key, String olderValue) {
        long current = store.getRaw(key).orElseThrow().getTimestamp();
        store.delete(key);
        store.putIfNewer(key, bytes(olderValue), current - 1, 0);
    }

    private static AntiEntropy antiEntropy(TitanKVServer server) {
        return server.getReplicationManager().getAntiEntropy();
    }

    private static Node peer(TestCluster cluster, int from, int to) {
        return cluster.node(from).getClusterManager().getNode(cluster.node(to).getNodeId());
    }

    private static String value(KVStore store, String key) {
        return store.get(key).map(kv -> new String(kv.getValueUnsafe(), StandardCharsets.UTF_8)).orElse(null);
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
