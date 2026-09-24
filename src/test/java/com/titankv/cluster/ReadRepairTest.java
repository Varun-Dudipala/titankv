package com.titankv.cluster;

import com.titankv.TitanKVClient;
import com.titankv.core.KVStore;
import com.titankv.core.KeyValuePair;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * QUORUM reads return the newest version and repair replicas that hold an older one.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ReadRepairTest {

    private static final int BASE_PORT = 19900;
    private TestCluster cluster;
    private TitanKVClient client;

    @BeforeAll
    void startCluster() throws Exception {
        cluster = TestCluster.start(BASE_PORT, 3);
        client = cluster.client();
    }

    @AfterAll
    void stopCluster() {
        client.close();
        cluster.close();
    }

    @Test
    void replicaMissingAValueIsRepairedByARead() throws Exception {
        client.put("repair-missing", "value");
        KVStore replica = cluster.node(2).getStore();
        replica.delete("repair-missing"); // local-only removal: this replica lost the write

        assertThat(client.getString("repair-missing")).contains("value");
        TestCluster.awaitCondition(() -> replica.get("repair-missing").isPresent(), 5_000,
                "stale replica to be repaired");
    }

    @Test
    void staleValueDoesNotResurrectADeletedKey() throws Exception {
        client.put("repair-deleted", "old");
        KeyValuePair oldVersion = cluster.node(2).getStore().getRaw("repair-deleted").orElseThrow();
        client.delete("repair-deleted");

        // Roll one replica back to the pre-delete value, as if it had missed the delete
        KVStore replica = cluster.node(2).getStore();
        replica.delete("repair-deleted");
        replica.putIfNewer("repair-deleted", "old".getBytes(StandardCharsets.UTF_8),
                oldVersion.getTimestamp(), 0);

        assertThat(client.getString("repair-deleted")).isEmpty();
        TestCluster.awaitCondition(() -> replica.getRaw("repair-deleted").map(KeyValuePair::isTombstone)
                .orElse(false), 5_000, "stale replica to receive the tombstone");
    }

    @Test
    void newerWriteWinsOverOlderReplicaVersion() throws Exception {
        client.put("repair-newest", "v1");
        client.put("repair-newest", "v2");
        KVStore replica = cluster.node(1).getStore();
        long newest = replica.getRaw("repair-newest").orElseThrow().getTimestamp();
        replica.delete("repair-newest");
        replica.putIfNewer("repair-newest", "v1".getBytes(StandardCharsets.UTF_8), newest - 1, 0);

        for (int i = 0; i < 5; i++) {
            assertThat(client.getString("repair-newest")).contains("v2");
        }
        TestCluster.awaitCondition(() -> replica.get("repair-newest")
                .map(kv -> new String(kv.getValueUnsafe(), StandardCharsets.UTF_8).equals("v2"))
                .orElse(false), 5_000, "replica to be repaired to v2");
    }
}
