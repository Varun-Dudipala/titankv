package com.titankv.cluster;

import com.titankv.TitanKVClient;
import com.titankv.consistency.ConsistencyLevel;
import com.titankv.core.KeyValuePair;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Time-to-live set by the client, on a single node and replicated across a cluster.
 */
@Tag("integration")
class TtlTest {

    private static final int BASE_PORT = 19850;

    @Test
    void singleNodeExpiresKeys() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT, 1);
             TitanKVClient client = cluster.client()) {
            assertExpires(client);
        }
    }

    @Test
    void clusterExpiresKeysOnEveryReplica() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT + 10, 3);
             TitanKVClient client = cluster.client()) {
            assertExpires(client);
            // Each replica keeps the expired write's version as a tombstone
            for (int i = 0; i < 3; i++) {
                assertThat(cluster.node(i).getStore().get("ttl-key")).isEmpty();
                assertThat(cluster.node(i).getStore().getRaw("ttl-key").orElseThrow().isTombstone()).isTrue();
            }
        }
    }

    /**
     * A value is overwritten with a short-lived one that reaches a quorum but not the third
     * replica. Once the overwrite expires, the third replica's older copy must not be read, or
     * repaired back onto the others, as if the overwrite had never happened.
     */
    @Test
    void expiredOverwriteDoesNotBringBackTheOlderValue() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT + 20, 3);
             TitanKVClient client = cluster.client();
             TitanKVClient viaStaleReplica = cluster.client(cluster.address(0))) {
            client.put("overwritten", "old".getBytes(StandardCharsets.UTF_8), 0, ConsistencyLevel.ALL);
            long oldVersion = client.getWithMetadata("overwritten", ConsistencyLevel.ALL).orElseThrow().getTimestamp();

            // The overwrite as nodes 2 and 3 applied it; node 1 missed it
            long newVersion = oldVersion + 1;
            long expiresAt = System.currentTimeMillis() + 300;
            byte[] shortLived = "short-lived".getBytes(StandardCharsets.UTF_8);
            cluster.node(1).getStore().putIfNewer("overwritten", shortLived, newVersion, expiresAt);
            cluster.node(2).getStore().putIfNewer("overwritten", shortLived, newVersion, expiresAt);
            Thread.sleep(500);

            // Node 1 coordinates, so the read quorum is its own stale copy plus one expired copy
            assertThat(viaStaleReplica.getString("overwritten", ConsistencyLevel.QUORUM)).isEmpty();
            assertThat(viaStaleReplica.exists("overwritten", ConsistencyLevel.QUORUM)).isFalse();

            // Read repair brings node 1 up to date, rather than spreading its old value
            TestCluster.awaitCondition(() -> cluster.node(0).getStore().getRaw("overwritten")
                    .map(KeyValuePair::isTombstone).orElse(false), 5_000, "the stale replica to be repaired");
            for (int i = 0; i < 3; i++) {
                assertThat(cluster.node(i).getStore().get("overwritten")).as("node " + (i + 1)).isEmpty();
            }
            assertThat(client.getString("overwritten", ConsistencyLevel.ALL)).isEmpty();
        }
    }

    private static void assertExpires(TitanKVClient client) throws Exception {
        client.put("ttl-key", "short-lived".getBytes(StandardCharsets.UTF_8), 500);
        client.put("durable-key", "stays");

        assertThat(client.getString("ttl-key")).contains("short-lived");
        assertThat(client.getWithMetadata("ttl-key").orElseThrow().getExpiresAt())
                .isGreaterThan(System.currentTimeMillis());

        Thread.sleep(800);
        assertThat(client.getString("ttl-key")).isEmpty();
        assertThat(client.exists("ttl-key")).isFalse();
        assertThat(client.getString("durable-key")).contains("stays");
    }
}
