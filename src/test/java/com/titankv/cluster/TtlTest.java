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

    /**
     * A write with a TTL that is sent twice with one version (a failover, or a retry on a stale
     * connection) gets its expiry from each coordinator's clock. The replicas then hold the same
     * version with two expiry times. They must settle on one: before expiry was part of the
     * version order, neither copy replaced the other, and anti-entropy exchanged the key on
     * every round without ever bringing the replicas together.
     */
    @Test
    void writeRetriedThroughAnotherNodeEndsWithOneExpiryEverywhere() throws Exception {
        System.setProperty("titankv.anti.entropy.interval.ms", "0"); // only the explicit rounds below
        try (TestCluster cluster = TestCluster.start(BASE_PORT + 30, 3);
             java.net.Socket viaNode1 = new java.net.Socket("localhost", BASE_PORT + 30);
             java.net.Socket viaNode2 = new java.net.Socket("localhost", BASE_PORT + 31)) {
            long version = com.titankv.util.HybridLogicalClock.encode(System.currentTimeMillis());
            byte[] value = "v".getBytes(StandardCharsets.UTF_8);
            com.titankv.network.protocol.Command put = new com.titankv.network.protocol.Command(
                    com.titankv.network.protocol.Command.PUT, "ttl-retry", value, version, 600_000);

            // First attempt through node 1, which cannot reach node 3: nodes 1 and 2 take it
            cluster.cutOneWay(0, 2);
            assertThat(ClockSkewTest.send(viaNode1, put).isOk()).isTrue();
            Thread.sleep(50);
            // The retry, with the same version, through node 2: a later expiry, on all three
            assertThat(ClockSkewTest.send(viaNode2, put).isOk()).as("a retry of the same write is not stale").isTrue();
            cluster.heal();
            TestCluster.awaitCondition(() -> cluster.node(0).getReplicationManager().getHintedHandoff()
                    .totalPendingHints() == 0, 10_000, "node 1 to deliver its hint for node 3");

            com.titankv.consistency.AntiEntropy repair = cluster.node(0).getReplicationManager().getAntiEntropy();
            for (int peer = 1; peer < 3; peer++) {
                Node node = cluster.node(0).getClusterManager().getNode(cluster.node(peer).getNodeId());
                repair.repairWith(node);
                assertThat(repair.repairWith(node).differingLeaves()).as("after one repair round").isZero();
            }
            long expiry = cluster.node(0).getStore().getRaw("ttl-retry").orElseThrow().getExpiresAt();
            for (int i = 0; i < 3; i++) {
                KeyValuePair copy = cluster.node(i).getStore().getRaw("ttl-retry").orElseThrow();
                assertThat(copy.getTimestamp()).isEqualTo(version);
                assertThat(copy.getExpiresAt()).as("expiry on node " + (i + 1)).isEqualTo(expiry);
            }
        } finally {
            System.clearProperty("titankv.anti.entropy.interval.ms");
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
