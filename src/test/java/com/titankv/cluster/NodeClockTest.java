package com.titankv.cluster;

import com.titankv.TitanKVClient;
import com.titankv.consistency.ConsistencyLevel;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What a node's own clock still decides, with that clock wrong on purpose. Each node reads the
 * time from one place ({@code HybridLogicalClock.wallMillis}), so one node of a cluster inside
 * this JVM can be given a clock of its own.
 */
@Tag("integration")
class NodeClockTest {

    private static final int BASE_PORT = 24400;

    /**
     * Each replica ends a TTL by its own clock, and an expired value is a tombstone that wins
     * over the same version's value. So the replica whose clock runs fastest ends the TTL for
     * the whole cluster: here 30 seconds fast, with a TTL of 20.
     */
    @Test
    void aTtlEndsWhenTheFirstReplicasClockSaysSo() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT, 3);
             TitanKVClient viaNode1 = cluster.client(cluster.address(0));
             TitanKVClient viaFastNode = cluster.client(cluster.address(2))) {
            cluster.node(2).getClusterManager().getClock().setSkewMillis(30_000);

            viaNode1.put("session", "token".getBytes(), 20_000, ConsistencyLevel.ALL);
            assertThat(cluster.node(0).getStore().get("session")).as("by node 1's clock").isPresent();
            assertThat(cluster.node(2).getStore().get("session")).as("by the fast node's clock").isEmpty();
            assertThat(viaFastNode.getString("session", ConsistencyLevel.ONE)).isEmpty();

            // A read that includes the fast node finds its tombstone and repairs the others with it
            assertThat(viaNode1.getString("session", ConsistencyLevel.ALL)).isEmpty();
            TestCluster.awaitCondition(() -> cluster.node(0).getStore().get("session").isEmpty()
                    && cluster.node(1).getStore().get("session").isEmpty(), 5_000,
                    "the other replicas to drop the key well before their own clocks end its TTL");
        }
    }

    /**
     * Gossip more than 60 seconds ahead of the receiver's clock is dropped, so a node whose clock
     * is that wrong is taken for dead by the others, and the cluster carries on without it. Once
     * its clock is right again it is a member again.
     */
    @Test
    void aNodeWhoseClockIsFarAheadIsTakenForDeadUntilItIsCorrected() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT + 20, 3);
             TitanKVClient viaNode1 = cluster.client(cluster.address(0))) {
            viaNode1.put("before", "skew");

            cluster.node(2).getClusterManager().getClock().setSkewMillis(70_000);
            cluster.awaitAliveCount(0, 2, 30_000);
            cluster.awaitAliveCount(1, 2, 30_000);

            viaNode1.put("during", "skew");
            assertThat(viaNode1.getString("during")).contains("skew");
            assertThat(viaNode1.getString("before")).contains("skew");

            // Its gossip clock only moves forward, so it is believed again once real time has
            // caught up to within 60 seconds of what it last sent: about 10 seconds from here
            cluster.node(2).getClusterManager().getClock().setSkewMillis(0);
            cluster.awaitAliveCount(0, 3, 45_000);
            cluster.awaitAliveCount(1, 3, 45_000);
            TestCluster.awaitCondition(() -> cluster.node(2).getStore().get("during").isPresent(), 30_000,
                    "the corrected node to receive the write it missed");
        }
    }
}
