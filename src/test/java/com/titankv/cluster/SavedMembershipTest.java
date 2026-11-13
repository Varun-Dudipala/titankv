package com.titankv.cluster;

import com.titankv.TitanKVClient;
import com.titankv.TitanKVServer;
import com.titankv.consistency.ConsistencyLevel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Restarts of durable nodes, which save their membership next to the WAL. A restarted node must
 * know the whole ring before it serves, whichever of its peers happen to be up.
 */
@Tag("integration")
class SavedMembershipTest {

    private static final int BASE_PORT = 23200;

    private TestCluster cluster;

    @BeforeEach
    void productionMode() throws Exception {
        System.setProperty("titankv.dev.mode", "false");
        System.setProperty("titankv.cluster.secret", "saved-membership-secret");
        System.setProperty("titankv.data.dir", Files.createTempDirectory("titankv-membership").toString());
    }

    @AfterEach
    void stopCluster() {
        if (cluster != null) {
            cluster.close();
        }
        System.clearProperty("titankv.cluster.secret");
        System.clearProperty("titankv.data.dir");
        System.setProperty("titankv.dev.mode", "true");
    }

    private String keyWhoseReplicas(Predicate<List<Integer>> condition) {
        for (int i = 0; ; i++) {
            String key = "saved-" + i;
            if (condition.test(cluster.replicasOf(key, 0))) {
                return key;
            }
        }
    }

    /**
     * All five nodes stop, and only two come back. They must not take the two of them for the
     * cluster. This is what happened before membership was saved: each learned the other from
     * its first gossip message, reported ready with a two-node ring, and acknowledged QUORUM
     * writes for keys whose three replicas were all still down.
     */
    @Test
    void nodesBackAfterAFullStopKnowTheWholeRingBeforeTheOthersReturn() throws Exception {
        cluster = TestCluster.start(BASE_PORT, 5);
        String heldByOthers = keyWhoseReplicas(replicas -> !replicas.contains(0) && !replicas.contains(1));
        String heldByBoth = keyWhoseReplicas(replicas -> replicas.contains(0) && replicas.contains(1));
        try (TitanKVClient client = cluster.client()) {
            client.put(heldByOthers, "before", ConsistencyLevel.ALL);
            client.put(heldByBoth, "before", ConsistencyLevel.ALL);
        }

        for (int i = 0; i < 5; i++) {
            cluster.crashNode(i);
        }
        TitanKVServer first = cluster.startNode(0);
        TitanKVServer second = cluster.startNode(1);
        assertThat(first.getClusterManager().getHashRing().getNodeCount()).as("ring on restart").isEqualTo(5);
        assertThat(second.getClusterManager().getHashRing().getNodeCount()).as("ring on restart").isEqualTo(5);

        TestCluster.awaitCondition(() -> first.getClusterManager().isReady() && second.getClusterManager().isReady(),
                15_000, "the two restarted nodes to be ready");
        cluster.awaitAliveCount(0, 2, 15_000);
        cluster.awaitAliveCount(1, 2, 15_000);
        try (TitanKVClient client = cluster.client(cluster.address(0), cluster.address(1))) {
            assertThatThrownBy(() -> client.put(heldByOthers, "must-not-be-acknowledged"))
                    .isInstanceOf(IOException.class).hasMessageContaining("Not enough replicas");
            assertThatThrownBy(() -> client.getString(heldByOthers))
                    .isInstanceOf(IOException.class).hasMessageContaining("Not enough replicas");
            client.put(heldByBoth, "after");
            assertThat(client.getString(heldByBoth)).contains("after");
        }
        assertThat(first.getStore().getRaw(heldByOthers)).as("a key this node does not replicate").isEmpty();
        assertThat(second.getStore().getRaw(heldByOthers)).as("a key this node does not replicate").isEmpty();

        for (int i = 2; i < 5; i++) {
            cluster.startNode(i);
        }
        cluster.awaitConverged(30_000);
        try (TitanKVClient client = cluster.client()) {
            assertThat(client.getString(heldByOthers)).contains("before");
            assertThat(client.getString(heldByBoth)).contains("after");
        }
    }

    /**
     * A node that comes back alone waits a few seconds for a peer to confirm the membership, then
     * serves on what it saved: reads at ONE work, and QUORUM still needs the other replicas.
     */
    @Test
    void nodeBackAloneServesOnItsSavedMembershipAfterWaitingForPeers() throws Exception {
        cluster = TestCluster.start(BASE_PORT + 10, 3);
        try (TitanKVClient client = cluster.client()) {
            client.put("alone-key", "value", ConsistencyLevel.ALL);
        }
        for (int i = 0; i < 3; i++) {
            cluster.stopNode(i);
        }

        long started = System.currentTimeMillis();
        TitanKVServer alone = cluster.startNode(0);
        assertThat(alone.getClusterManager().isReady()).as("ready before any peer could confirm").isFalse();
        assertThat(alone.getClusterManager().getHashRing().getNodeCount()).isEqualTo(3);

        TestCluster.awaitCondition(() -> alone.getClusterManager().isReady(), 15_000, "the lone node to be ready");
        assertThat(System.currentTimeMillis() - started)
                .isGreaterThanOrEqualTo(ClusterManager.SAVED_MEMBERSHIP_GRACE_MS - 500);
        try (TitanKVClient client = cluster.client(cluster.address(0))) {
            assertThat(client.getString("alone-key", ConsistencyLevel.ONE)).contains("value");
            assertThatThrownBy(() -> client.getString("alone-key", ConsistencyLevel.QUORUM))
                    .hasMessageContaining("Not enough replicas");
        }
    }

    /**
     * A restarted node has its data and is no newcomer to its peers. It used to treat every peer
     * it rediscovered as a node that had just joined, and send each one every key they share.
     */
    @Test
    void restartedNodeDoesNotResendItsDataToItsPeers() throws Exception {
        cluster = TestCluster.start(BASE_PORT + 20, 3);
        try (TitanKVClient client = cluster.client()) {
            for (int i = 0; i < 200; i++) {
                client.put("stream-" + i, "value-" + i, ConsistencyLevel.ALL);
            }
        }
        double before1 = replicaWrites(cluster.node(1));
        double before2 = replicaWrites(cluster.node(2));

        cluster.crashNode(0);
        cluster.startNode(0);
        cluster.awaitConverged(30_000);
        Thread.sleep(4_000); // past the anti-entropy round its peers run with a restarted node

        assertThat(replicaWrites(cluster.node(1)) - before1).as("replica writes node 2 received").isZero();
        assertThat(replicaWrites(cluster.node(2)) - before2).as("replica writes node 3 received").isZero();
        assertThat(cluster.node(0).getStore().size()).isEqualTo(200);
    }

    private static double replicaWrites(TitanKVServer server) {
        return server.getMetrics().getRegistry().get("titankv.replica.ops").tag("operation", "write")
                .counter().count();
    }
}
