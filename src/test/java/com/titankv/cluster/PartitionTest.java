package com.titankv.cluster;

import com.titankv.TitanKVClient;
import com.titankv.consistency.ConsistencyLevel;
import com.titankv.core.KVStore;
import com.titankv.core.KeyValuePair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Behaviour under network faults between nodes, injected with {@link FaultInjector}: a partition,
 * an isolated node, a link cut in one direction, and a slow link.
 */
@Tag("integration")
class PartitionTest {

    private static final int BASE_PORT = 23000;

    @AfterEach
    void resetProperties() {
        System.clearProperty("titankv.replication.threads");
        System.clearProperty("titankv.speculative.retry.ms");
    }

    private static String text(Optional<KeyValuePair> entry) {
        return entry.map(kv -> kv.isTombstone() ? "(tombstone)" : new String(kv.getValueUnsafe(), StandardCharsets.UTF_8))
                .orElse("(none)");
    }

    /**
     * A 5-node cluster splits 2 | 3. Replica sets are fixed, so each key can be served at QUORUM
     * by exactly one side: the side holding two of its three replicas. The other side must
     * refuse, not accept the write on the replicas it happens to reach. After the partition
     * heals, every replica holds what was acknowledged and no other node holds the key.
     */
    @Test
    void eachSideServesExactlyTheKeysItHoldsAQuorumOf() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT, 5)) {
            cluster.partition(new int[] {0, 1}, new int[] {2, 3, 4});
            cluster.awaitAliveCount(0, 2, 20_000);
            cluster.awaitAliveCount(1, 2, 20_000);
            for (int i = 2; i < 5; i++) {
                cluster.awaitAliveCount(i, 3, 20_000);
            }

            Map<String, String> acknowledged = new LinkedHashMap<>();
            int servedBySmallSide = 0;
            try (TitanKVClient small = cluster.client(cluster.address(0), cluster.address(1));
                 TitanKVClient large = cluster.client(cluster.address(2), cluster.address(3), cluster.address(4))) {
                for (int i = 0; i < 60; i++) {
                    String key = "partition-" + i;
                    long replicasOnSmallSide = cluster.replicasOf(key, 0).stream().filter(r -> r < 2).count();
                    boolean smallServes = replicasOnSmallSide >= 2;
                    TitanKVClient serving = smallServes ? small : large;
                    TitanKVClient cutOff = smallServes ? large : small;
                    String value = (smallServes ? "small:" : "large:") + key;

                    serving.put(key, value);
                    assertThat(serving.getString(key)).as(key).contains(value);
                    assertThatThrownBy(() -> cutOff.put(key, "must-not-be-acknowledged")).as(key)
                            .isInstanceOf(IOException.class).hasMessageContaining("Not enough replicas");
                    assertThatThrownBy(() -> cutOff.getString(key)).as(key)
                            .isInstanceOf(IOException.class).hasMessageContaining("Not enough replicas");

                    acknowledged.put(key, value);
                    servedBySmallSide += smallServes ? 1 : 0;
                }
            }
            assertThat(servedBySmallSide).as("keys only the small side could serve").isBetween(1, 59);

            cluster.heal();
            cluster.awaitConverged(30_000);
            TestCluster.awaitCondition(() -> everyReplicaHolds(cluster, acknowledged), 30_000,
                    "every replica to hold the acknowledged value after the partition healed");
            for (String key : acknowledged.keySet()) {
                List<Integer> replicas = cluster.replicasOf(key, 0);
                for (int node = 0; node < 5; node++) {
                    if (!replicas.contains(node)) {
                        assertThat(cluster.node(node).getStore().getRaw(key))
                                .as(key + " on node " + (node + 1) + ", which does not replicate it").isEmpty();
                    }
                }
            }
            try (TitanKVClient client = cluster.client()) {
                for (Map.Entry<String, String> entry : acknowledged.entrySet()) {
                    assertThat(client.getString(entry.getKey(), ConsistencyLevel.ALL)).contains(entry.getValue());
                }
            }
        }
    }

    private static boolean everyReplicaHolds(TestCluster cluster, Map<String, String> expected) {
        for (Map.Entry<String, String> entry : expected.entrySet()) {
            for (int replica : cluster.replicasOf(entry.getKey(), 0)) {
                if (!entry.getValue().equals(text(cluster.node(replica).getStore().getRaw(entry.getKey())))) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * One node of three is cut off. The other two keep serving QUORUM; the isolated node refuses
     * QUORUM but still answers ONE with what it has, which is stale. Once the network heals it
     * receives the writes and deletes it missed, and the deleted key stays deleted.
     */
    @Test
    void isolatedNodeServesStaleDataOnlyAtOneAndCatchesUpAfterHealing() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT + 10, 3);
             TitanKVClient connected = cluster.client(cluster.address(1), cluster.address(2));
             TitanKVClient isolated = cluster.client(cluster.address(0))) {
            connected.put("updated", "before".getBytes(StandardCharsets.UTF_8), 0, ConsistencyLevel.ALL);
            connected.put("deleted", "before".getBytes(StandardCharsets.UTF_8), 0, ConsistencyLevel.ALL);

            cluster.isolate(0);
            cluster.awaitAliveCount(0, 1, 20_000);
            cluster.awaitAliveCount(1, 2, 20_000);
            cluster.awaitAliveCount(2, 2, 20_000);

            connected.put("updated", "during");
            connected.delete("deleted");
            connected.put("created", "during");
            assertThat(connected.getString("updated")).contains("during");
            assertThat(connected.getString("deleted")).isEmpty();

            assertThatThrownBy(() -> isolated.getString("updated", ConsistencyLevel.QUORUM))
                    .hasMessageContaining("Not enough replicas");
            assertThatThrownBy(() -> isolated.put("updated", "from-the-isolated-node"))
                    .hasMessageContaining("Not enough replicas");
            assertThat(isolated.getString("updated", ConsistencyLevel.ONE)).contains("before");
            assertThat(isolated.getString("deleted", ConsistencyLevel.ONE)).contains("before");

            cluster.heal();
            cluster.awaitConverged(30_000);
            KVStore rejoined = cluster.node(0).getStore();
            TestCluster.awaitCondition(() -> text(rejoined.getRaw("updated")).equals("during")
                            && text(rejoined.getRaw("deleted")).equals("(tombstone)")
                            && text(rejoined.getRaw("created")).equals("during"),
                    30_000, "the rejoined node to catch up");
            assertThat(isolated.getString("deleted", ConsistencyLevel.ALL)).isEmpty();
            assertThat(isolated.getString("updated", ConsistencyLevel.ALL)).contains("during");
        }
    }

    /**
     * Node 1 cannot send to node 2, but node 2 can send to node 1. Gossip reaches node 2 through
     * node 3, so nobody is marked down. Node 1 still meets QUORUM with node 3, cannot meet ALL, and
     * keeps node 2's copies as hints until the link is back.
     */
    @Test
    void linkCutInOneDirectionLeavesQuorumWorkingAndIsRepairedByHints() throws Exception {
        try (TestCluster cluster = TestCluster.start(BASE_PORT + 20, 3);
             TitanKVClient viaNode1 = cluster.client(cluster.address(0));
             TitanKVClient viaNode2 = cluster.client(cluster.address(1))) {
            cluster.cutOneWay(0, 1);
            Thread.sleep(4_500); // longer than the 3s after which a silent node is suspected
            for (int i = 0; i < 3; i++) {
                assertThat(cluster.node(i).getClusterManager().getAliveNodeCount())
                        .as("alive nodes seen by node " + (i + 1)).isEqualTo(3);
            }

            List<String> keys = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                keys.add("one-way-" + i);
                viaNode1.put("one-way-" + i, "value-" + i);
            }
            KVStore unreached = cluster.node(1).getStore();
            for (String key : keys) {
                assertThat(unreached.getRaw(key)).as(key + " on the node the coordinator cannot reach").isEmpty();
            }
            assertThat(cluster.node(0).getReplicationManager().getHintedHandoff()
                    .pendingHints(cluster.node(1).getNodeId())).isEqualTo(20);
            assertThatThrownBy(() -> viaNode1.getString("one-way-0", ConsistencyLevel.ALL))
                    .isInstanceOf(IOException.class);
            // The other direction works: node 2 reads through nodes 1 and 3
            assertThat(viaNode2.getString("one-way-0", ConsistencyLevel.ALL)).contains("value-0");

            cluster.heal();
            TestCluster.awaitCondition(() -> keys.stream().allMatch(key -> unreached.getRaw(key).isPresent()),
                    15_000, "hints to reach the node once the link is back");
            assertThat(cluster.node(0).getReplicationManager().getHintedHandoff().totalPendingHints()).isZero();
        }
    }

    /**
     * Requests from node 1 to node 2 take a second. QUORUM needs only one replica besides the
     * coordinator, so requests through node 1 should be answered by node 3 without waiting for
     * node 2: writes complete on the first two acknowledgements, and a read that picked node 2
     * asks node 3 as well after 10 ms (speculative retry). Calls to each node run on that node's
     * own threads. When all nodes shared one pool, the calls stuck on node 2 took every thread
     * and the coordinator fell to about 20 requests a second (threads / the slow node's latency).
     */
    @Test
    void slowReplicaDoesNotHoldUpQuorumRequests() throws Exception {
        System.setProperty("titankv.replication.threads", "16");
        System.setProperty("titankv.speculative.retry.ms", "10");
        try (TestCluster cluster = TestCluster.start(BASE_PORT + 30, 3);
             TitanKVClient viaNode1 = cluster.client(cluster.address(0))) {
            for (int i = 0; i < 40; i++) {
                viaNode1.put("slow-" + i, "value-" + i); // also warms up connections
            }
            cluster.slowLink(0, 1, 1_000);

            long slowest = 0;
            long deadline = System.currentTimeMillis() + 5_000;
            int operations = 0;
            while (System.currentTimeMillis() < deadline) {
                String key = "slow-" + (operations % 40);
                long start = System.nanoTime();
                if (operations % 2 == 0) {
                    viaNode1.put(key, "value-" + operations);
                } else {
                    assertThat(viaNode1.getString(key)).isPresent();
                }
                slowest = Math.max(slowest, (System.nanoTime() - start) / 1_000_000);
                operations++;
            }

            System.out.printf("PartitionTest (slow replica): %d operations in 5s, slowest %d ms, %d speculative reads%n",
                    operations, slowest, cluster.node(0).getReplicationManager().getReadRepairHandler().getSpeculativeReads());
            assertThat(slowest).as("slowest QUORUM request in ms, with one replica a second away").isLessThan(600);
            assertThat(operations).as("requests completed in 5s").isGreaterThan(400);
        }
    }
}
